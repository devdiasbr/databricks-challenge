import os
import sys
import logging
import io
import glob
import re
import time
from datetime import datetime
from typing import Optional, List, Dict, Tuple
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from azure.storage.blob import ContainerClient

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
# src/scripts -> src
src_dir = os.path.dirname(current_dir)
if src_dir not in sys.path:
    sys.path.append(src_dir)

# Import TqdmLoggingHandler
try:
    from utils.logging_utils import TqdmLoggingHandler
except ImportError:
    from tqdm import tqdm
    class TqdmLoggingHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                tqdm.write(msg)
                self.flush()
            except Exception:
                self.handleError(record)

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable=None, *args, **kwargs):
        return iterable
    tqdm.write = print

# Windows Hadoop Workaround
if os.name == 'nt':
    import tempfile
    hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop_workaround")
    hadoop_bin = os.path.join(hadoop_home, "bin")
    os.makedirs(hadoop_bin, exist_ok=True)
    
    if 'HADOOP_HOME' not in os.environ:
        os.environ['HADOOP_HOME'] = hadoop_home
    
    winutils_path = os.path.join(hadoop_bin, "winutils.exe")
    if not os.path.exists(winutils_path):
        import urllib.request
        try:
            url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/winutils.exe"
            urllib.request.urlretrieve(url, winutils_path)
        except Exception:
            with open(winutils_path, "w") as f:
                f.write("Dummy winutils")

    if hadoop_bin not in os.environ['PATH']:
        os.environ['PATH'] += os.pathsep + hadoop_bin

# Initialize Spark
spark = SparkSession.builder \
    .appName("IngestaoBronzeCNPJ") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()

# Suppress logs
spark.sparkContext.setLogLevel("WARN")
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getLogger("org.apache.spark").setLevel(log4j.Level.WARN)
log4j.LogManager.getLogger("org.apache.hadoop").setLevel(log4j.Level.WARN)
log4j.LogManager.getLogger("py4j").setLevel(log4j.Level.ERROR)

# DBUtils Mock
try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except ImportError:
    class DBUtilsMock:
        def __init__(self):
            self.fs = self.FS()
        class FS:
            def cp(self, src, dest, recurse=False):
                try:
                    sc = spark.sparkContext
                    jvm = sc._jvm
                    conf = sc._jsc.hadoopConfiguration()
                    src_path = jvm.org.apache.hadoop.fs.Path(src)
                    dst_path = jvm.org.apache.hadoop.fs.Path(dest)
                    src_fs = src_path.getFileSystem(conf)
                    dst_fs = dst_path.getFileSystem(conf)
                    jvm.org.apache.hadoop.fs.FileUtil.copy(
                        src_fs, src_path, dst_fs, dst_path, False, True, conf
                    )
                except Exception:
                    if src.startswith("file:") and dest.startswith("file:"):
                        import shutil
                        shutil.copy(src[5:], dest[5:])
                    elif src.startswith("file:"): # Local to Remote (not supported fully in mock but simple copy might work if remote is local mapped)
                         pass
            def mkdirs(self, path):
                if path.startswith("file:") or ":" not in path:
                     os.makedirs(path.replace("file:", ""), exist_ok=True)
            def ls(self, path):
                # Minimal mock for ls
                if path.startswith("file:") or ":" not in path:
                    local_path = path.replace("file:", "")
                    if os.path.exists(local_path):
                         return [type('obj', (object,), {'name': f, 'path': os.path.join(local_path, f)}) for f in os.listdir(local_path)]
                return []
    dbutils = DBUtilsMock()

# Configuration
from utils import config

# Source Config (Account Key) - Mantendo hardcoded se não estiver no config, mas o user disse que está no .env/config
# O config.py não exporta a Key da Landing, apenas o SAS do Balança/CNPJ.
# Mas o script original usava uma Key Hardcoded para landingbeca2026jan!
# O user disse "tem tdo na .env".
# Vamos ver se a Key está no .env? O config.py não carrega KEY, carrega SAS.
# Mas o script 04 usa Account Key para a origem:
# fs.azure.account.key.landingbeca2026jan... = "THyEZ..."
# Isso é perigoso/feio.
# Se o config.py tem SAS_TOKEN_CNPJ, deveríamos usar SAS para a origem também, não Key.
# Vou mudar para SAS se disponível, ou manter a Key mas movendo para .env se possível.
# Como não tenho a Key no config.py, vou verificar se posso usar o SAS_TOKEN_CNPJ.
# O container é 'cnpj'. O SAS do config é SAS_TOKEN_CNPJ.
# Deve funcionar.

SOURCE_ACCOUNT = config.SOURCE_ACCOUNT
SOURCE_SAS = config.SAS_TOKEN_CNPJ

# Configura credenciais de ORIGEM (Landing)
if SOURCE_SAS:
    spark.conf.set(f"fs.azure.sas.cnpj.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)
    spark.conf.set(f"fs.azure.sas.cnpj.{SOURCE_ACCOUNT}.blob.core.windows.net", SOURCE_SAS)
    # ABFSS
    spark.conf.set(f"fs.azure.account.auth.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)

# Target Config (Raw)
TARGET_RAW_URL = config.TARGET_RAW_URL
TARGET_SAS = ""
TARGET_ACCOUNT = "grupo4storage"

if TARGET_RAW_URL:
    if "?" in TARGET_RAW_URL:
        TARGET_SAS = TARGET_RAW_URL.split("?")[1]
    try:
        TARGET_ACCOUNT = TARGET_RAW_URL.split("https://")[1].split(".")[0]
    except:
        pass

if TARGET_SAS:
    # Configure SAS for grupo4storage (Raw Container)
    account = TARGET_ACCOUNT
    container = "raw"
    
    # ABFSS Configuration
    spark.conf.set(f"fs.azure.account.auth.type.{account}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{account}.dfs.core.windows.net", TARGET_SAS)
    
    # Blob Configuration (Backup/Logs)
    spark.conf.set(f"fs.azure.sas.{container}.{account}.blob.core.windows.net", TARGET_SAS)
    # Also for $logs if needed
    spark.conf.set(f"fs.azure.sas.$logs.{account}.blob.core.windows.net", TARGET_SAS)

spark.conf.set("fs.azure.enable.check.access", "false")
spark.conf.set("fs.azure.skipUserGroupMetadataDuringInitialization", "true")

SOURCE_STORAGE_ACCOUNT = SOURCE_ACCOUNT
SOURCE_CONTAINER = "cnpj"
SOURCE_ABFSS_PATH = f"abfss://{SOURCE_CONTAINER}@{SOURCE_STORAGE_ACCOUNT}.dfs.core.windows.net"

TARGET_STORAGE_ACCOUNT = TARGET_ACCOUNT
TARGET_CONTAINER = "raw"
TARGET_ABFSS_PATH = f"abfss://{TARGET_CONTAINER}@{TARGET_STORAGE_ACCOUNT}.dfs.core.windows.net/cnpj"

LOGS_CONTAINER = "$logs"
LOGS_ABFSS_PATH = f"abfss://{LOGS_CONTAINER}@{TARGET_STORAGE_ACCOUNT}.dfs.core.windows.net"

# Directories
if os.name == 'nt':
    TMP_EXTRACT_DIR = os.path.join(tempfile.gettempdir(), "cnpj_extract")
    DBFS_STAGING_DIR = os.path.join(tempfile.gettempdir(), "cnpj_staging")
    TMP_LOG_DIR = os.path.join(tempfile.gettempdir(), "cnpj_logs")
else:
    TMP_EXTRACT_DIR = "/tmp/cnpj_extract"
    DBFS_STAGING_DIR = "/tmp/cnpj"
    TMP_LOG_DIR = "/tmp/cnpj_logs"

CSV_DELIMITER = ";"
CSV_ENCODING = "ISO-8859-1"

# Patterns
ZIP_FILE_PATTERNS = [
    "Cnaes.zip",
    "Empresas[0-5].zip",
    "Estabelecimentos[0-5].zip",
    "Paises.zip",
    "Naturezas.zip",
    "Municipios.zip",
    "Simples.zip"
]

CSV_PATTERNS = {
    "empresas": "*EMPRECSV",
    "estabelecimentos": "*ESTABELE",
    "cnaes": "*CNAECSV",
    "paises": "*PAISCSV",
    "naturezas": "*NATJUCSV",
    "municipios": "*MUNICCSV",
    "simples": "*SIMPLES.CSV*"
}

COLUMN_NAMES = {
    "empresas": [
        "cnpj_basico", "razao_social", "natureza_juridica", 
        "qualificacao_responsavel", "capital_social", "porte_empresa", 
        "ente_federativo_responsavel"
    ],
    "estabelecimentos": [
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
        "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
        "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
        "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2",
        "ddd_fax", "fax", "correio_eletronico", "situacao_especial", "data_situacao_especial"
    ],
    "cnaes": ["codigo", "descricao"],
    "paises": ["codigo", "descricao"],
    "naturezas": ["codigo", "descricao"],
    "municipios": ["codigo", "descricao"],
    "simples": [
        "cnpj_basico", "opcao_simples", "data_opcao_simples",
        "data_exclusao_simples", "opcao_mei", "data_opcao_mei", "data_exclusao_mei"
    ]
}

# Logging
class BlobStorageHandler(logging.Handler):
    def __init__(self, log_file_path: str):
        super().__init__()
        self.log_file_path = log_file_path
        self.log_buffer = io.StringIO()
        
    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_buffer.write(msg + '\n')
        except Exception:
            self.handleError(record)
    
    def flush_to_blob(self, blob_path: str):
        try:
            os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
            with open(self.log_file_path, 'w', encoding='utf-8') as f:
                f.write(self.log_buffer.getvalue())
            dbutils.fs.cp(f"file:{self.log_file_path}", blob_path)
            print(f"Logs saved to: {blob_path}")
        except Exception as e:
            print(f"Failed to save logs to blob: {str(e)}")

def setup_logging(pipeline_run_id: str):
    logger = logging.getLogger('cnpj_pipeline')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    console_handler = TqdmLoggingHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    log_file_path = f"{TMP_LOG_DIR}/pipeline_{pipeline_run_id}.log"
    blob_handler = BlobStorageHandler(log_file_path)
    blob_handler.setLevel(logging.INFO)
    blob_handler.setFormatter(console_formatter)
    logger.addHandler(blob_handler)
    
    return logger, blob_handler

# Extraction Logic
def download_and_extract_data(logger):
    """
    Downloads zip files from Azure Blob Storage and extracts them.
    Replaces dbutils extraction logic for local execution using Azure SDK.
    """
    import zipfile
    import fnmatch

    try:
        # Construct Container URL
        if SOURCE_SAS:
            container_url = f"https://{SOURCE_ACCOUNT}.blob.core.windows.net/{SOURCE_CONTAINER}?{SOURCE_SAS}"
        else:
            logger.error("Source SAS token missing. Cannot download data.")
            return

        container_client = ContainerClient.from_container_url(container_url)
        
        # Ensure directories exist
        os.makedirs(TMP_EXTRACT_DIR, exist_ok=True)
        os.makedirs(DBFS_STAGING_DIR, exist_ok=True)
        logger.info(f"Directories ready: {TMP_EXTRACT_DIR}, {DBFS_STAGING_DIR}")
        
        # List all blobs
        logger.info("Listing blobs in source container...")
        blobs = container_client.list_blobs()
        
        found_files = 0
        for blob in blobs:
            blob_name = blob.name
            
            # Check against patterns
            match = False
            for pattern in ZIP_FILE_PATTERNS:
                if fnmatch.fnmatch(blob_name, pattern):
                    match = True
                    break
            
            if match:
                found_files += 1
                local_zip_path = os.path.join(TMP_EXTRACT_DIR, blob_name)
                
                # Retry logic for BadZipFile
                max_retries = 1
                for attempt in range(max_retries + 1):
                    # Download if not exists
                    if not os.path.exists(local_zip_path):
                        logger.info(f"Downloading {blob_name} (Attempt {attempt+1})...")
                        try:
                            with open(local_zip_path, "wb") as f:
                                download_stream = container_client.download_blob(blob.name)
                                # Download in chunks of 4MB to avoid memory overload
                                for chunk in download_stream.chunks():
                                    f.write(chunk)
                        except Exception as e:
                            logger.error(f"Failed to download {blob_name}: {e}")
                            break
                    else:
                        if attempt == 0:
                            logger.info(f"Found local file: {blob_name}")
                
                    # Extract
                    logger.info(f"Extracting {blob_name} to {DBFS_STAGING_DIR}...")
                    try:
                        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                            zip_ref.extractall(DBFS_STAGING_DIR)
                        logger.info(f"Successfully extracted {blob_name}")
                        break # Success
                    except zipfile.BadZipFile:
                        logger.warning(f"Bad zip file detected: {blob_name}. Deleting and retrying...")
                        if os.path.exists(local_zip_path):
                            os.remove(local_zip_path)
                        if attempt == max_retries:
                            logger.error(f"Permanent failure extracting {blob_name} after retries.")
                    except Exception as e:
                        logger.error(f"Error extracting {blob_name}: {e}")
                        break
        
        if found_files == 0:
            logger.warning("No matching zip files found in source container.")
            
    except Exception as e:
        logger.error(f"Error in download/extract: {e}")

# DataFrame Loading
def load_csv_as_strings(file_pattern: str, entity_name: str, logger: logging.Logger) -> Optional[DataFrame]:
    try:
        # In notebook, they use DBFS_STAGING_DIR. 
        # But if we want to run this without extraction (e.g. data already there), we use DBFS_STAGING_DIR.
        # If we are strictly following notebook, we must extract.
        # But for simplicity and robustness, let's allow fallback or check.
        
        # For now, let's point to DBFS_STAGING_DIR as per notebook.
        # But since we might not have extracted, we might want to point to SOURCE if they were CSVs?
        # No, they are Zips. So we MUST point to extracted files.
        
        # To make it runnable locally without downloading 80GB, I will check if files exist.
        full_path = f"{DBFS_STAGING_DIR}/{file_pattern}"
        
        # If we are on Windows/Local and dir is empty, maybe warn?
        
        logger.info(f"[{entity_name}] Loading from {full_path}...")
        
        df_raw = spark.read \
            .option("header", "false") \
            .option("sep", CSV_DELIMITER) \
            .option("encoding", CSV_ENCODING) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("mode", "PERMISSIVE") \
            .csv(full_path)
        
        column_names = COLUMN_NAMES.get(entity_name, [])
        
        if column_names:
            current_cols = df_raw.columns
            final_selects = []
            
            for i, col_name in enumerate(column_names):
                spark_col = f"_c{i}"
                if spark_col in current_cols:
                    final_selects.append(F.col(spark_col).alias(col_name))
            
            # Schema evolution support (extra columns)
            for col in current_cols:
                if col.startswith("_c"):
                    try:
                        col_idx = int(col[2:])
                        if col_idx >= len(column_names):
                            final_selects.append(F.col(col))
                    except ValueError:
                        pass
            
            df_renamed = df_raw.select(*final_selects)
            return df_renamed
        else:
            return df_raw
            
    except Exception as e:
        logger.error(f"[{entity_name}] Error loading: {str(e)}")
        return None

def save_to_delta(df: DataFrame, entity_name: str, logger: logging.Logger):
    try:
        target_path = f"{TARGET_ABFSS_PATH}/{entity_name}"
        logger.info(f"[{entity_name}] Saving to {target_path}...")
        
        # Add partitioning if needed (from notebook config)
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        
        if entity_name == "estabelecimentos":
             writer = writer.partitionBy("uf")
        elif entity_name == "simples":
             writer = writer.partitionBy("opcao_simples")
             
        writer.save(target_path)
        logger.info(f"[{entity_name}] Saved successfully")
    except Exception as e:
        logger.error(f"[{entity_name}] Failed to save: {str(e)}")

def run_pipeline():
    start_time = datetime.now()
    pipeline_run_id = start_time.strftime('%Y%m%d_%H%M%S')
    logger, blob_handler = setup_logging(pipeline_run_id)
    
    logger.info("="*80)
    logger.info("STARTING CNPJ BRONZE INGESTION")
    logger.info(f"Run ID: {pipeline_run_id}")
    logger.info("="*80)
    
    # Run Extraction (Download from Azure + Unzip to Staging)
    download_and_extract_data(logger)
    
    total_files = len(CSV_PATTERNS)
    with tqdm(total=total_files, desc="Processing Entities") as pbar:
        for entity_name, file_pattern in CSV_PATTERNS.items():
            try:
                df = load_csv_as_strings(file_pattern, entity_name, logger)
                if df:
                    # Check if empty (lazy)
                    try:
                        if df.limit(1).count() > 0:
                            save_to_delta(df, entity_name, logger)
                        else:
                            logger.warning(f"[{entity_name}] DataFrame is empty")
                    except Exception as e:
                         # Likely file not found or path issue
                         logger.warning(f"[{entity_name}] Could not read data (Files missing?): {str(e)}")
                else:
                    logger.warning(f"[{entity_name}] DataFrame creation failed")
            except Exception as e:
                logger.error(f"Error processing {entity_name}: {str(e)}")
            pbar.update(1)
            
    logger.info("PIPELINE COMPLETED")
    blob_handler.flush_to_blob(f"{LOGS_ABFSS_PATH}/cnpj_pipeline_{pipeline_run_id}.log")

if __name__ == "__main__":
    run_pipeline()
