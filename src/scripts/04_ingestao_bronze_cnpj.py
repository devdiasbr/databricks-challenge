import os
import sys
import logging
import io
import glob
import re
import time
from datetime import datetime
from typing import Optional, List, Dict, Tuple
import zipfile
import json
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from azure.storage.blob import ContainerClient

# Add project root to sys.path
# Configuração robusta de caminhos (Híbrido Local/Databricks)
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

# Navega para cima até encontrar a pasta 'src' para definir o project_root
project_root = base_dir
while not os.path.exists(os.path.join(project_root, 'src')) and project_root != os.path.dirname(project_root):
    project_root = os.path.dirname(project_root)

# Fallback: se não achou src, usa o base_dir (assume execução na raiz ou flat)
if not os.path.exists(os.path.join(project_root, 'src')):
    project_root = base_dir

src_dir = os.path.join(project_root, "src")
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

# Import SmartFileLoader
from utils.file_validator import SmartFileLoader

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
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.speculation", "false") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.network.timeout", "600s") \
    .master("local[*]") \
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

# Source Config (Account Key) - From config/env (Secure)
SOURCE_ACCOUNT = config.SOURCE_ACCOUNT
SOURCE_SAS = config.SAS_TOKEN_CNPJ
SOURCE_KEY = getattr(config, 'LANDING_ACCOUNT_KEY', None)

# Configura credenciais de ORIGEM (Landing) - Usando Account Key (Prioritário)
if SOURCE_KEY:
    # Configura Account Key para acesso total ao container
    spark.conf.set(f"fs.azure.account.key.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_KEY)
    spark.conf.set(f"fs.azure.account.key.{SOURCE_ACCOUNT}.blob.core.windows.net", SOURCE_KEY)
    print(f"Configured Account Key for {SOURCE_ACCOUNT}")
elif SOURCE_SAS:
    # Fallback para SAS se necessário
    spark.conf.set(f"fs.azure.sas.cnpj.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)
    spark.conf.set(f"fs.azure.sas.cnpj.{SOURCE_ACCOUNT}.blob.core.windows.net", SOURCE_SAS)
    # ABFSS
    spark.conf.set(f"fs.azure.account.auth.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)

# Target Config (Raw)
TARGET_RAW_URL = config.TARGET_RAW_URL
TARGET_SAS = ""
TARGET_ACCOUNT = config.TARGET_ACCOUNT

if TARGET_RAW_URL:
    if "?" in TARGET_RAW_URL:
        TARGET_SAS = TARGET_RAW_URL.split("?")[1]

if TARGET_SAS:
    # Configure SAS for grupo4storage (Raw Container)
    account = TARGET_ACCOUNT
    container = "raw"
    
    # Configuração para WASBS (Blob API) - Mais estável para SAS em local mode
    # Evita o erro ClassNotFoundException: FixedSASTokenProvider
    spark.conf.set(f"fs.azure.sas.{container}.{account}.blob.core.windows.net", TARGET_SAS)
    
    # Configuração legado para ABFSS (caso ainda seja usado, mas sem o Provider fixo que falha)
    # spark.conf.set(f"fs.azure.sas.{container}.{account}.dfs.core.windows.net", TARGET_SAS)

spark.conf.set("fs.azure.enable.check.access", "false")
spark.conf.set("fs.azure.skipUserGroupMetadataDuringInitialization", "true")

SOURCE_STORAGE_ACCOUNT = SOURCE_ACCOUNT
SOURCE_CONTAINER = "cnpj"
# SOURCE mantido em ABFSS pois está usando Account Key (que funciona bem com ABFSS)
SOURCE_ABFSS_PATH = f"abfss://{SOURCE_CONTAINER}@{SOURCE_STORAGE_ACCOUNT}.dfs.core.windows.net"

TARGET_STORAGE_ACCOUNT = TARGET_ACCOUNT
TARGET_CONTAINER = "raw"
# Alterado para WASBS para contornar erro de Provider do SAS
# Ajuste: Adicionado /cnpj para garantir organização dentro do raw (raw/cnpj/empresas...)
TARGET_ABFSS_PATH = f"wasbs://{TARGET_CONTAINER}@{TARGET_STORAGE_ACCOUNT}.blob.core.windows.net/cnpj"

LOGS_CONTAINER = "$logs"
# Logs também via WASBS
LOGS_ABFSS_PATH = f"wasbs://{LOGS_CONTAINER}@{TARGET_STORAGE_ACCOUNT}.blob.core.windows.net"

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
    "Empresas*.zip",
    "Estabelecimentos*.zip",
    "Socios*.zip",
    "Simples*.zip",
    "Cnaes*.zip",
    "Motivos*.zip",
    "Municipios*.zip",
    "Naturezas*.zip",
    "Paises*.zip",
    "Qualificacoes*.zip"
]

CSV_PATTERNS = {
    "EMPRECSV": "Empresas*.csv",   # Matches key in JSON
    "ESTABELE": "Estabelecimentos*.csv", # Matches key in JSON
    "SOCIOCSV": "Socios*.csv",
    "SIMPLES": "Simples*.csv",
    "CNAECSV": "Cnaes*.csv",
    "MOTIVOS": "Motivos*.csv",
    "MUNICIPIOS": "Municipios*.csv",
    "NATUREZAS": "Naturezas*.csv",
    "PAISES": "Paises*.csv",
    "QUALIFICACOES": "Qualificacoes*.csv"
}

# Mapeamento para nomes de pastas amigáveis (conforme config.py)
ENTITY_FOLDER_MAP = {
    "EMPRECSV": "empresas",
    "ESTABELE": "estabelecimentos",
    "SOCIOCSV": "socios",
    "SIMPLES": "simples",
    "CNAECSV": "cnaes",
    "MOTIVOS": "motivos",
    "MUNICIPIOS": "municipios",
    "NATUREZAS": "naturezas",
    "PAISES": "paises",
    "QUALIFICACOES": "qualificacoes"
}

# =============================================================================
# CARREGAMENTO DE SCHEMA (Substitui COLUMN_NAMES hardcoded)
# =============================================================================
schema_path = os.path.join(project_root, 'docs', 'schemas', 'cnpj_schema.json')
try:
    with open(schema_path, 'r', encoding='utf-8') as f:
        COLUMN_NAMES = json.load(f)
    print(f"Schema carregado de: {schema_path}")
except Exception as e:
    print(f"Erro ao carregar schema CNPJ: {e}")
    # Fallback vazio ou erro crítico? Melhor erro crítico ou logar
    COLUMN_NAMES = {}

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
def download_data(logger):
    """
    Downloads zip files from Azure Blob Storage.
    Extraction is now handled by SmartFileLoader during processing.
    """
    import fnmatch

    try:
        # Construct Container Client
        if SOURCE_KEY:
            # Authenticate with Account Key (Prioritized)
            account_url = f"https://{SOURCE_ACCOUNT}.blob.core.windows.net"
            container_client = ContainerClient(account_url=account_url, container_name=SOURCE_CONTAINER, credential=SOURCE_KEY)
            logger.info(f"Authenticated with Account Key for container: {SOURCE_CONTAINER}")
        elif SOURCE_SAS:
            # Authenticate with SAS Token
            container_url = f"https://{SOURCE_ACCOUNT}.blob.core.windows.net/{SOURCE_CONTAINER}?{SOURCE_SAS}"
            container_client = ContainerClient.from_container_url(container_url)
            logger.info(f"Authenticated with SAS Token for container: {SOURCE_CONTAINER}")
        else:
            logger.error("Source credentials (Key or SAS) missing. Cannot download data.")
            return
        
        # Ensure directories exist
        os.makedirs(TMP_EXTRACT_DIR, exist_ok=True)
        # DBFS_STAGING_DIR will be used by SmartLoader for extraction
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
                
                # Retry logic for download
                max_retries = 1
                for attempt in range(max_retries + 1):
                    should_download = True
                    if os.path.exists(local_zip_path):
                        # Verify integrity
                        try:
                            # Verifica tamanho mínimo (ex: 100 bytes) para evitar zips vazios
                            if os.path.getsize(local_zip_path) < 100:
                                logger.warning(f"Cached file {blob_name} is too small (<100b). Re-downloading...")
                                os.remove(local_zip_path)
                            else:
                                with zipfile.ZipFile(local_zip_path, 'r') as zf:
                                    if zf.testzip() is None:
                                        logger.info(f"Using cached valid file: {blob_name}")
                                        should_download = False
                                    else:
                                        logger.warning(f"Cached file {blob_name} is corrupted. Re-downloading...")
                                        os.remove(local_zip_path)
                        except zipfile.BadZipFile:
                            logger.warning(f"Cached file {blob_name} is invalid (BadZipFile). Re-downloading...")
                            os.remove(local_zip_path)
                        except Exception:
                            # Other errors (e.g. incomplete write), remove and retry
                            logger.warning(f"Cached file {blob_name} check failed. Re-downloading...")
                            if os.path.exists(local_zip_path):
                                os.remove(local_zip_path)

                    if should_download:
                        logger.info(f"Downloading {blob_name} (Attempt {attempt+1})...")
                        try:
                            with open(local_zip_path, "wb") as f:
                                download_stream = container_client.download_blob(blob.name)
                                for chunk in download_stream.chunks():
                                    f.write(chunk)
                            logger.info(f"Downloaded: {blob_name}")
                            break
                        except Exception as e:
                            logger.error(f"Failed to download {blob_name}: {e}")
                            if os.path.exists(local_zip_path):
                                os.remove(local_zip_path)
                            break
                    else:
                        logger.info(f"Using cached file: {blob_name}")
                        break
        
        if found_files == 0:
            logger.warning("No matching zip files found in source container.")
            
    except Exception as e:
        logger.error(f"Error in download: {e}")

# DataFrame Loading and Saving Logic
def process_entity(entity_name: str, file_pattern_csv: str, logger: logging.Logger):
    try:
        # 1. Encontrar todos os arquivos ZIP correspondentes
        zip_pattern = file_pattern_csv.replace(".csv", ".zip")
        found_zips = sorted(glob.glob(os.path.join(TMP_EXTRACT_DIR, zip_pattern)))
        
        if not found_zips:
            # Tenta busca case-insensitive ou sem sufixo numérico se falhar
            logger.warning(f"[{entity_name}] Nenhum ZIP exato encontrado para {zip_pattern} em {TMP_EXTRACT_DIR}.")
            # Fallback de busca manual
            all_zips = glob.glob(os.path.join(TMP_EXTRACT_DIR, "*.zip"))
            logger.info(f"[{entity_name}] Arquivos disponíveis na pasta: {[os.path.basename(z) for z in all_zips]}")
            return
            
        logger.info(f"[{entity_name}] Found {len(found_zips)} zip files to process.")
        
        first_batch = True
        
        for zip_file in found_zips:
            try:
                logger.info(f"[{entity_name}] Processing batch: {os.path.basename(zip_file)}")
                
                # 2. Smart Load
                loader = SmartFileLoader(temp_dir=DBFS_STAGING_DIR)
                file_info = loader.inspect_and_prepare(zip_file)
                
                # Validação de formato (Novo)
                if file_info.get('format') == 'unknown':
                    logger.error(f"[{entity_name}] ❌ Erro de validação: Formato desconhecido ou não suportado para {os.path.basename(zip_file)}")
                    continue
                
                df_raw = spark.read.format(file_info['format']).options(**file_info['options']).load(file_info['path'])
                
                # 3. Rename/Select Columns
                column_names = COLUMN_NAMES.get(entity_name, [])
                df_to_write = df_raw
                
                if column_names:
                    current_cols = df_raw.columns
                    final_selects = []
                    for i, col_name in enumerate(column_names):
                        spark_col = f"_c{i}"
                        if spark_col in current_cols:
                            final_selects.append(F.col(spark_col).alias(col_name))
                    
                    # Schema evolution (extra columns)
                    for col in current_cols:
                        if col.startswith("_c"):
                            try:
                                col_idx = int(col[2:])
                                if col_idx >= len(column_names):
                                    final_selects.append(F.col(col))
                            except ValueError:
                                pass
                    
                    df_to_write = df_raw.select(*final_selects)
                
                # 4. Save to Delta
                # Usa mapeamento para nome amigável ou fallback para minúsculo
                folder_name = ENTITY_FOLDER_MAP.get(entity_name, entity_name.lower())
                target_path = f"{TARGET_ABFSS_PATH}/{folder_name}"
                mode = "overwrite" if first_batch else "append"
                
                writer = df_to_write.write.format("delta").mode(mode).option("overwriteSchema", "true" if first_batch else "false")
                
                if entity_name == "ESTABELE":
                     writer = writer.partitionBy("uf")
                elif entity_name == "SIMPLES":
                     writer = writer.partitionBy("opcao_simples")
                
                writer.save(target_path)
                logger.info(f"[{entity_name}] Batch saved ({mode})")
                first_batch = False
                
                # Cleanup extracted file to save space
                try:
                    if os.path.exists(file_info['path']):
                        # os.remove(file_info['path']) # COMENTADO PARA DEBUG DO USUÁRIO
                        logger.info(f"   [DEBUG] Mantendo arquivo temporário em: {file_info['path']}")
                except:
                    pass
                    
            except Exception as e:
                logger.error(f"[{entity_name}] Error processing zip {zip_file}: {str(e)}")
                # Continue to next zip?
                
    except Exception as e:
        logger.error(f"[{entity_name}] Critical error: {str(e)}")

def run_pipeline():
    start_time = datetime.now()
    pipeline_run_id = start_time.strftime('%Y%m%d_%H%M%S')
    logger, blob_handler = setup_logging(pipeline_run_id)
    
    logger.info("="*80)
    logger.info("STARTING CNPJ BRONZE INGESTION")
    logger.info(f"Run ID: {pipeline_run_id}")
    logger.info("="*80)
    
    if os.name == 'nt':
        TMP_EXTRACT_DIR = os.path.join(tempfile.gettempdir(), "cnpj_extract")
        DBFS_STAGING_DIR = os.path.join(tempfile.gettempdir(), "cnpj_staging")
        TMP_LOG_DIR = os.path.join(tempfile.gettempdir(), "cnpj_logs")
    else:
        TMP_EXTRACT_DIR = "/tmp/cnpj_extract"
        DBFS_STAGING_DIR = "/tmp/cnpj"
        TMP_LOG_DIR = "/tmp/cnpj_logs"

    # Log dos diretórios para debug do usuário
    logger.info(f"📂 Diretório de Download (ZIPs): {TMP_EXTRACT_DIR}")
    logger.info(f"📂 Diretório de Staging (Extração): {DBFS_STAGING_DIR}")
    logger.info(f"📂 Diretório de Logs: {TMP_LOG_DIR}")
    
    # Run Extraction (Download from Azure)
    download_data(logger)
    
    total_entities = len(CSV_PATTERNS)
    with tqdm(total=total_entities, desc="Processing Entities") as pbar:
        for entity_name, file_pattern in CSV_PATTERNS.items():
            process_entity(entity_name, file_pattern, logger)
            pbar.update(1)
            
    logger.info("PIPELINE COMPLETED")
    blob_handler.flush_to_blob(f"{LOGS_ABFSS_PATH}/cnpj_pipeline_{pipeline_run_id}.log")

if __name__ == "__main__":
    run_pipeline()
