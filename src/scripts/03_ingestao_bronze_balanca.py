# Databricks notebook source
import os
import sys
import json
import logging
import re
import tqdm
import shutil
import tempfile
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
# =============================================================================
# LOGGING SETUP
# =============================================================================
# Tenta importar do utils, assumindo estrutura do projeto
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

src_path = os.path.join(project_root, "src")
if src_path not in sys.path:
    sys.path.append(src_path)

try:
    from utils.logging_utils import TqdmLoggingHandler
except ImportError:
    # Fallback simples se não encontrar
    class TqdmLoggingHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                tqdm.tqdm.write(msg)
                self.flush()
            except Exception:
                self.handleError(record)

logger = logging.getLogger("IngestaoBronzeBalanca")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TqdmLoggingHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(handler)

# =============================================================================
# WINDOWS HADOOP WORKAROUND
# =============================================================================
if os.name == 'nt':
    # Define HADOOP_HOME apontando para a pasta hadoop na raiz do projeto
    hadoop_home = os.path.join(project_root, "hadoop")
    
    # Valida se a pasta existe
    if not os.path.exists(hadoop_home):
        logger.warning(f"Aviso: Pasta HADOOP_HOME não encontrada em: {hadoop_home}")
        import tempfile
        hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop_workaround")
    
    os.environ['HADOOP_HOME'] = hadoop_home
    hadoop_bin = os.path.join(hadoop_home, "bin")
    
    # Garante que a pasta bin exista (se for fallback)
    os.makedirs(hadoop_bin, exist_ok=True)
    
    # Verifica winutils.exe
    winutils_path = os.path.join(hadoop_bin, "winutils.exe")
    if not os.path.exists(winutils_path):
        logger.warning(f"winutils.exe não encontrado em {winutils_path}. Tentando baixar...")
        import urllib.request
        try:
            url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/winutils.exe"
            urllib.request.urlretrieve(url, winutils_path)
            logger.info("winutils.exe baixado com sucesso.")
        except Exception as e:
            logger.error(f"Falha ao baixar winutils: {e}")
            with open(winutils_path, "w") as f:
                f.write("Dummy winutils")

    # Adiciona ao PATH
    if hadoop_bin not in os.environ['PATH']:
        os.environ['PATH'] += os.pathsep + hadoop_bin
        
    # logger.info(f"🔧 Configurado HADOOP_HOME: {hadoop_home}")

import utils.config as config
from utils.file_validator import SmartFileLoader
from azure.storage.blob import ContainerClient

# =============================================================================
# INITIALIZE SPARK SESSION
# =============================================================================
# COMMAND ----------

def get_spark_session():
    # logger.info("Initializing Spark Session with Delta support...")
    is_databricks = ("DATABRICKS_RUNTIME_VERSION" in os.environ or os.path.exists("/dbfs")) and os.name != 'nt'
    
    if is_databricks:
        # No Databricks, usar a sessão existente
        return SparkSession.builder.getOrCreate()
    
    # Local Environment
    builder = SparkSession.builder \
        .appName("IngestaoBronzeBalanca") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.speculation", "false") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
        .config("spark.driver.extraJavaOptions", "-Divy.message.logger.level=4 -Dlog4j.rootCategory=ERROR") \
        .master("local[*]")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    return spark

spark = get_spark_session()

# =============================================================================
# CONFIGURAÇÕES DE STORAGE E ACESSO (Via config.py)
# =============================================================================

# Configuração para evitar erro de "Failed to get primary group" no Windows
spark.conf.set("fs.azure.enable.check.access", "false")

SOURCE_CONTAINER = "balancacomercial"
SOURCE_ACCOUNT = config.SOURCE_ACCOUNT
SOURCE_SAS = config.SAS_TOKEN_BALANCA

TARGET_CONTAINER = "raw"
TARGET_RAW_URL = config.TARGET_RAW_URL

# Extrai SAS e Account do Target URL se necessário
TARGET_SAS = ""
TARGET_ACCOUNT = config.TARGET_ACCOUNT

if TARGET_RAW_URL:
    if "?" in TARGET_RAW_URL:
        TARGET_SAS = TARGET_RAW_URL.split("?")[1]

# Configura credenciais
if SOURCE_SAS:
    spark.conf.set(f"fs.azure.sas.{SOURCE_CONTAINER}.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)
    spark.conf.set(f"fs.azure.sas.{SOURCE_CONTAINER}.{SOURCE_ACCOUNT}.blob.core.windows.net", SOURCE_SAS)
    
    # Configuração explícita para ABFSS SAS Provider
    spark.conf.set(f"fs.azure.account.auth.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)

if TARGET_SAS:
    # Use WASBS for stability with SAS on Local/Windows
    spark.conf.set(f"fs.azure.sas.{TARGET_CONTAINER}.{TARGET_ACCOUNT}.blob.core.windows.net", TARGET_SAS)
    TARGET_ABFSS_PATH = f"wasbs://{TARGET_CONTAINER}@{TARGET_ACCOUNT}.blob.core.windows.net"
    
    # Configuração legado (ABFSS) mantida apenas se necessário, mas path aponta para WASBS
    spark.conf.set(f"fs.azure.account.auth.type.{TARGET_ACCOUNT}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{TARGET_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{TARGET_ACCOUNT}.dfs.core.windows.net", TARGET_SAS)
    
else:
    # Se não tem SAS (Account Key?), usa ABFSS
    TARGET_ABFSS_PATH = f"abfss://{TARGET_CONTAINER}@{TARGET_ACCOUNT}.dfs.core.windows.net"

SOURCE_ABFSS_PATH = f"abfss://{SOURCE_CONTAINER}@{SOURCE_ACCOUNT}.dfs.core.windows.net"

# =============================================================================
# CARREGAMENTO DE SCHEMA
# =============================================================================
schema_path = os.path.join(project_root, 'docs', 'schemas', 'balanca_schema.json')
try:
    with open(schema_path, 'r', encoding='utf-8') as f:
        full_schema = json.load(f)
    logger.info(f"Schema carregado.")
except Exception as e:
    logger.error(f"Erro ao carregar schema: {e}")
    full_schema = {}

def get_mapping_for_file(filename):
    """
    Retorna o dicionário de mapeamento (col_origem -> col_destino) para o arquivo.
    Suporta correspondência exata, por prefixo e insensível ao ano (ex: IMP_2022_MUN -> IMP_2021_MUN).
    """
    name_no_ext = os.path.splitext(filename)[0]
    
    # 1. Match exato
    if name_no_ext in full_schema:
        return full_schema[name_no_ext]
    
    # 2. Match removendo o ano (IMP_2022_MUN -> IMP_MUN vs IMP_2021_MUN -> IMP_MUN)
    # Remove _20xx ou _19xx
    def strip_year(s):
        return re.sub(r'_\d{4}', '', s)
    
    target_base = strip_year(name_no_ext)
    
    # Procura chaves que tenham a mesma base
    for key in full_schema.keys():
        if strip_year(key) == target_base:
            return full_schema[key]

    # 3. Match de prefixo (fallback)
    sorted_keys = sorted(full_schema.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if name_no_ext.startswith(key):
            return full_schema[key]
            
    return None

# =============================================================================
# LÓGICA DE INGESTÃO
# =============================================================================
# COMMAND ----------

arquivos_para_ignorar = [] #["NBM.csv", "NBM_NCM.csv"]

logger.info(f"Listando arquivos em: {SOURCE_CONTAINER} (via Azure SDK)")

# Substituição do Hadoop FS pelo Azure SDK (ContainerClient) para maior estabilidade local
try:
    if config.LANDING_ACCOUNT_KEY:
        account_url = f"https://{SOURCE_ACCOUNT}.blob.core.windows.net"
        container_client = ContainerClient(account_url=account_url, container_name=SOURCE_CONTAINER, credential=config.LANDING_ACCOUNT_KEY)
        logger.info("Autenticado com Account Key.")
    else:
        # SAS Token fallback
        container_url = f"https://{SOURCE_ACCOUNT}.blob.core.windows.net/{SOURCE_CONTAINER}?{SOURCE_SAS}"
        container_client = ContainerClient.from_container_url(container_url)
        logger.info("Autenticado com SAS Token.")

    blobs = container_client.list_blobs()
    arquivos = []
    for blob in blobs:
        arquivos.append((blob.name, blob.name)) # path (name key), name

except Exception as e:
    logger.error(f"Erro ao listar blobs: {e}")
    raise e

# Filtra arquivos relevantes com validação e logging
arquivos_filtrados = []
logger.info(f"Iniciando validação de {len(arquivos)} arquivos encontrados...")

for path, name in arquivos:
    # 1. Verifica lista de ignorados explícita
    if name in arquivos_para_ignorar:
        logger.info(f"   [IGNORADO] Arquivo na lista de exclusão: {name}")
        continue
        
    # 2. Valida extensão suportada
    _, ext = os.path.splitext(name)
    if ext.lower() not in SmartFileLoader.SUPPORTED_EXTENSIONS:
        logger.warning(f"   [IGNORADO] Extensão não suportada ({ext}): {name}")
        continue
        
    # Se passou, adiciona para processamento
    arquivos_filtrados.append((path, name))

logger.info(f"Arquivos válidos para ingestão: {len(arquivos_filtrados)}")

# Loop com TQDM
TEMP_DIR = os.path.join(tempfile.gettempdir(), "balanca_ingestion")
os.makedirs(TEMP_DIR, exist_ok=True)

pbar = tqdm.tqdm(arquivos_filtrados, desc="Ingestão Bronze")

for blob_name, name in pbar:
    pbar.set_description(f"Ingerindo: {name}")
    
    # Cria diretório de staging para este arquivo
    file_temp_dir = os.path.join(TEMP_DIR, f"staging_{name}")
    os.makedirs(file_temp_dir, exist_ok=True)
    local_path = os.path.join(file_temp_dir, name)
    
    try:
        # 1. Download do arquivo (Azure -> Local) para inspeção e leitura
        # Usando Azure SDK em vez de Hadoop FS
        with open(local_path, "wb") as f:
            download_stream = container_client.download_blob(blob_name)
            f.write(download_stream.readall())
        
        # 2. Inspeção Inteligente (SmartFileLoader)
        loader = SmartFileLoader(temp_dir=file_temp_dir)
        file_info = loader.inspect_and_prepare(local_path)

        if file_info.get('format') == 'unknown':
            logger.error(f"   ❌ Erro de validação: Formato ou conteúdo não suportado para {name}")
            continue

        # Determina nome da pasta destino usando o arquivo efetivamente processado (ex: extraído do ZIP)
        effective_filename = os.path.basename(file_info['path'])
        
        nome_base = os.path.splitext(effective_filename)[0]
        # Remove ano para agrupar (ex: EXP_2021 -> exp)
        nome_limpo = re.sub(r'_\d{4}', '', nome_base) 
        nome_pasta_assunto = nome_limpo.replace("__", "_").strip("_").lower()
        nome_pasta_raw = f"balancacomercial/{nome_pasta_assunto}"
        
        # Ajuste de Encoding (Dados de governo BR costumam ser Latin1)
        # SmartFileLoader detecta delimitador, mas assumimos Latin1 para Balança
        if file_info.get('format') == 'csv':
            file_info['options']['encoding'] = 'ISO-8859-1'

        # 3. Leitura com Spark (Lê do disco local processado/extraído)
        spark_path = local_path # file_info['path']
        
        # [DATABRICKS COMPATIBILITY]
        # Se estiver no Databricks, usamos Auto Loader se possível ou leitura direta via DBFS
        is_databricks = ("DATABRICKS_RUNTIME_VERSION" in os.environ or os.path.exists("/dbfs")) and os.name != 'nt'
        
        # DataFrame a ser lido
        df_temp = None

        if is_databricks:
            # -------------------------------------------------------------------------
            # ESTRATÉGIA DATABRICKS: AUTO LOADER (cloudFiles)
            # -------------------------------------------------------------------------
            logger.info(f"   [Databricks] Usando Auto Loader (cloudFiles) para ingestão otimizada: {name}")
            
            # Para o Auto Loader, precisamos apontar para a pasta onde os arquivos chegam no Lake
            # Mas aqui estamos lendo de um arquivo processado localmente/staging.
            # O Auto Loader é ideal para ler diretamente do Container de Origem (Landing).
            # Como este script faz download + processamento local (SmartFileLoader), 
            # o uso "puro" do Auto Loader apontando para a Landing exigiria reimplementar a lógica de unzip/tratamento.
            
            # DADO QUE a arquitetura atual baixa e pré-processa localmente (unzip/fix),
            # vamos usar a leitura padrão Spark, mas otimizada para Databricks (sem Auto Loader neste ponto específico do fluxo),
            # pois o Auto Loader não suporta ler de sistema de arquivos local do driver eficientemente em modo streaming
            # para arquivos temporários que são deletados logo depois.
            
            # AJUSTE: Para este script híbrido que faz download manual, manteremos a leitura Batch.
            # Se quiséssemos Auto Loader puro, teríamos que apontar direto para o Blob Storage e usar
            # cloudFiles.format="binary" (para zips) ou lidar com a extração via UDFs complexas.
            
            # Portanto, para manter a consistência com o SmartFileLoader existente:
            try:
                fname = os.path.basename(spark_path)
                dbfs_bridge_path = f"dbfs:/tmp/balanca_bridge/{fname}"
                
                try:
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    src_path_with_schema = f"file:{spark_path}" if not spark_path.startswith("file:") else spark_path
                    dbutils.fs.cp(src_path_with_schema, dbfs_bridge_path)
                    spark_path = dbfs_bridge_path
                except ImportError:
                     if os.path.exists("/dbfs"):
                        dbfs_dir = os.path.join("/dbfs", "tmp", "balanca_bridge")
                        os.makedirs(dbfs_dir, exist_ok=True)
                        dbfs_path_os = os.path.join(dbfs_dir, fname)
                        shutil.copy2(spark_path, dbfs_path_os)
                        spark_path = f"dbfs:/tmp/balanca_bridge/{fname}"
                     else:
                        spark_path = f"file://{spark_path}"

            except Exception as e:
                logger.warning(f"   [Databricks] Falha na ponte Local-DBFS ({e}). Tentando leitura direta.")
                spark_path = f"file://{spark_path}"

            logger.info(f"   Lendo arquivo em: {spark_path}")
            df_temp = spark.read.format(file_info['format']).options(**file_info['options']).load(spark_path)

        else:
            # -------------------------------------------------------------------------
            # ESTRATÉGIA LOCAL: Leitura Padrão
            # -------------------------------------------------------------------------
            logger.info(f"   [Local] Lendo arquivo em: {spark_path}")
            df_temp = spark.read.format(file_info['format']).options(**file_info['options']).load(spark_path)
        
        # Aplicar mapeamento de schema se existir
        
        # Aplicar mapeamento de schema se existir
        mapping = get_mapping_for_file(effective_filename)
        
        if mapping:
            # logger.info(f"   Mapeando {len(mapping)} colunas pelo schema...")
            # Seleciona e renomeia apenas colunas presentes no mapping e no DF
            cols_to_select = []
            for old_col, new_col in mapping.items():
                if old_col in df_temp.columns:
                    cols_to_select.append(F.col(old_col).alias(new_col))
                else:
                    pass # Coluna do schema não existe no CSV, ignorar ou logar
            
            if cols_to_select:
                df_temp = df_temp.select(*cols_to_select)
        else:
            # logger.warning("   ⚠️ Schema não encontrado para este arquivo. Apenas normalizando nomes.")
            # Normalização fallback
            for col_name in df_temp.columns:
                novo_nome = col_name.lower().replace(" ", "_")
                df_temp = df_temp.withColumnRenamed(col_name, novo_nome)

        # Escrita Delta
        path_destino = f"{TARGET_ABFSS_PATH}/{nome_pasta_raw}"
        
        (df_temp.write 
            .format("delta") 
            .mode("append") 
            .option("mergeSchema", "true") 
            .save(path_destino) 
        )
        logger.info(f"   Salvo com sucesso em: {path_destino}")
        
    except Exception as e:
        logger.error(f"   Erro ao processar {name}: {e}")
    finally:
        # Limpeza do staging
        shutil.rmtree(file_temp_dir, ignore_errors=True)

logger.info("--- Processo de Ingestão Finalizado ---")
