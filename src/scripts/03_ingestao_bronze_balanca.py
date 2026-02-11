# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão Bronze: Balança Comercial
# MAGIC 
# MAGIC Ingestão de dados da Balança Comercial para a camada Bronze (Raw -> Bronze).
# MAGIC Execução exclusiva para ambiente Databricks.

# COMMAND ----------

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
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

# Navega para cima até encontrar a pasta 'src'
project_root = base_dir
while not os.path.exists(os.path.join(project_root, 'src')) and project_root != os.path.dirname(project_root):
    project_root = os.path.dirname(project_root)

if not os.path.exists(os.path.join(project_root, 'src')):
    project_root = base_dir

src_path = os.path.join(project_root, "src")
if src_path not in sys.path:
    sys.path.append(src_path)

try:
    from utils.logging_utils import TqdmLoggingHandler
except ImportError:
    class TqdmLoggingHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                print(msg) # Simple fallback
            except Exception:
                self.handleError(record)

logger = logging.getLogger("IngestaoBronzeBalanca")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TqdmLoggingHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(handler)

import utils.config as config
from utils.file_validator import SmartFileLoader
from azure.storage.blob import ContainerClient

# =============================================================================
# INITIALIZE SPARK SESSION
# =============================================================================
# COMMAND ----------

def get_spark_session():
    # No Databricks, a sessão já existe.
    return SparkSession.builder.getOrCreate()

spark = get_spark_session()

# =============================================================================
# CONFIGURAÇÕES DE STORAGE E ACESSO
# =============================================================================
# COMMAND ----------

# Configura acesso via Key Vault (Secret Scopes)
protocol = config.configure_spark_access(spark)

SOURCE_CONTAINER = "balancacomercial"
SOURCE_ACCOUNT = config.SOURCE_ACCOUNT
SOURCE_SAS = config.SAS_TOKEN_BALANCA

TARGET_CONTAINER = "raw"

# Define caminhos base
SOURCE_ABFSS_PATH = config.get_base_path(SOURCE_CONTAINER, "landing", protocol)
TARGET_ABFSS_PATH = config.get_base_path(TARGET_CONTAINER, "target", protocol)

# =============================================================================
# CARREGAMENTO DE SCHEMA
# =============================================================================
# COMMAND ----------

schema_path = os.path.join(project_root, 'docs', 'schemas', 'balanca_schema.json')
try:
    with open(schema_path, 'r', encoding='utf-8') as f:
        full_schema = json.load(f)
    logger.info(f"Schema carregado.")
except Exception as e:
    logger.error(f"Erro ao carregar schema: {e}")
    full_schema = {}

# COMMAND ----------

def get_mapping_for_file(filename):
    """
    Retorna o dicionário de mapeamento (col_origem -> col_destino) para o arquivo.
    """
    name_no_ext = os.path.splitext(filename)[0]
    
    # 1. Match exato
    if name_no_ext in full_schema:
        return full_schema[name_no_ext]
    
    # 2. Match removendo o ano
    def strip_year(s):
        return re.sub(r'_\d{4}', '', s)
    
    target_base = strip_year(name_no_ext)
    
    for key in full_schema.keys():
        if strip_year(key) == target_base:
            return full_schema[key]

    # 3. Match de prefixo
    sorted_keys = sorted(full_schema.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if name_no_ext.startswith(key):
            return full_schema[key]
            
    return None

# =============================================================================
# LÓGICA DE INGESTÃO
# =============================================================================
# COMMAND ----------

arquivos_para_ignorar = []

logger.info(f"Listando arquivos em: {SOURCE_CONTAINER} (via Azure SDK)")

try:
    # Tenta usar credenciais do Key Vault (configuradas no config.py)
    if config.LANDING_ACCOUNT_KEY:
        account_url = f"https://{SOURCE_ACCOUNT}.blob.core.windows.net"
        container_client = ContainerClient(account_url=account_url, container_name=SOURCE_CONTAINER, credential=config.LANDING_ACCOUNT_KEY)
        logger.info("Autenticado com Account Key (Key Vault/Env).")
    else:
        # Fallback para SAS Token
        container_url = f"https://{SOURCE_ACCOUNT}.blob.core.windows.net/{SOURCE_CONTAINER}?{SOURCE_SAS}"
        container_client = ContainerClient.from_container_url(container_url)
        logger.info("Autenticado com SAS Token.")

    blobs = container_client.list_blobs()
    arquivos = []
    for blob in blobs:
        arquivos.append((blob.name, blob.name))

except Exception as e:
    logger.error(f"Erro ao listar blobs: {e}")
    raise e

arquivos_filtrados = []
logger.info(f"Iniciando validação de {len(arquivos)} arquivos encontrados...")

for path, name in arquivos:
    if name in arquivos_para_ignorar:
        continue
    _, ext = os.path.splitext(name)
    if ext.lower() not in SmartFileLoader.SUPPORTED_EXTENSIONS:
        continue
    arquivos_filtrados.append((path, name))

logger.info(f"Arquivos válidos para ingestão: {len(arquivos_filtrados)}")

# Diretório temporário local (Driver do Databricks)
TEMP_DIR = os.path.join(tempfile.gettempdir(), "balanca_ingestion")
os.makedirs(TEMP_DIR, exist_ok=True)

pbar = tqdm.tqdm(arquivos_filtrados, desc="Ingestão Bronze")

for blob_name, name in pbar:
    pbar.set_description(f"Ingerindo: {name}")
    
    file_temp_dir = os.path.join(TEMP_DIR, f"staging_{name}")
    os.makedirs(file_temp_dir, exist_ok=True)
    local_path = os.path.join(file_temp_dir, name)
    
    try:
        # 1. Download (Azure -> Driver Local)
        with open(local_path, "wb") as f:
            download_stream = container_client.download_blob(blob_name)
            f.write(download_stream.readall())
        
        # 2. Inspeção e Preparação (SmartFileLoader)
        loader = SmartFileLoader(temp_dir=file_temp_dir)
        file_info = loader.inspect_and_prepare(local_path)

        if file_info.get('format') == 'unknown':
            logger.error(f"   ❌ Erro de validação: Formato não suportado para {name}")
            continue

        effective_filename = os.path.basename(file_info['path'])
        nome_base = os.path.splitext(effective_filename)[0]
        nome_limpo = re.sub(r'_\d{4}', '', nome_base) 
        nome_pasta_assunto = nome_limpo.replace("__", "_").strip("_").lower()
        nome_pasta_raw = f"balancacomercial/{nome_pasta_assunto}"
        
        if file_info.get('format') == 'csv':
            file_info['options']['encoding'] = 'ISO-8859-1'

        spark_path = local_path 
        
        # 3. Mover para DBFS (Obrigatório para Spark Cluster ler do Driver)
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            
            fname = os.path.basename(spark_path)
            dbfs_bridge_path = f"dbfs:/tmp/balanca_bridge/{fname}"
            
            # Copia do Driver (file:) para DBFS (dbfs:)
            src_path_with_schema = f"file:{spark_path}" if not spark_path.startswith("file:") else spark_path
            dbutils.fs.cp(src_path_with_schema, dbfs_bridge_path)
            
            spark_path = dbfs_bridge_path
            logger.info(f"   Arquivo movido para DBFS: {spark_path}")
            
        except ImportError:
            logger.error("   ❌ DBUtils não disponível. Este script requer ambiente Databricks.")
            continue

        # 4. Leitura e Escrita
        df_temp = spark.read.format(file_info['format']).options(**file_info['options']).load(spark_path)

        mapping = get_mapping_for_file(effective_filename)
        if mapping:
            cols_to_select = []
            for old_col, new_col in mapping.items():
                if old_col in df_temp.columns:
                    cols_to_select.append(F.col(old_col).alias(new_col))
            if cols_to_select:
                df_temp = df_temp.select(*cols_to_select)
        else:
            for col_name in df_temp.columns:
                novo_nome = col_name.lower().replace(" ", "_")
                df_temp = df_temp.withColumnRenamed(col_name, novo_nome)

        path_destino = f"{TARGET_ABFSS_PATH}/{nome_pasta_raw}"
        
        (df_temp.write 
            .format("delta") 
            .mode("append") 
            .option("mergeSchema", "true") 
            .save(path_destino) 
        )
        logger.info(f"   Salvo em: {path_destino}")
        
    except Exception as e:
        logger.error(f"   Erro ao processar {name}: {e}")
    finally:
        shutil.rmtree(file_temp_dir, ignore_errors=True)
        # Opcional: Limpar DBFS bridge também se desejar economizar espaço, mas /tmp é limpo eventualmente.

logger.info("--- Processo de Ingestão Finalizado ---")
