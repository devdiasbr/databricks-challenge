# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão Bronze: Dados Públicos CNPJ
# MAGIC 
# MAGIC Ingestão e extração de dados públicos do CNPJ (ZIPs) para a camada Bronze.

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

"""
Script de Ingestão Bronze para dados públicos do CNPJ.
Responsável por extrair arquivos ZIP da camada Landing e carregar para a camada Bronze usando Autoloader.
"""

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

project_root = base_dir
while not os.path.exists(os.path.join(project_root, 'src')) and project_root != os.path.dirname(project_root):
    project_root = os.path.dirname(project_root)

if not os.path.exists(os.path.join(project_root, 'src')):
    project_root = base_dir

src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.append(src_dir)

from utils.file_validator import SmartFileLoader

def get_spark_session():
    """Obtém ou cria a sessão Spark ativa."""
    return SparkSession.builder.getOrCreate()

spark = get_spark_session()
spark.sparkContext.setLogLevel("WARN")

log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getLogger("org.apache.spark").setLevel(log4j.Level.WARN)
log4j.LogManager.getLogger("org.apache.hadoop").setLevel(log4j.Level.WARN)
log4j.LogManager.getLogger("py4j").setLevel(log4j.Level.ERROR)

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except ImportError:
    raise ImportError("Este script deve ser executado no Databricks (DBUtils required).")
from utils import config

protocol = config.configure_spark_access(spark)

SOURCE_ACCOUNT = config.SOURCE_ACCOUNT
SOURCE_SAS = config.SAS_TOKEN_CNPJ
SOURCE_KEY = config.LANDING_ACCOUNT_KEY

SOURCE_CONTAINER = "cnpj"
TARGET_ACCOUNT = config.TARGET_ACCOUNT
TARGET_CONTAINER = "raw"

SOURCE_ABFSS_PATH = config.get_base_path(SOURCE_CONTAINER, "landing", protocol)
TARGET_ABFSS_PATH = f"{config.get_base_path(TARGET_CONTAINER, 'target', protocol)}/cnpj"

FORCE_FULL_LOAD = True
LOGS_CONTAINER = "$logs"
LOGS_ABFSS_PATH = config.get_base_path(LOGS_CONTAINER, "target", protocol)

# Configuração para evitar erro de "Failed to get primary group" no Windows
spark.conf.set("fs.azure.enable.check.access", "false")
spark.conf.set("fs.azure.skipUserGroupMetadataDuringInitialization", "true")

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
    "EMPRECSV": "Empresas*.csv",
    "ESTABELE": "Estabelecimentos*.csv",
    "SOCIOCSV": "Socios*.csv",
    "SIMPLES": "Simples*.csv",
    "CNAECSV": "Cnaes*.csv",
    "MOTIVOS": "Motivos*.csv",
    "MUNICIPIOS": "Municipios*.csv",
    "NATUREZAS": "Naturezas*.csv",
    "PAISES": "Paises*.csv",
    "QUALIFICACOES": "Qualificacoes*.csv"
}

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

FORCE_FULL_LOAD = True
processed_folders = set()

schema_path = os.path.join(project_root, 'docs', 'schemas', 'cnpj_schema.json')
try:
    with open(schema_path, 'r', encoding='utf-8') as f:
        COLUMN_NAMES = json.load(f)
    print(f"Schema carregado de: {schema_path}")
except Exception as e:
    print(f"Erro ao carregar schema CNPJ: {e}")
    COLUMN_NAMES = {}

class BlobStorageHandler(logging.Handler):
    """Handler de log customizado para salvar logs no Azure Blob Storage."""
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
        """Faz o upload do buffer de log para o Blob Storage."""
        try:
            os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
            with open(self.log_file_path, 'w', encoding='utf-8') as f:
                f.write(self.log_buffer.getvalue())
            dbutils.fs.cp(f"file:{self.log_file_path}", blob_path)
            print(f"Logs saved to: {blob_path}")
        except Exception as e:
            print(f"Failed to save logs to blob: {str(e)}")

def setup_logging(pipeline_run_id: str):
    """Configura logging para console e Blob Storage."""
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

# COMMAND ----------

def ingest_cnpj_autoloader(entity_key, file_pattern, logger):
    """
    Ingestão incremental de ZIPs do CNPJ usando Autoloader.
    
    Args:
        entity_key (str): Chave da entidade no mapeamento.
        file_pattern (str): Padrão de glob para os arquivos ZIP.
        logger: Objeto de logging.
    """
    entity_name = ENTITY_FOLDER_MAP.get(entity_key, entity_key.lower())
    source_path = SOURCE_ABFSS_PATH
    target_path = f"{TARGET_ABFSS_PATH}/{entity_name}"
    checkpoint_path = f"{TARGET_ABFSS_PATH}/_checkpoints/{entity_name}"
    
    logger.info(f"🚀 [Autoloader] Processando CNPJ: {entity_name} ({file_pattern})")
    
    df_zips = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", file_pattern)
        .load(source_path)
    )
    
    def process_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
            
        for row in batch_df.collect():
            zip_path = row.path
            logger.info(f"   📦 Extraindo: {zip_path}")
            
            temp_zip = os.path.join(TMP_EXTRACT_DIR, os.path.basename(zip_path))
            os.makedirs(TMP_EXTRACT_DIR, exist_ok=True)
            
            dbutils.fs.cp(zip_path, f"file:{temp_zip}")
            
            with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
                zip_ref.extractall(TMP_EXTRACT_DIR)
                extracted_files = zip_ref.namelist()
                
                for f in extracted_files:
                    local_extracted = os.path.join(TMP_EXTRACT_DIR, f)
                    
                    df_extracted = (spark.read
                        .format("csv")
                        .option("header", "false")
                        .option("delimiter", CSV_DELIMITER)
                        .option("encoding", CSV_ENCODING)
                        .load(f"file:{local_extracted}")
                    )
                    
                    cols = COLUMN_NAMES.get(entity_key, [])
                    if cols:
                        current_cols = df_extracted.columns
                        if len(current_cols) > len(cols):
                            final_cols = cols + [f"_c{i}" for i in range(len(cols), len(current_cols))]
                            df_extracted = df_extracted.toDF(*final_cols)
                        else:
                            df_extracted = df_extracted.toDF(*cols[:len(current_cols)])
                    
                    (df_extracted.write
                        .format("delta")
                        .mode("append")
                        .option("mergeSchema", "true")
                        .save(target_path)
                    )
                    
                    os.remove(local_extracted)
            
            os.remove(temp_zip)

    query = (df_zips.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    
    query.awaitTermination()
    logger.info(f"✅ Ingestão de {entity_name} concluída.")

def run_pipeline():
    """Executa o pipeline completo de ingestão do CNPJ."""
    start_time = datetime.now()
    pipeline_run_id = start_time.strftime('%Y%m%d_%H%M%S')
    logger, blob_handler = setup_logging(pipeline_run_id)
    
    logger.info("="*80)
    logger.info("STARTING CNPJ BRONZE INGESTION")
    logger.info(f"Run ID: {pipeline_run_id}")
    logger.info("="*80)
    
    logger.info(f"📂 Diretório de Download (ZIPs): {TMP_EXTRACT_DIR}")
    logger.info(f"📂 Diretório de Staging (Extração): {DBFS_STAGING_DIR}")
    logger.info(f"📂 Diretório de Logs: {TMP_LOG_DIR}")
    
    total_entities = len(CSV_PATTERNS)
    logger.info(f"Processing {total_entities} entities using Autoloader...")
    for entity_key, pattern in CSV_PATTERNS.items():
        zip_pattern = pattern.replace(".csv", ".zip")
        try:
            ingest_cnpj_autoloader(entity_key, zip_pattern, logger)
        except Exception as e:
            logger.error(f"❌ Erro ao processar {entity_key}: {e}")
            
    logger.info("PIPELINE COMPLETED")
    blob_handler.flush_to_blob(f"{LOGS_ABFSS_PATH}/cnpj_pipeline_{pipeline_run_id}.log")

if __name__ == "__main__":
    run_pipeline()
