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

"""
Script de Ingestão Bronze para dados da Balança Comercial.
Responsável por mover dados da camada Landing para a camada Bronze usando Databricks Autoloader.
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

src_path = os.path.join(project_root, "src")
if src_path not in sys.path:
    sys.path.append(src_path)

import utils.config as config
from utils.file_validator import SmartFileLoader

logger = logging.getLogger("IngestaoBronzeBalanca")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TqdmLoggingHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(handler)

def get_spark_session():
    """Obtém ou cria a sessão Spark ativa."""
    return SparkSession.builder.getOrCreate()

spark = get_spark_session()

# Configurações de Acesso
protocol = config.configure_spark_access(spark)

SOURCE_CONTAINER = "balancacomercial"
SOURCE_ACCOUNT = config.SOURCE_ACCOUNT
SOURCE_SAS = config.SAS_TOKEN_BALANCA
TARGET_CONTAINER = "raw"

SOURCE_ABFSS_PATH = config.get_base_path(SOURCE_CONTAINER, "landing", protocol)
TARGET_ABFSS_PATH = config.get_base_path(TARGET_CONTAINER, "target", protocol)

FORCE_FULL_LOAD = True
processed_folders = set()

schema_path = os.path.join(project_root, 'docs', 'schemas', 'balanca_schema.json')
try:
    with open(schema_path, 'r', encoding='utf-8') as f:
        full_schema = json.load(f)
    logger.info("Schema carregado.")
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

def ingest_with_autoloader(entity_name, file_pattern, schema_mapping=None):
    """
    Ingere dados usando Databricks Autoloader (cloudFiles).
    
    Args:
        entity_name (str): Nome da entidade/pasta de destino.
        file_pattern (str): Padrão de glob para identificar os arquivos.
        schema_mapping (dict, optional): Mapeamento de renomeação de colunas.
    """
    source_path = SOURCE_ABFSS_PATH
    target_path = f"{TARGET_ABFSS_PATH}/balancacomercial/{entity_name}"
    checkpoint_path = f"{TARGET_ABFSS_PATH}/_checkpoints/balanca/{entity_name}"
    
    logger.info(f"\n🚀 Iniciando Autoloader para: {entity_name}")
    logger.info(f"   Pattern: {file_pattern}")
    
    df_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .option("encoding", "ISO-8859-1")
        .option("pathGlobFilter", file_pattern)
        .load(source_path)
    )
    
    if schema_mapping:
        cols_to_select = []
        for old_col, new_col in schema_mapping.items():
            cols_to_select.append(F.col(old_col).alias(new_col))
        df_stream = df_stream.select(*cols_to_select)
    else:
        for col_name in df_stream.columns:
            df_stream = df_stream.withColumnRenamed(col_name, col_name.lower().replace(" ", "_"))

    query = (df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(target_path)
    )
    
    query.awaitTermination()
    logger.info(f"✅ Ingestão de {entity_name} concluída com sucesso.")

entities_to_process = {
    "exp": ("EXP_*.csv", full_schema.get("EXPO_2021")),
    "imp": ("IMP_*.csv", full_schema.get("IMP_2021")),
    "exp_mun": ("EXP_*_MUN.csv", full_schema.get("EXP_2021_MUN")),
    "imp_mun": ("IMP_*_MUN.csv", full_schema.get("IMP_2021_MUN")),
    "ncm": ("NCM.csv", full_schema.get("NCM")),
    "pais": ("PAIS.csv", full_schema.get("PAIS")),
    "via": ("VIA.csv", full_schema.get("VIA")),
    "ncm_unidade": ("NCM_UNIDADE.csv", full_schema.get("NCM_UNIDADE")),
    "ncm_cgce": ("NCM_CGCE.csv", full_schema.get("NCM_CGCE")),
    "ncm_cuci": ("NCM_CUCI.csv", full_schema.get("NCM_CUCI")),
    "ncm_isic": ("NCM_ISIC.csv", full_schema.get("NCM_ISIC")),
    "ncm_itn": ("NCM_ITN.csv", full_schema.get("NCM_ITN"))
}

def run_ingestion():
    """Executa a pipeline de ingestão para todas as entidades configuradas."""
    logger.info("Iniciando Pipeline de Ingestão Bronze (Balança Comercial)...")
    for entity, (pattern, mapping) in entities_to_process.items():
        try:
            ingest_with_autoloader(entity, pattern, mapping)
        except Exception as e:
            logger.error(f"Erro ao processar {entity}: {e}")

if __name__ == "__main__":
    run_ingestion()

logger.info("--- Processo de Ingestão Autoloader Finalizado ---")
