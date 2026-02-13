"""
Script de Transformação Silver para dados da Balança Comercial.
Responsável por limpar, padronizar e tipar os dados da camada Bronze para a camada Silver (Trusted).
"""

import os
import sys
import json
import logging
import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, count, lit, upper
import unicodedata
from azure.storage.blob import ContainerClient

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

project_root = base_dir
if os.path.basename(project_root) == "scripts":
    project_root = os.path.dirname(os.path.dirname(project_root))
elif os.path.basename(project_root) == "src":
    project_root = os.path.dirname(project_root)

src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.append(src_path)

import utils.config as config
from utils.transformations import BaseTransform, normalize_column_name
from utils.logging_utils import TqdmLoggingHandler
logger = logging.getLogger("BronzeToSilver_Balanca")
logger.setLevel(logging.INFO)
if not logger.handlers:
    try:
        handler = TqdmLoggingHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
        logger.addHandler(handler)
    except Exception:
        handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(handler)

def get_spark_session():
    """Obtém a sessão Spark ativa (Databricks)."""
    return SparkSession.builder.getOrCreate()

def list_raw_folders(spark, protocol):
    """
    Lista as pastas dentro de 'balancacomercial/' no container RAW.
    Suporta Databricks (dbutils) e Local (Azure SDK).
    """
    # 1. Tentar via dbutils (Databricks)
    if config.IS_DATABRICKS:
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            
            base_path = config.get_base_path("raw", "target", protocol)
            target_path = f"{base_path}/balancacomercial/"
            
            logger.info(f"Listando pastas via dbutils em: {target_path}")
            paths = dbutils.fs.ls(target_path)
            
            folders = []
            for p in paths:
                name = p.name.strip('/')
                if '/' in name:
                    name = name.split('/')[-1]
                folders.append(name)
                
            return sorted(folders)
        except Exception as e:
            logger.warning(f"Falha ao listar via dbutils ({e}). Tentando fallback Azure SDK.")

    # 2. Fallback: Azure SDK (ContainerClient)
    logger.info("Listando pastas via Azure SDK (ContainerClient)...")
    
    target_key = config.get_config("AZURE_STORAGE_ACCOUNT_KEY_TARGET", secret_key=config.KV_SECRET_TARGET)
    target_account = config.TARGET_ACCOUNT
    
    folders = set()
    try:
        account_url = f"https://{target_account}.blob.core.windows.net"
        if target_key:
            container_client = ContainerClient(account_url=account_url, container_name="raw", credential=target_key)
        else:
            # Tenta via SAS se configurado no config.py
            raw_url = config.TARGET_RAW_URL
            if raw_url:
                container_client = ContainerClient.from_container_url(raw_url)
            else:
                logger.error("Sem credenciais (Key/URL) para listar pastas via SDK.")
                return []

        # Lista blobs com prefixo
        blobs = container_client.list_blobs(name_starts_with="balancacomercial/")
        
        for blob in blobs:
            name = blob.name 
            parts = name.split('/')
            if len(parts) > 1:
                subfolder = parts[1]
                if subfolder and subfolder != "":
                    folders.add(subfolder)
                    
    except Exception as e:
        logger.error(f"Erro ao listar via SDK: {e}")
        return []

    return sorted(list(folders))

def load_schema(schema_path):
    """Carrega o arquivo JSON de schema com tratamento de encoding."""
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except UnicodeDecodeError:
        logger.warning(f"⚠️ Aviso: Falha com UTF-8, tentando Latin1 para {schema_path}")
        with open(schema_path, 'r', encoding='latin1') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"❌ Erro ao carregar schema JSON de {schema_path}: {e}")
        return {}

def get_schema_mapping(folder_name, schema):
    """Retorna o mapeamento de colunas adequado para a pasta com base no nome."""
    folder_lower = folder_name.lower()
    
    is_mun = "_mun" in folder_lower
    is_exp = "exp" in folder_lower
    is_imp = "imp" in folder_lower
    
    key = None
    if is_mun:
        if is_exp: key = "EXP_2021_MUN"
        elif is_imp: key = "IMP_2021_MUN"
    else:
        if is_exp: key = "EXPO_2021"
        elif is_imp: key = "IMP_2021"
        
    if key and key in schema:
        logger.info(f"  🔍 Schema selecionado: {key}")
        return schema[key]
    
    logger.warning(f"  ⚠️ Nenhum schema específico encontrado para '{folder_name}'. Usando normalização padrão.")
    return {}

def process_balanca_comercial():
    """Executa o processamento completo da Balança Comercial: Bronze -> Silver."""
    logger.info(f"\n🚀 Iniciando processamento Balança Comercial: Bronze -> Silver")
    
    spark = get_spark_session()
    protocol = config.configure_spark_access(spark)

    schema_path = os.path.join(project_root, 'docs', 'schemas', 'balanca_schema.json')
    logger.info(f"📄 Carregando schema de: {schema_path}")
    full_schema = load_schema(schema_path)

    try:
        folders = list_raw_folders(spark, protocol)
        logger.info(f"📂 Pastas encontradas em 'balancacomercial/': {folders}")
    except Exception as e:
        logger.error(f"❌ Erro ao listar pastas: {e}")
        return

    if not folders:
        logger.warning("⚠️ Nenhuma pasta encontrada para processar.")
        return
    
    base_url_raw = f"{config.get_base_path('raw', 'target', protocol)}/balancacomercial"
    base_url_trusted = f"{config.get_base_path('trusted', 'target', protocol)}/balancacomercial"

    logger.info("Processando Pastas...")
    for folder_name in folders:
        logger.info(f"Processando: {folder_name}")
        
        source_path = f"{base_url_raw}/{folder_name}"
        target_path = f"{base_url_trusted}/{folder_name}"
        
        try:
            logger.info(f"  📂 Lendo dados de: {source_path}")
            
            try:
                df = spark.read.format("delta").load(source_path)
            except Exception:
                logger.warning(f"  ⚠️ Falha ao ler como Delta em {source_path}, tentando Parquet...")
                df = spark.read.option("recursiveFileLookup", "true").parquet(source_path)
            
            count_records = df.count()
            logger.info(f"  📊 Registros encontrados: {count_records}")
            
            if count_records == 0:
                logger.warning("  ⚠️ Pasta vazia ou sem dados válidos. Pulando.")
                continue

            mapping = get_schema_mapping(folder_name, full_schema)
            logger.info("  🔧 Aplicando pipeline de transformações (BaseTransform)...")
            
            transformer = BaseTransform(df)
            
            if mapping:
                transformer.rename_columns(mapping)
            else:
                transformer.normalize_headers()
            
            df_clean = (transformer
                        .trim_strings()
                        .upper_strings()
                        .drop_duplicates()
                        .treat_nulls()
                        .get_dataframe())
            
            logger.info(f"  💾 Salvando em: {target_path}")
            
            cols = df_clean.columns
            partition_cols = []
            
            potential_year_cols = ['ano', 'co_ano', 'CO_ANO']
            potential_month_cols = ['mes', 'co_mes', 'CO_MES']
            
            for c in potential_year_cols:
                if c in cols: 
                    partition_cols.append(c)
                    break
            
            for c in potential_month_cols:
                if c in cols: 
                    partition_cols.append(c)
                    break
            
            logger.info("  💾 Salvando em formato Delta...")
            writer = df_clean.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true")
            
            if partition_cols:
                logger.info(f"    Particionando por: {partition_cols}")
                writer = writer.partitionBy(*partition_cols)
                
            writer.save(target_path)
            
            logger.info("  ⚡ Executando OPTIMIZE e VACUUM...")
            try:
                spark.conf.set("spark.databricks.delta.optimize.maxFileSize", config.DELTA_OPTIMIZE_FILE_SIZE)
                spark.sql(f"OPTIMIZE delta.`{target_path}`")
                spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                spark.sql(f"VACUUM delta.`{target_path}` RETAIN {config.DELTA_VACUUM_RETENTION_DAYS * 24} HOURS")
            except Exception as e:
                logger.warning(f"  ⚠️ Otimização não executada: {e}")

            logger.info(f"  ✅ Pasta {folder_name} concluída!")
            
        except Exception as e:
            logger.error(f"  ❌ Erro ao processar pasta {folder_name}: {str(e)}")

    logger.info("\n🏁 Processamento global finalizado.")
    try:
        spark.stop()
    except Exception:
        pass

if __name__ == "__main__":
    process_balanca_comercial()
