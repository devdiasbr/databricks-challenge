# Databricks notebook source
# MAGIC %md
# MAGIC # Transformação Silver: Dados CNPJ
# MAGIC 
# MAGIC Aplicação de schema, limpeza e padronização dos dados de CNPJ (Bronze -> Silver).

import os
import sys
import json
import logging
import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, count, lit, upper
from dotenv import load_dotenv
import unicodedata
from azure.storage.blob import ContainerClient

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de caminhos
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

project_root = base_dir
# Ajuste simples para encontrar a raiz do projeto
if os.path.basename(project_root) == "scripts":
    project_root = os.path.dirname(os.path.dirname(project_root))
elif os.path.basename(project_root) == "src":
    project_root = os.path.dirname(project_root)

# Adiciona src ao path
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.append(src_path)

import utils.config as config
from utils.transformations import BaseTransform, normalize_column_name
from utils.logging_utils import TqdmLoggingHandler

# Configura Logger Global
logger = logging.getLogger("BronzeToSilver_CNPJ")
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




def list_raw_folders_cnpj(spark=None, protocol=None):
    """
    Lista as pastas CNPJ dentro da pasta 'cnpj' no container RAW.
    Suporta Databricks (dbutils) e Local (Azure SDK).
    """
    known_cnpj_entities = [
        "empresas", "estabelecimentos", "socios", "cnaes", 
        "paises", "naturezas", "municipios", "simples", 
        "motivos", "qualificacoes"
    ]

    # 1. Tentar via dbutils (Databricks)
    try:
        from pyspark.dbutils import DBUtils
        if spark:
            try:
                dbutils = DBUtils(spark)
                # Constrói o path
                base_path = config.get_base_path("raw", "target", protocol)
                cnpj_path = f"{base_path}/cnpj/"
                
                logger.info(f"Listando pastas via dbutils em: {cnpj_path}")
                paths = dbutils.fs.ls(cnpj_path)
                
                found_folders = []
                for p in paths:
                    name = p.name.strip('/')
                    if name in known_cnpj_entities:
                        found_folders.append(name)
                return sorted(found_folders)
            except Exception as e:
                logger.warning(f"Falha ao listar via dbutils ({e}). Tentando fallback Azure SDK.")
    except ImportError:
        pass

    # 2. Fallback: Azure SDK (ContainerClient) - Local ou Databricks sem mount
    logger.info("Listando pastas via Azure SDK (ContainerClient)...")
    
    target_key = config.get_config("AZURE_STORAGE_ACCOUNT_KEY_TARGET", secret_key=config.KV_SECRET_TARGET)
    target_account = config.TARGET_ACCOUNT
    
    folders = set()
    
    try:
        if target_key:
             account_url = f"https://{target_account}.blob.core.windows.net"
             container_client = ContainerClient(account_url=account_url, container_name="raw", credential=target_key)
        else:
             # Fallback para SAS
             raw_container_url = config.get_target_url("raw")
             if not raw_container_url:
                 logger.warning("Sem credenciais (Key/SAS) para listar pastas.")
                 return []
             container_client = ContainerClient.from_container_url(raw_container_url)
        
        # Lista blobs com prefixo
        blobs = container_client.list_blobs(name_starts_with="cnpj/")
        
        for blob in blobs:
            name = blob.name 
            parts = name.split('/')
            if len(parts) > 1:
                subfolder = parts[1]
                if subfolder in known_cnpj_entities:
                    folders.add(subfolder)
                    
    except Exception as e:
        logger.error(f"Erro ao listar via SDK: {e}")
        return []

    return sorted(list(folders))


# Função delete_virtual_directory removida (depreciada)


def load_schema(schema_path):
    """Carrega o arquivo JSON de schema (tenta UTF-8, fallback para Latin1)."""
    try:
        # Tenta primeiro UTF-8 (padrão JSON)
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except UnicodeDecodeError:
        logger.warning(f"⚠️ Aviso: Falha com UTF-8, tentando Latin1 para {schema_path}")
        # Fallback para Latin1 (ISO-8859-1)
        with open(schema_path, 'r', encoding='latin1') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"❌ Erro ao carregar schema JSON de {schema_path}: {e}")
        return {}


def get_schema_mapping(folder_name, schema):
    """
    Retorna a lista de colunas para a pasta.
    Mapeia nomes de pasta (empresas) para chaves do JSON (EMPRECSV).
    """
    folder_lower = folder_name.lower()
    
    # Mapa de Folder -> JSON Key
    mapping_keys = {
        "empresas": "EMPRECSV",
        "estabelecimentos": "ESTABELE",
        "socios": "SOCIOCSV",
        "cnaes": "CNAECSV",
        "municipios": "MUNICIPIOS", 
        "naturezas": "NATUREZAS",
        "paises": "PAISES",
        "simples": "SIMPLES",
        "motivos": "MOTIVOS",
        "qualificacoes": "QUALIFICACOES"
    }
    
    key = mapping_keys.get(folder_lower)
    
    if key and key in schema:
        logger.info(f"  🔍 Schema selecionado: {key}")
        return schema[key]
    
    logger.warning(f"  ⚠️ Nenhum schema específico encontrado para '{folder_name}'. Usando normalização padrão.")
    return []


def process_cnpj():
    logger.info(f"\n🚀 Iniciando processamento CNPJ: Bronze -> Silver")
    
    # 1. Inicializar Spark (Prioritário para configurar acesso e Key Vault)
    spark = get_spark_session()
    protocol = config.configure_spark_access(spark)
    
    # Carregar Schema usando o project_root global calculado no início do script
    schema_path = os.path.join(project_root, 'docs', 'schemas', 'cnpj_schema.json')
    
    logger.info(f"📄 Carregando schema de: {schema_path}")
    full_schema = load_schema(schema_path)

    # 2. Identificar pastas para processar
    try:
        folders = list_raw_folders_cnpj(spark, protocol)
        logger.info(f"📂 Pastas CNPJ encontradas em 'raw': {folders}")
    except Exception as e:
        logger.error(f"❌ Erro ao listar pastas: {e}")
        spark.stop()
        return

    if not folders:
        logger.warning("⚠️ Nenhuma pasta CNPJ encontrada para processar.")
        spark.stop()
        return
    
    # Extrai account para montar URL baseada no protocolo
    account = config.TARGET_ACCOUNT
    
    base_url_raw = config.get_base_path("raw", "target", protocol)
    base_url_trusted = config.get_base_path("trusted", "target", protocol)

    # 3. Processar cada pasta individualmente
    pbar = tqdm.tqdm(folders, desc="Processando CNPJ")
    for folder_name in pbar:
        pbar.set_description(f"Processando: {folder_name}")
        
        source_path = f"{base_url_raw}/cnpj/{folder_name}"
        target_path = f"{base_url_trusted}/cnpj/{folder_name}"
        
        try:
            logger.info(f"  📂 Lendo dados de: {source_path}")
            
            # Tenta ler como Delta/Parquet
            try:
                df = spark.read.format("delta").load(source_path)
            except:
                logger.warning("  ⚠️ Falha ao ler como Delta, tentando Parquet...")
                df = spark.read.option("recursiveFileLookup", "true").parquet(source_path)
            
            # count_records = df.count()
            # logger.info(f"  📊 Registros encontrados: {count_records}")
            
            # if count_records == 0:
            #     logger.warning("  ⚠️ Pasta vazia ou sem dados válidos. Pulando.")
            #     continue

            # --- Transformações ---
            
            mapping_list = get_schema_mapping(folder_name, full_schema)

            logger.info("  🔧 Aplicando pipeline de transformações (BaseTransform)...")
            
            transformer = BaseTransform(df)
            transformer.normalize_headers()
            
            df_clean = (transformer
                        .drop_foreign_columns()
                        .trim_strings()
                        .upper_strings()
                        .drop_duplicates()
                        .treat_nulls()
                        .get_dataframe())
            
            # --- Escrita ---
            
            # if trusted_url:
            #     delete_virtual_directory(trusted_url, f"cnpj/{folder_name}")

            logger.info(f"  💾 Salvando em: {target_path} (Delta)")
            
            writer = df_clean.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true")
                
            writer.save(target_path)
            
            # Otimização Delta
            logger.info("  ⚡ Executando OPTIMIZE e VACUUM...")
            try:
                # Configura tamanho alvo do arquivo para OPTIMIZE (10MB)
                spark.conf.set("spark.databricks.delta.optimize.maxFileSize", config.DELTA_OPTIMIZE_FILE_SIZE)

                spark.sql(f"OPTIMIZE delta.`{target_path}`")
                spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                spark.sql(f"VACUUM delta.`{target_path}` RETAIN {config.DELTA_VACUUM_RETENTION_DAYS * 24} HOURS")
            except Exception as e:
                logger.warning(f"  ⚠️ Otimização não executada: {e}")

            logger.info(f"  ✅ Pasta {folder_name} concluída!")
            
        except Exception as e:
            logger.error(f"  ❌ Erro ao processar pasta {folder_name}: {str(e)}")

    logger.info("\n🏁 Processamento CNPJ finalizado.")
    try:
        spark.stop()
    except Exception:
        pass


if __name__ == "__main__":
    process_cnpj()
