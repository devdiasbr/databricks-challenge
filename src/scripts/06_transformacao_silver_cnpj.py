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

# Configuração do HADOOP_HOME para execução local no Windows
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(src_dir)

# Adiciona src ao path para importar utils
sys.path.append(src_dir)

hadoop_home = os.path.join(project_root, 'hadoop')
if os.path.exists(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
    # Adiciona bin ao PATH se não estiver
    hadoop_bin = os.path.join(hadoop_home, 'bin')
    if hadoop_bin not in os.environ['PATH']:
        os.environ['PATH'] += os.pathsep + hadoop_bin
    # print(f"✅ HADOOP_HOME configurado: {hadoop_home}")

import utils.config as config
from utils.transformations import BaseTransform, normalize_column_name
from utils.logging_utils import TqdmLoggingHandler

# Configura Logger Global
logger = logging.getLogger("BronzeToSilver_CNPJ")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TqdmLoggingHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(handler)

def get_spark_session():
    """Cria e configura a sessão Spark com suporte a Delta e Azure."""
    builder = SparkSession.builder \
        .appName("BronzeToSilver_CNPJ") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6,io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.driver.extraJavaOptions", "-Divy.message.logger.level=4 -Dlog4j.rootCategory=ERROR") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.speculation", "false") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
        .config("spark.hadoop.fs.azure.enable.check.access", "false") \
        .master("local[*]")

    spark = builder.getOrCreate()
    
    # Configuração de Logs para reduzir verbosidade
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    return spark

def configure_azure_access(spark):
    """Configura o acesso ao Azure Blob Storage usando SAS Tokens do config.py."""
    
    # Lista de layers para configurar
    layers = ["raw", "trusted"]
    
    for layer in layers:
        url = config.get_target_url(layer)
        if not url:
            logger.warning(f"⚠️ Aviso: URL para camada {layer} não encontrada no config.")
            continue
            
        # Extrai o SAS Token da URL (tudo depois do ?)
        sas_token = ""
        account = "grupo4storage" # Default
        
        if "?" in url:
            sas_token = url.split("?")[1]
            
        # Tenta extrair account da URL
        try:
            account = url.split("https://")[1].split(".")[0]
        except:
            pass
            
        if sas_token:
            spark.conf.set(f"fs.azure.sas.{layer}.{account}.blob.core.windows.net", sas_token)
            # ABFSS
            spark.conf.set(f"fs.azure.account.auth.type.{account}.dfs.core.windows.net", "SAS")
            spark.conf.set(f"fs.azure.sas.token.provider.type.{account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
            spark.conf.set(f"fs.azure.sas.fixed.token.{account}.dfs.core.windows.net", sas_token)
            
            logger.info(f"✅ Configurado acesso SAS para {account}/{layer}")

def list_raw_folders_cnpj():
    """Lista as pastas CNPJ dentro da pasta 'cnpj' no container RAW."""
    raw_container_url = config.get_target_url("raw")
    if not raw_container_url:
        raise ValueError("URL do container RAW não encontrada.")
    
    container_client = ContainerClient.from_container_url(raw_container_url)
    # Lista blobs dentro da pasta 'cnpj/'
    blobs = container_client.list_blobs(name_starts_with="cnpj/") 
    
    # Pastas esperadas (whitelist)
    known_cnpj_entities = [
        "empresas", "estabelecimentos", "socios", "cnaes", 
        "paises", "naturezas", "municipios", "simples", 
        "motivos", "qualificacoes"
    ]
    
    folders = set()
    for blob in blobs:
        name = blob.name # Ex: cnpj/empresas/part-0000.parquet
        parts = name.split('/')
        
        # Precisa ter pelo menos 2 partes: cnpj/pasta/...
        if len(parts) > 1:
            subfolder = parts[1]
            if subfolder in known_cnpj_entities:
                folders.add(subfolder)
    
    return sorted(list(folders))

def delete_virtual_directory(container_url, folder_name):
    """
    Remove todos os blobs que começam com o folder_name para simular overwrite de diretório.
    """
    try:
        container_client = ContainerClient.from_container_url(container_url)
        # Para CNPJ, as pastas ficam na raiz do container trusted
        prefix = f"{folder_name}/"
        
        logger.info(f"  🔍 Buscando arquivos para deletar com prefixo: {prefix} no container {container_client.container_name}")
        blobs = container_client.list_blobs(name_starts_with=prefix)
        batch = []
        count = 0
        for blob in blobs:
            batch.append(blob.name)
            count += 1
            container_client.delete_blob(blob.name)
            
        if count > 0:
            logger.info(f"  🗑️ Limpeza prévia: {count} arquivos removidos de {prefix}")
            
    except Exception as e:
        logger.warning(f"  ⚠️ Erro ao tentar limpar diretório {folder_name}: {e}")

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
    
    # Carregar Schema
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    schema_path = os.path.join(project_root, 'docs', 'cnpj_schema.json')
    
    logger.info(f"📄 Carregando schema de: {schema_path}")
    full_schema = load_schema(schema_path)

    # 1. Identificar pastas para processar
    try:
        folders = list_raw_folders_cnpj()
        logger.info(f"📂 Pastas CNPJ encontradas em 'raw': {folders}")
    except Exception as e:
        logger.error(f"❌ Erro ao listar pastas: {e}")
        return

    if not folders:
        logger.warning("⚠️ Nenhuma pasta CNPJ encontrada para processar.")
        return

    # 2. Inicializar Spark
    spark = get_spark_session()
    configure_azure_access(spark)
    
    # Extrai account para montar URL WASBS
    raw_url = config.get_target_url("raw")
    trusted_url = config.get_target_url("trusted")
    
    # Default fallback
    account = "grupo4storage"
    try:
        if raw_url: account = raw_url.split("https://")[1].split(".")[0]
    except:
        pass
    
    base_url_raw = f"wasbs://raw@{account}.blob.core.windows.net"
    base_url_trusted = f"wasbs://trusted@{account}.blob.core.windows.net"

    # 3. Processar cada pasta individualmente
    pbar = tqdm.tqdm(folders, desc="Processando CNPJ")
    for folder_name in pbar:
        pbar.set_description(f"Processando: {folder_name}")
        
        source_path = f"{base_url_raw}/cnpj/{folder_name}"
        target_path = f"{base_url_trusted}/{folder_name}"
        
        try:
            logger.info(f"  📂 Lendo dados de: {source_path}")
            
            # Tenta ler como Delta/Parquet
            try:
                df = spark.read.format("delta").load(source_path)
            except:
                logger.warning("  ⚠️ Falha ao ler como Delta, tentando Parquet...")
                df = spark.read.option("recursiveFileLookup", "true").parquet(source_path)
            
            count_records = df.count()
            logger.info(f"  📊 Registros encontrados: {count_records}")
            
            if count_records == 0:
                logger.warning("  ⚠️ Pasta vazia ou sem dados válidos. Pulando.")
                continue

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
            
            if trusted_url:
                delete_virtual_directory(trusted_url, folder_name)

            logger.info(f"  💾 Salvando em: {target_path} (CSV Latin1)")
            
            writer = df_clean.write.format("csv") \
                .option("header", "true") \
                .option("sep", ";") \
                .option("encoding", "ISO-8859-1") \
                .mode("append")
                
            writer.save(target_path)
            logger.info(f"  ✅ Pasta {folder_name} concluída!")
            
        except Exception as e:
            logger.error(f"  ❌ Erro ao processar pasta {folder_name}: {str(e)}")

    logger.info("\n🏁 Processamento CNPJ finalizado.")
    spark.stop()

if __name__ == "__main__":
    process_cnpj()
