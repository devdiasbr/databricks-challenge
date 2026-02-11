# Databricks notebook source
# MAGIC %md
# MAGIC # Transformação Silver: Balança Comercial
# MAGIC 
# MAGIC Limpeza, padronização e tipagem dos dados da Balança Comercial (Bronze -> Silver).

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

# COMMAND ----------

def get_spark_session():
    """Obtém a sessão Spark ativa (Databricks)."""
    return SparkSession.builder.getOrCreate()

# COMMAND ----------

def list_raw_folders(spark, protocol):
    """
    Lista as pastas dentro de 'balancacomercial/' no container RAW.
    Usa dbutils (Databricks).
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        
        base_path = config.get_base_path("raw", "target", protocol)
        target_path = f"{base_path}/balancacomercial/"
        
        logger.info(f"Listando pastas via dbutils em: {target_path}")
        paths = dbutils.fs.ls(target_path)
        
        folders = []
        for p in paths:
            # p.name retorna ex: 'balancacomercial/EXP_2021/' ou apenas 'EXP_2021/' dependendo da versão
            # O importante é pegar o último componente que é o diretório
            name = p.name.strip('/') # remove barra final
            if '/' in name:
                name = name.split('/')[-1]
            folders.append(name)
            
        return sorted(folders)
        
    except Exception as e:
        logger.error(f"Erro ao listar pastas via dbutils: {e}")
        # Fallback para Azure SDK apenas se falhar muito feio e tivermos chaves
        # Mas preferimos falhar aqui para garantir ambiente correto
        raise e


# COMMAND ----------

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
    Retorna o mapeamento de colunas adequado para a pasta com base no nome.
    Lógica:
    - Se tem '_mun', usa mapping EXP_2021_MUN ou IMP_2021_MUN
    - Senão, usa EXPO_2021 ou IMP_2021
    """
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

# COMMAND ----------

def process_balanca_comercial():
    logger.info(f"\n🚀 Iniciando processamento Balança Comercial: Bronze -> Silver")
    
    # 1. Inicializar Spark (Prioritário para configurar acesso)
    spark = get_spark_session()
    protocol = config.configure_spark_access(spark)

    # Carregar Schema
    schema_path = os.path.join(project_root, 'docs', 'schemas', 'balanca_schema.json')
    logger.info(f"📄 Carregando schema de: {schema_path}")
    full_schema = load_schema(schema_path)

    # 2. Identificar pastas para processar
    try:
        folders = list_raw_folders(spark, protocol)
        logger.info(f"📂 Pastas encontradas em 'balancacomercial/': {folders}")
    except Exception as e:
        logger.error(f"❌ Erro ao listar pastas: {e}")
        return

    if not folders:
        logger.warning("⚠️ Nenhuma pasta encontrada para processar.")
        return
    
    # URLs base
    base_url_raw = f"{config.get_base_path('raw', 'target', protocol)}/balancacomercial"
    base_url_trusted = f"{config.get_base_path('trusted', 'target', protocol)}/balancacomercial"

    # 3. Processar cada pasta individualmente
    pbar = tqdm.tqdm(folders, desc="Processando Pastas")
    for folder_name in pbar:
        pbar.set_description(f"Processando: {folder_name}")
        
        source_path = f"{base_url_raw}/{folder_name}"
        target_path = f"{base_url_trusted}/{folder_name}"
        
        try:
            logger.info(f"  📂 Lendo dados de: {source_path}")
            
            # Tenta ler como Delta/Parquet (Balança Comercial pode ter sido ingerida como Delta)
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

            # --- Transformações ---
            
            # Selecionar mapping do schema
            mapping = get_schema_mapping(folder_name, full_schema)

            # Utilizando a classe BaseTransform para orquestrar as limpezas
            logger.info("  🔧 Aplicando pipeline de transformações (BaseTransform)...")
            
            transformer = BaseTransform(df)
            
            # Aplica renomeação se houver mapping, senão normaliza
            if mapping:
                transformer.rename_columns(mapping)
            else:
                transformer.normalize_headers()
            
            df_clean = (transformer
                        .trim_strings()     # Remove espaços em branco
                        .upper_strings()    # Padroniza para caixa alta (ajuda em joins futuros)
                        .drop_duplicates()  # Remove linhas idênticas
                        .treat_nulls()      # Trata nulos básicos
                        .get_dataframe())
            
            # Exibe amostra
            # logger.info("  👀 Amostra dos dados tratados:")
            # df_clean.show(3)
            
            # --- Escrita ---
            
            logger.info(f"  💾 Salvando em: {target_path}")
            
            # Verifica colunas para particionamento (usando nomes novos do schema se aplicável)
            cols = df_clean.columns
            partition_cols = []
            
            # Tenta identificar colunas de ano/mes (pode ser CO_ANO ou ano, dependendo do mapping)
            potential_year_cols = ['ano', 'co_ano', 'CO_ANO']
            potential_month_cols = ['mes', 'co_mes', 'CO_MES']
            
            for c in potential_year_cols:
                if c in cols: 
                    partition_cols.append(c)
                    break # Pega só um
            
            for c in potential_month_cols:
                if c in cols: 
                    partition_cols.append(c)
                    break
            
            # Configuração para escrita em Delta
            logger.info("  💾 Salvando em formato Delta...")
            writer = df_clean.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true")
            
            if partition_cols:
                logger.info(f"    Particionando por: {partition_cols}")
                writer = writer.partitionBy(*partition_cols)
                
            writer.save(target_path)
            
            # Otimização Delta (Z-ORDER e Compactação)
            logger.info("  ⚡ Executando OPTIMIZE e VACUUM...")
            try:
                # Configura tamanho alvo do arquivo para OPTIMIZE (10MB)
                spark.conf.set("spark.databricks.delta.optimize.maxFileSize", config.DELTA_OPTIMIZE_FILE_SIZE)
                
                # Otimização
                spark.sql(f"OPTIMIZE delta.`{target_path}`")
                
                # Limpeza (Mantém 60 dias de histórico conforme config)
                spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                spark.sql(f"VACUUM delta.`{target_path}` RETAIN {config.DELTA_VACUUM_RETENTION_DAYS * 24} HOURS")
            except Exception as e:
                logger.warning(f"  ⚠️ Otimização não executada (pode exigir Databricks Runtime ou Spark configurado): {e}")

            logger.info(f"  ✅ Pasta {folder_name} concluída!")
            
        except Exception as e:
            logger.error(f"  ❌ Erro ao processar pasta {folder_name}: {str(e)}")

    logger.info("\n🏁 Processamento global finalizado.")
    try:
        spark.stop()
    except Exception:
        pass

# COMMAND ----------

if __name__ == "__main__":
    process_balanca_comercial()
