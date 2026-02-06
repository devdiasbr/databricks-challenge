import os
import sys
import json
import logging
import re
import tqdm
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# =============================================================================
# LOGGING SETUP
# =============================================================================
# Tenta importar do utils, assumindo estrutura do projeto
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(os.path.join(project_root, "src"))

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
        logger.warning(f"⚠️ Aviso: Pasta HADOOP_HOME não encontrada em: {hadoop_home}")
        import tempfile
        hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop_workaround")
    
    os.environ['HADOOP_HOME'] = hadoop_home
    hadoop_bin = os.path.join(hadoop_home, "bin")
    
    # Garante que a pasta bin exista (se for fallback)
    os.makedirs(hadoop_bin, exist_ok=True)
    
    # Verifica winutils.exe
    winutils_path = os.path.join(hadoop_bin, "winutils.exe")
    if not os.path.exists(winutils_path):
        logger.warning(f"⚠️ winutils.exe não encontrado em {winutils_path}. Tentando baixar...")
        import urllib.request
        try:
            url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/winutils.exe"
            urllib.request.urlretrieve(url, winutils_path)
            logger.info("✅ winutils.exe baixado com sucesso.")
        except Exception as e:
            logger.error(f"❌ Falha ao baixar winutils: {e}")
            with open(winutils_path, "w") as f:
                f.write("Dummy winutils")

    # Adiciona ao PATH
    if hadoop_bin not in os.environ['PATH']:
        os.environ['PATH'] += os.pathsep + hadoop_bin
        
    # logger.info(f"🔧 Configurado HADOOP_HOME: {hadoop_home}")

import utils.config as config

# =============================================================================
# INITIALIZE SPARK SESSION
# =============================================================================
def get_spark_session():
    # logger.info("Initializing Spark Session with Delta support...")
    builder = SparkSession.builder \
        .appName("IngestaoBronzeBalanca") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
TARGET_ACCOUNT = "grupo4storage" # Default

if TARGET_RAW_URL:
    if "?" in TARGET_RAW_URL:
        TARGET_SAS = TARGET_RAW_URL.split("?")[1]
    
    # Tenta extrair account da URL
    try:
        TARGET_ACCOUNT = TARGET_RAW_URL.split("https://")[1].split(".")[0]
    except:
        pass

# Configura credenciais
if SOURCE_SAS:
    spark.conf.set(f"fs.azure.sas.{SOURCE_CONTAINER}.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)
    spark.conf.set(f"fs.azure.sas.{SOURCE_CONTAINER}.{SOURCE_ACCOUNT}.blob.core.windows.net", SOURCE_SAS)
    
    # Configuração explícita para ABFSS SAS Provider
    spark.conf.set(f"fs.azure.account.auth.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)

if TARGET_SAS:
    spark.conf.set(f"fs.azure.sas.{TARGET_CONTAINER}.{TARGET_ACCOUNT}.dfs.core.windows.net", TARGET_SAS)
    spark.conf.set(f"fs.azure.sas.{TARGET_CONTAINER}.{TARGET_ACCOUNT}.blob.core.windows.net", TARGET_SAS)
    
    spark.conf.set(f"fs.azure.account.auth.type.{TARGET_ACCOUNT}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{TARGET_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{TARGET_ACCOUNT}.dfs.core.windows.net", TARGET_SAS)

SOURCE_ABFSS_PATH = f"abfss://{SOURCE_CONTAINER}@{SOURCE_ACCOUNT}.dfs.core.windows.net"
TARGET_ABFSS_PATH = f"abfss://{TARGET_CONTAINER}@{TARGET_ACCOUNT}.dfs.core.windows.net"

# =============================================================================
# CARREGAMENTO DE SCHEMA
# =============================================================================
schema_path = os.path.join(project_root, 'docs', 'balanca_schema.json')
try:
    with open(schema_path, 'r', encoding='utf-8') as f:
        full_schema = json.load(f)
    logger.info(f"✅ Schema carregado.")
except Exception as e:
    logger.error(f"❌ Erro ao carregar schema: {e}")
    full_schema = {}

def get_mapping_for_file(filename):
    """
    Retorna o dicionário de mapeamento (col_origem -> col_destino) para o arquivo.
    Suporta correspondência exata, por prefixo e insensível ao ano (ex: IMP_2022_MUN -> IMP_2021_MUN).
    """
    name_no_ext = filename.replace(".csv", "")
    
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
arquivos_para_ignorar = ["NBM.csv", "NBM_NCM.csv"]

logger.info(f"Listando arquivos em: {SOURCE_ABFSS_PATH}")

# Listagem de arquivos via Hadoop API (funciona com SAS configurado acima)
Path = spark._jvm.org.apache.hadoop.fs.Path
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.azure.enable.check.access", "false")

# Propaga configurações de SAS para o Hadoop Conf do contexto (importante para o FileSystem)
if SOURCE_SAS:
    # Configuração ABFSS
    hadoop_conf.set(f"fs.azure.account.auth.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "SAS")
    hadoop_conf.set(f"fs.azure.sas.token.provider.type.{SOURCE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    hadoop_conf.set(f"fs.azure.sas.fixed.token.{SOURCE_ACCOUNT}.dfs.core.windows.net", SOURCE_SAS)
    
    # Configuração WASBS (Fallback)
    hadoop_conf.set(f"fs.azure.sas.{SOURCE_CONTAINER}.{SOURCE_ACCOUNT}.blob.core.windows.net", SOURCE_SAS)

try:
    fs = Path(SOURCE_ABFSS_PATH).getFileSystem(hadoop_conf)
    status_list = fs.listStatus(Path(SOURCE_ABFSS_PATH))
except Exception as e:
    logger.warning(f"⚠️ Erro ao listar com ABFSS ({e}). Tentando fallback para WASBS...")
    SOURCE_ABFSS_PATH = f"wasbs://{SOURCE_CONTAINER}@{SOURCE_ACCOUNT}.blob.core.windows.net"
    # Se falhou no Source com ABFSS, provavelmente falhará no Target também. Muda Target para WASBS.
    TARGET_ABFSS_PATH = f"wasbs://{TARGET_CONTAINER}@{TARGET_ACCOUNT}.blob.core.windows.net"
    
    fs = Path(SOURCE_ABFSS_PATH).getFileSystem(hadoop_conf)
    status_list = fs.listStatus(Path(SOURCE_ABFSS_PATH))
    logger.info(f"✅ Fallback para WASBS bem sucedido. \nOrigem: {SOURCE_ABFSS_PATH}\nDestino: {TARGET_ABFSS_PATH}")

arquivos = []
for status in status_list:
    path = status.getPath().toString()
    name = status.getPath().getName()
    if status.isFile():
        arquivos.append((path, name))

# Filtra arquivos relevantes
arquivos_filtrados = [
    (path, name) for path, name in arquivos 
    if name.endswith(".csv") and name not in arquivos_para_ignorar
]

# Loop com TQDM
pbar = tqdm.tqdm(arquivos_filtrados, desc="Ingestão Bronze")

for full_path, name in pbar:
    pbar.set_description(f"Ingerindo: {name}")
    
    # Determina nome da pasta destino
    nome_base = name.replace(".csv", "")
    # Remove ano para agrupar (ex: EXP_2021 -> exp)
    nome_limpo = re.sub(r'_\d{4}', '', nome_base) 
    nome_pasta_assunto = nome_limpo.replace("__", "_").strip("_").lower()
    nome_pasta_raw = f"balancacomercial/{nome_pasta_assunto}"
    
    # logger.info(f"Processing: {name} -> {nome_pasta_raw}")
    
    # Leitura
    # Usando latin1 pois dados de governo costumam usar esse encoding
    try:
        df_temp = (spark.read 
            .format("csv") 
            .option("header", True) 
            .option("delimiter", ";") 
            .option("encoding", "ISO-8859-1") 
            .option("inferSchema", "true")
            .load(full_path) 
        )
        
        # Aplicar mapeamento de schema se existir
        mapping = get_mapping_for_file(name)
        
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
        # logger.info(f"   ✅ Salvo com sucesso em: {path_destino}")
        
    except Exception as e:
        logger.error(f"   ❌ Erro ao processar {name}: {e}")

logger.info("--- Processo de Ingestão Finalizado ---")
