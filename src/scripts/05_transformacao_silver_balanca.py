import os
import sys
import json
import logging
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
    print(f"✅ HADOOP_HOME configurado: {hadoop_home}")

from utils.transformations import BaseTransform, normalize_column_name

def get_spark_session():
    """Cria e configura a sessão Spark com suporte a Delta e Azure."""
    builder = SparkSession.builder \
        .appName("BronzeToSilver_BalancaComercial") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6,io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.driver.extraJavaOptions", "-Divy.message.logger.level=4") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .master("local[*]")

    spark = builder.getOrCreate()
    
    # Configuração de Logs para reduzir verbosidade
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    return spark

def configure_azure_access(spark):
    """Configura o acesso ao Azure Blob Storage usando SAS Tokens do .env."""
    
    # Mapeamento de containers para chaves no .env
    configs = [
        ("grupo4storage", "raw", "AZURE_TARGET_STORAGE_RAW_URL"),
        ("grupo4storage", "trusted", "AZURE_TARGET_STORAGE_TRUSTED_URL")
    ]
    
    for account, container, env_var in configs:
        url = os.getenv(env_var)
        if not url:
            print(f"⚠️ Aviso: Variável {env_var} não encontrada.")
            continue
            
        # Extrai o SAS Token da URL (tudo depois do ?)
        if "?" in url:
            sas_token = url.split("?")[1]
        else:
            sas_token = "" 
            
        if sas_token:
            # Configuração para WASBS (Blob Storage) - Mais compatível localmente
            # Formato: fs.azure.sas.<container>.<account>.blob.core.windows.net
            spark.conf.set(f"fs.azure.sas.{container}.{account}.blob.core.windows.net", sas_token)
            
            # Configuração ABFSS (Opcional, mantida caso rode em ambiente que suporte)
            spark.conf.set(f"fs.azure.account.auth.type.{account}.dfs.core.windows.net", "SAS")
            spark.conf.set(f"fs.azure.sas.token.provider.type.{account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
            spark.conf.set(f"fs.azure.sas.fixed.token.{account}.dfs.core.windows.net", sas_token)
            
            print(f"✅ Configurado acesso SAS para {account}/{container}")

def list_raw_folders(prefix="balanco_comercial/"):
    """Lista as pastas dentro do prefixo especificado no container RAW."""
    raw_container_url = os.getenv("AZURE_TARGET_STORAGE_RAW_URL")
    if not raw_container_url:
        raise ValueError("URL do container RAW não encontrada.")
    
    container_client = ContainerClient.from_container_url(raw_container_url)
    blobs = container_client.list_blobs(name_starts_with=prefix)
    
    folders = set()
    for blob in blobs:
        name = blob.name
        # Remove o prefixo base para pegar as subpastas
        relative_path = name[len(prefix):]
        if '/' in relative_path:
            # Pega a primeira parte do caminho relativo (nome da subpasta)
            subfolder = relative_path.split('/')[0]
            if subfolder:
                folders.add(subfolder)
    
    return sorted(list(folders))

def load_schema(schema_path):
    """Carrega o arquivo JSON de schema (tenta UTF-8, fallback para Latin1)."""
    try:
        # Tenta primeiro UTF-8 (padrão JSON)
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except UnicodeDecodeError:
        print(f"⚠️ Aviso: Falha com UTF-8, tentando Latin1 para {schema_path}")
        # Fallback para Latin1 (ISO-8859-1)
        with open(schema_path, 'r', encoding='latin1') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Erro ao carregar schema JSON de {schema_path}: {e}")
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
        print(f"  🔍 Schema selecionado: {key}")
        return schema[key]
    
    print(f"  ⚠️ Nenhum schema específico encontrado para '{folder_name}'. Usando normalização padrão.")
    return {}

def process_balanca_comercial():
    print(f"\n🚀 Iniciando processamento Bronze -> Silver")
    
    # Carregar Schema
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # scripts -> src -> Projeto Integrado
    project_root = os.path.dirname(os.path.dirname(current_dir))
    schema_path = os.path.join(project_root, 'docs', 'balanca_schema.json')
    
    print(f"📄 Carregando schema de: {schema_path}")
    full_schema = load_schema(schema_path)

    # 1. Identificar pastas para processar
    try:
        folders = list_raw_folders()
        print(f"📂 Pastas encontradas em 'balanco_comercial/': {folders}")
    except Exception as e:
        print(f"❌ Erro ao listar pastas: {e}")
        return

    if not folders:
        print("⚠️ Nenhuma pasta encontrada para processar.")
        return

    # 2. Inicializar Spark
    spark = get_spark_session()
    configure_azure_access(spark)
    
    base_url_raw = "wasbs://raw@grupo4storage.blob.core.windows.net/balanco_comercial"
    base_url_trusted = "wasbs://trusted@grupo4storage.blob.core.windows.net/balanco_comercial"

    # 3. Processar cada pasta individualmente
    for folder_name in folders:
        print(f"\n🔄 Processando pasta: {folder_name} ...")
        
        source_path = f"{base_url_raw}/{folder_name}"
        target_path = f"{base_url_trusted}/{folder_name}"
        
        try:
            print(f"  📂 Lendo dados de: {source_path}")
            
            # Tenta ler como Parquet (recursivo para pegar partitions se houver)
            df = spark.read.option("recursiveFileLookup", "true").parquet(source_path)
            
            count_records = df.count()
            print(f"  📊 Registros encontrados: {count_records}")
            
            if count_records == 0:
                print("  ⚠️ Pasta vazia ou sem dados válidos. Pulando.")
                continue

            # --- Transformações ---
            
            # Selecionar mapping do schema
            mapping = get_schema_mapping(folder_name, full_schema)

            # Utilizando a classe BaseTransform para orquestrar as limpezas
            print("  � Aplicando pipeline de transformações (BaseTransform)...")
            
            transformer = BaseTransform(df)
            
            # Aplica renomeação se houver mapping, senão normaliza
            if mapping:
                transformer.rename_columns(mapping)
                # Opcional: normalizar o resto que não foi mapeado? 
                # O usuário prefere o JSON, então confiamos nele.
                # Mas para garantir consistência (lowercase), podemos normalizar depois?
                # Se o JSON tem "ano", "mes", já está lowercase.
                # Se sobrar coluna sem mapping, ela fica original.
                # Vamos forçar lowercase em tudo por segurança após mapping?
                # Melhor não alterar o que o JSON definiu.
            else:
                transformer.normalize_headers()
            
            df_clean = (transformer
                        .trim_strings()     # Remove espaços em branco
                        .upper_strings()    # Padroniza para caixa alta (ajuda em joins futuros)
                        .drop_duplicates()  # Remove linhas idênticas
                        .treat_nulls()      # Trata nulos básicos
                        .get_dataframe())
            
            # Exibe amostra
            print("  👀 Amostra dos dados tratados:")
            df_clean.show(3)
            
            # --- Escrita ---
            
            print(f"  💾 Salvando em: {target_path}")
            
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
            
            # Configuração para escrita em Latin1 (CSV) conforme solicitado
            # Delta/Parquet são nativamente UTF-8, então usamos CSV para garantir Latin1 (ISO-8859-1)
            print("  💾 Salvando em formato CSV (Latin1/ISO-8859-1)...")
            writer = df_clean.write.format("csv") \
                .option("header", "true") \
                .option("sep", ";") \
                .option("encoding", "ISO-8859-1") \
                .mode("overwrite")
            
            if partition_cols:
                print(f"    Particionando por: {partition_cols}")
                writer = writer.partitionBy(*partition_cols)
                
            writer.save(target_path)
            print(f"  ✅ Pasta {folder_name} concluída!")
            
        except Exception as e:
            print(f"  ❌ Erro ao processar pasta {folder_name}: {str(e)}")
            # import traceback
            # traceback.print_exc() 

    print("\n🏁 Processamento global finalizado.")
    spark.stop()

if __name__ == "__main__":
    process_balanca_comercial()
