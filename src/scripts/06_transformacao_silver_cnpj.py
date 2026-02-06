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
        .appName("BronzeToSilver_CNPJ") \
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
            spark.conf.set(f"fs.azure.sas.{container}.{account}.blob.core.windows.net", sas_token)
            # ABFSS
            spark.conf.set(f"fs.azure.account.auth.type.{account}.dfs.core.windows.net", "SAS")
            spark.conf.set(f"fs.azure.sas.token.provider.type.{account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
            spark.conf.set(f"fs.azure.sas.fixed.token.{account}.dfs.core.windows.net", sas_token)
            
            print(f"✅ Configurado acesso SAS para {account}/{container}")

def list_raw_folders_cnpj():
    """Lista as pastas CNPJ na raiz do container RAW."""
    raw_container_url = os.getenv("AZURE_TARGET_STORAGE_RAW_URL")
    if not raw_container_url:
        raise ValueError("URL do container RAW não encontrada.")
    
    container_client = ContainerClient.from_container_url(raw_container_url)
    blobs = container_client.list_blobs() # Lista tudo na raiz
    
    # Pastas esperadas (whitelist) para evitar processar balanco_comercial
    known_cnpj_entities = [
        "empresas", "estabelecimentos", "socios", "cnaes", 
        "paises", "naturezas", "municipios", "simples", 
        "motivos", "qualificacoes"
    ]
    
    folders = set()
    for blob in blobs:
        name = blob.name
        # Se for pasta (tem /) ou arquivo direto (parquet), pega a primeira parte
        if '/' in name:
            top_folder = name.split('/')[0]
            if top_folder in known_cnpj_entities:
                folders.add(top_folder)
        else:
            # Caso seja um arquivo na raiz (pouco provavel para delta/parquet, mas possivel)
            pass
    
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
    Retorna a lista de colunas para a pasta.
    Mapeia nomes de pasta (empresas) para chaves do JSON (EMPRECSV).
    """
    folder_lower = folder_name.lower()
    
    # Mapa de Folder -> JSON Key
    # Ajuste conforme ingestion_cnpj.ipynb e docs/cnpj_schema.json
    mapping_keys = {
        "empresas": "EMPRECSV",
        "estabelecimentos": "ESTABELE",
        "socios": "SOCIOCSV",
        "cnaes": "CNAECSV",
        "municipios": "MUNICIPIOS", # No JSON está MUNICIPIOS
        "naturezas": "NATUREZAS",
        "paises": "PAISES",
        "simples": "SIMPLES",
        "motivos": "MOTIVOS",
        "qualificacoes": "QUALIFICACOES"
    }
    
    key = mapping_keys.get(folder_lower)
    
    if key and key in schema:
        print(f"  🔍 Schema selecionado: {key}")
        return schema[key]
    
    print(f"  ⚠️ Nenhum schema específico encontrado para '{folder_name}'. Usando normalização padrão.")
    return []

def process_cnpj():
    print(f"\n🚀 Iniciando processamento Bronze -> Silver (CNPJ)")
    
    # Carregar Schema
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # scripts -> src -> Projeto Integrado
    project_root = os.path.dirname(os.path.dirname(current_dir))
    schema_path = os.path.join(project_root, 'docs', 'cnpj_schema.json')
    
    print(f"📄 Carregando schema de: {schema_path}")
    full_schema = load_schema(schema_path)

    # 1. Identificar pastas para processar
    try:
        folders = list_raw_folders_cnpj()
        print(f"📂 Pastas CNPJ encontradas em 'raw': {folders}")
    except Exception as e:
        print(f"❌ Erro ao listar pastas: {e}")
        return

    if not folders:
        print("⚠️ Nenhuma pasta CNPJ encontrada para processar.")
        return

    # 2. Inicializar Spark
    spark = get_spark_session()
    configure_azure_access(spark)
    
    base_url_raw = "wasbs://raw@grupo4storage.blob.core.windows.net"
    base_url_trusted = "wasbs://trusted@grupo4storage.blob.core.windows.net/cnpj" # Agrupa em subpasta cnpj na trusted? Ou raiz?
    # O user pediu "subir tanto balança quanto cnpj na trusted".
    # Balança vai para trusted/balanco_comercial/...
    # CNPJ pode ir para trusted/cnpj/... ou trusted/empresas...
    # Para organizar melhor, vou colocar em trusted/cnpj/{entidade}
    # Mas vou seguir o padrão do raw se possível. No raw parece ser raw/empresas.
    # Vou salvar em trusted/empresas para manter consistência com raw, ou trusted/cnpj/empresas?
    # Balança: raw/balanco_comercial/{folder} -> trusted/balanco_comercial/{folder}
    # CNPJ: raw/{folder} -> trusted/{folder} (se raw for raiz)
    # Vou assumir trusted/{folder} para ficar igual ao raw.
    base_url_trusted = "wasbs://trusted@grupo4storage.blob.core.windows.net"

    # 3. Processar cada pasta individualmente
    for folder_name in folders:
        print(f"\n🔄 Processando pasta: {folder_name} ...")
        
        source_path = f"{base_url_raw}/{folder_name}"
        target_path = f"{base_url_trusted}/{folder_name}"
        
        try:
            print(f"  📂 Lendo dados de: {source_path}")
            
            # Tenta ler como Delta/Parquet
            # A ingestão salva como Delta.
            try:
                df = spark.read.format("delta").load(source_path)
            except:
                print("  ⚠️ Falha ao ler como Delta, tentando Parquet...")
                df = spark.read.option("recursiveFileLookup", "true").parquet(source_path)
            
            count_records = df.count()
            print(f"  📊 Registros encontrados: {count_records}")
            
            if count_records == 0:
                print("  ⚠️ Pasta vazia ou sem dados válidos. Pulando.")
                continue

            # --- Transformações ---
            
            # Selecionar mapping do schema
            # O schema do CNPJ é uma LISTA de nomes de colunas na ordem.
            # O DF do Delta já deve ter nomes de colunas, possivelmente genéricos (_c0, _c1) ou já nomeados se a ingestão bronze fez schema evolution.
            # A ingestão diz "Schema evolution enabled". "Uses expected schema as reference but allows additional columns."
            # Se a ingestão já nomeou as colunas, ótimo.
            # Se a ingestão usou "header=false" no CSV e salvou, as colunas podem ser _c0...
            # Mas `load_csv_with_schema_evolution` usa `expected_schema` (StructType).
            # Então as colunas no RAW JÁ DEVEM ESTAR NOMEADAS corretamente.
            # O `get_schema_mapping` aqui retorna uma LISTA de strings do JSON.
            # Se as colunas já estiverem certas, o rename não fará nada ou confirmará.
            
            mapping_list = get_schema_mapping(folder_name, full_schema)

            print("  � Aplicando pipeline de transformações (BaseTransform)...")
            
            transformer = BaseTransform(df)
            
            # Se tiver schema mapping (lista de nomes), garantimos que as colunas tem esses nomes?
            # Se o DF já tem nomes corretos, ok. Se tiver _c0, _c1, renomeamos.
            # Vamos assumir que se o DF tem colunas que não batem com o schema, tentamos renomear posicionalmente APENAS SE parecerem genéricas ou se for a estratégia.
            # Mas como é Bronze->Silver, e Bronze já aplicou schema, talvez seja redundante.
            # Mas para garantir, vamos aplicar normalização.
            
            transformer.normalize_headers()
            
            df_clean = (transformer
                        .drop_foreign_columns()
                        .trim_strings()
                        .upper_strings()
                        .drop_duplicates()
                        .treat_nulls()
                        .get_dataframe())
            
            # --- Escrita ---
            
            # Configuração para escrita em Latin1 (CSV) conforme solicitado
            print(f"  💾 Salvando em: {target_path} (CSV Latin1)")
            
            writer = df_clean.write.format("csv") \
                .option("header", "true") \
                .option("sep", ";") \
                .option("encoding", "ISO-8859-1") \
                .mode("overwrite")
                
            writer.save(target_path)
            print(f"  ✅ Pasta {folder_name} concluída!")
            
        except Exception as e:
            print(f"  ❌ Erro ao processar pasta {folder_name}: {str(e)}")

    print("\n🏁 Processamento CNPJ finalizado.")
    spark.stop()

if __name__ == "__main__":
    process_cnpj()
