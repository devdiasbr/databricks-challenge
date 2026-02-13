# Databricks notebook source
import os
from dotenv import load_dotenv

# --- Carregamento de Variáveis de Ambiente ---
# Configuração robusta de caminhos (Híbrido Local/Databricks)
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

# Navega para cima até encontrar a pasta 'src' para definir o project_root
project_root = base_dir
while not os.path.exists(os.path.join(project_root, 'src')) and project_root != os.path.dirname(project_root):
    project_root = os.path.dirname(project_root)

# Fallback: se não achou src, usa o base_dir
if not os.path.exists(os.path.join(project_root, 'src')):
    project_root = base_dir

env_path = os.path.join(project_root, '.env')

print(f"[Config] Tentando carregar .env de: {env_path}")
loaded = load_dotenv(dotenv_path=env_path)

if not loaded:
    print("[Config] .env não encontrado no caminho explícito. Tentando busca padrão...")
    load_dotenv()


# --- Key Vault & Protocols ---
USE_KEY_VAULT = False # Definido como False para usar apenas .env por enquanto

# Detecta se está rodando no Databricks
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

DATABRICKS_SCOPE = "databricks-scope-4"
KV_SECRET_TARGET = "secret-storage-4"
KV_SECRET_LANDING = "secret-landing"

# --- Helpers de Configuração ---
def get_config(key, default=None, secret_key=None):
    """
    Tenta recuperar configuração de múltiplas fontes:
    1. Variável de Ambiente (OS)
    2. Dbutils Secrets (se disponível no Databricks e USE_KEY_VAULT=True)
    
    :param key: Nome da variável de ambiente
    :param default: Valor padrão
    :param secret_key: Nome da chave no Key Vault (se diferente da env var)
    """
    # 1. Tenta Env Var
    val = os.getenv(key)
    if val: return val
    
    # 2. Tenta Dbutils (apenas se estiver no Databricks e Key Vault estiver habilitado)
    if USE_KEY_VAULT:
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            
            # Tenta buscar pelo secret_key específico ou pelo nome da env var
            key_to_search = secret_key if secret_key else key
            return dbutils.secrets.get(scope=DATABRICKS_SCOPE, key=key_to_search)
        except Exception:
            pass
        
    return default


# --- Configurações de Storage ---

# 1. ORIGEM (Landing)
BALANCA_URL = get_config("BALANCA_ACCOUNT_URL")
CNPJ_URL = get_config("CNPJ_ACCOUNT_URL")

# Extrai nome da conta (ex: https://landingbeca2026jan.blob...)
SOURCE_ACCOUNT = get_config("SOURCE_STORAGE_ACCOUNT") or "landingbeca2026jan"
if BALANCA_URL:
    try:
        SOURCE_ACCOUNT = BALANCA_URL.split("https://")[1].split(".")[0]
    except IndexError:
        pass
elif CNPJ_URL:
    try:
        SOURCE_ACCOUNT = CNPJ_URL.split("https://")[1].split(".")[0]
    except IndexError:
        pass

# Tokens específicos de Origem
SAS_TOKEN_BALANCA = get_config("AZURE_STORAGE_SAS_TOKEN_BALANCA")
SAS_TOKEN_CNPJ = get_config("AZURE_STORAGE_SAS_TOKEN_CNPJ")
LANDING_ACCOUNT_KEY = get_config("AZURE_STORAGE_ACCOUNT_KEY_LANDING", secret_key=KV_SECRET_LANDING)

# 2. DESTINO (Lakehouse: Raw, Trusted, Refined)
TARGET_RAW_URL = get_config("AZURE_TARGET_STORAGE_RAW_URL")
TARGET_TRUSTED_URL = get_config("AZURE_TARGET_STORAGE_TRUSTED_URL")
TARGET_REFINED_URL = get_config("AZURE_TARGET_STORAGE_REFINED_URL")

# --- Configurações Delta Lake ---
DELTA_VACUUM_RETENTION_DAYS = 60
DELTA_OPTIMIZE_FILE_SIZE = 10485760  # 10 MB em bytes

# Extrai nome da conta de destino (Prioridade: Env Var > URL > Default)
TARGET_ACCOUNT = get_config("TARGET_STORAGE_ACCOUNT") or get_config("TARGET_ACCOUNT") or get_config("AZURE_TARGET_ACCOUNT") or "grupo4storage"

if not TARGET_ACCOUNT and TARGET_RAW_URL:
    try:
        TARGET_ACCOUNT = TARGET_RAW_URL.split("https://")[1].split(".")[0]
    except Exception:
        pass


def configure_spark_access(spark):
    """
    Configura acesso ao Storage.
    Retorna o protocolo a ser usado ('abfss' ou 'wasbs').
    Prioriza Account Keys do .env se Key Vault estiver desabilitado.
    """
    # 1. Tenta Key Vault (se habilitado)
    if USE_KEY_VAULT:
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            try:
                target_key = dbutils.secrets.get(scope=DATABRICKS_SCOPE, key=KV_SECRET_TARGET)
                landing_key = dbutils.secrets.get(scope=DATABRICKS_SCOPE, key=KV_SECRET_LANDING)
                
                # Configura ABFSS (DFS) e WASBS (Blob) com Account Key
                spark.conf.set(f"fs.azure.account.key.{TARGET_ACCOUNT}.dfs.core.windows.net", target_key)
                spark.conf.set(f"fs.azure.account.key.{SOURCE_ACCOUNT}.dfs.core.windows.net", landing_key)
                spark.conf.set(f"fs.azure.account.key.{TARGET_ACCOUNT}.blob.core.windows.net", target_key)
                spark.conf.set(f"fs.azure.account.key.{SOURCE_ACCOUNT}.blob.core.windows.net", landing_key)
                
                print(f"[Config] ✅ Acesso configurado via Key Vault ({DATABRICKS_SCOPE}). Protocolo: ABFSS.")
                return "abfss"
            except Exception as e:
                print(f"[Config] ⚠️ Falha ao buscar secrets no Key Vault: {e}")
        except ImportError:
            pass
        
    # 2. Tenta usar Account Keys do .env (Ambiente Local ou Databricks com .env)
    target_key_env = os.getenv("AZURE_STORAGE_ACCOUNT_KEY_TARGET")
    landing_key_env = os.getenv("AZURE_STORAGE_ACCOUNT_KEY_LANDING")

    if target_key_env or landing_key_env:
        if target_key_env:
            spark.conf.set(f"fs.azure.account.key.{TARGET_ACCOUNT}.dfs.core.windows.net", target_key_env)
            spark.conf.set(f"fs.azure.account.key.{TARGET_ACCOUNT}.blob.core.windows.net", target_key_env)
        if landing_key_env:
            spark.conf.set(f"fs.azure.account.key.{SOURCE_ACCOUNT}.dfs.core.windows.net", landing_key_env)
            spark.conf.set(f"fs.azure.account.key.{SOURCE_ACCOUNT}.blob.core.windows.net", landing_key_env)
        
        # Protocolo: ABFSS no Databricks, WASBS local (mais comum em jars padrão)
        protocol = "abfss" if IS_DATABRICKS else "wasbs"
        
        if target_key_env:
            print(f"[Config] ✅ Acesso configurado via Account Keys (.env). Protocolo: {protocol.upper()}.")
            return protocol
        else:
            print(f"[Config] ✅ Acesso Landing configurado via Account Key. Protocolo: {protocol.upper()}.")
            return protocol
    
    # 3. Fallback: SAS Tokens (Local/Env)
    print("[Config] ℹ️ Usando configuração de fallback (SAS/Env). Protocolo: WASBS.")
    
    # Configura SAS para camadas conhecidas
    layers = {
        "raw": TARGET_RAW_URL,
        "trusted": TARGET_TRUSTED_URL,
        "refined": TARGET_REFINED_URL
    }
    
    for layer, url in layers.items():
        if url and "?" in url:
            sas = url.split("?")[1]
            spark.conf.set(f"fs.azure.sas.{layer}.{TARGET_ACCOUNT}.blob.core.windows.net", sas)
            
    # Configura SAS para Landing
    if SAS_TOKEN_BALANCA:
        spark.conf.set(f"fs.azure.sas.balancacomercial.{SOURCE_ACCOUNT}.blob.core.windows.net", SAS_TOKEN_BALANCA)
    if SAS_TOKEN_CNPJ:
        spark.conf.set(f"fs.azure.sas.cnpj.{SOURCE_ACCOUNT}.blob.core.windows.net", SAS_TOKEN_CNPJ)
        
    return "wasbs"

def get_base_path(container, account_type="target", protocol="wasbs"):
    """Gera o caminho base compatível com o protocolo."""
    account = TARGET_ACCOUNT if account_type == "target" else SOURCE_ACCOUNT
    
    if protocol == "abfss":
        return f"abfss://{container}@{account}.dfs.core.windows.net"
    else:
        return f"wasbs://{container}@{account}.blob.core.windows.net"




# Debug de Credenciais (Seguro)
print(f"[Config] SOURCE Account: {SOURCE_ACCOUNT}")
print(f"[Config] TARGET Account: {TARGET_ACCOUNT}")
print(f"[Config] TARGET RAW URL encontrada? {'SIM' if TARGET_RAW_URL else 'NAO'}")
print(f"[Config] TARGET TRUSTED URL encontrada? {'SIM' if TARGET_TRUSTED_URL else 'NAO'}")
print(f"[Config] TARGET REFINED URL encontrada? {'SIM' if TARGET_REFINED_URL else 'NAO'}")

if not TARGET_RAW_URL:
    print("\n[ERRO CRITICO] Variáveis de ambiente não encontradas!")
    print("   O arquivo .env NÃO é sincronizado com o Git por segurança.")
    print("   No Databricks, você deve:")
    print("   1. Criar o arquivo .env manualmente na raiz do Repo (Upload ou Editor).")
    print("   2. OU configurar as variáveis de ambiente no Cluster (Compute > Config > Environment Variables).")
    print("   3. OU configurar Secrets (scope='project-secrets').")
    print(f"   Caminho esperado do .env: {env_path}\n")

# Definição da Estrutura de Containers e Pastas
STRUCTURE = {
    "landing": {
        "containers": ["balancacomercial", "cnpj"],
        "description": "Camada de origem (Landing Zone)",
        "credentials": {
            "balancacomercial": SAS_TOKEN_BALANCA,
            "cnpj": SAS_TOKEN_CNPJ
        }
    },
    "raw": {
        "containers": ["raw"],
        "folders": {
            "raw": [
                "balancacomercial", "cnpj"
            ]
        },
        "description": "Camada Raw (Bronze)",
        "url": TARGET_RAW_URL
    },
    "trusted": {
        "containers": ["trusted"],
        "folders": {
            "trusted": ["balancacomercial", "cnpj"] 
        },
        "description": "Camada Trusted (Silver)",
        "url": TARGET_TRUSTED_URL
    },
    "refined": {
        "containers": ["refined"],
        "folders": {
            "refined": [] 
        },
        "description": "Camada Refined (Gold)",
        "url": TARGET_REFINED_URL
    }
}


def get_target_url(layer):
    """Retorna a URL completa (com SAS) para a camada especificada."""
    if layer == "raw": return TARGET_RAW_URL
    if layer == "trusted": return TARGET_TRUSTED_URL
    if layer == "refined": return TARGET_REFINED_URL
    return None
