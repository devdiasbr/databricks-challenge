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

# COMMAND ----------

# --- Helpers de Configuração ---
def get_config(key, default=None):
    """
    Tenta recuperar configuração de múltiplas fontes:
    1. Variável de Ambiente (OS)
    2. Dbutils Secrets (se disponível no Databricks)
    """
    # 1. Tenta Env Var
    val = os.getenv(key)
    if val: return val
    
    # 2. Tenta Dbutils (apenas se estiver no Databricks)
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        # Assume um escopo padrão ou tenta buscar de um escopo configurado
        # Ajuste 'databricks-challenge-secrets' conforme necessário
        return dbutils.secrets.get(scope="project-secrets", key=key)
    except Exception:
        pass
        
    return default

# COMMAND ----------

# --- Configurações de Storage ---

# 1. ORIGEM (Landing)
BALANCA_URL = get_config("BALANCA_ACCOUNT_URL")
CNPJ_URL = get_config("CNPJ_ACCOUNT_URL")

# Extrai nome da conta (ex: https://landingbeca2026jan.blob...)
SOURCE_ACCOUNT = "landingbeca2026jan" # Fallback
if BALANCA_URL:
    SOURCE_ACCOUNT = BALANCA_URL.split("https://")[1].split(".")[0]
elif CNPJ_URL:
    SOURCE_ACCOUNT = CNPJ_URL.split("https://")[1].split(".")[0]

# Tokens específicos de Origem
SAS_TOKEN_BALANCA = get_config("AZURE_STORAGE_SAS_TOKEN_BALANCA")
SAS_TOKEN_CNPJ = get_config("AZURE_STORAGE_SAS_TOKEN_CNPJ")
LANDING_ACCOUNT_KEY = get_config("AZURE_STORAGE_ACCOUNT_KEY_LANDING")

# 2. DESTINO (Lakehouse: Raw, Trusted, Refined)
TARGET_RAW_URL = get_config("AZURE_TARGET_STORAGE_RAW_URL")
TARGET_TRUSTED_URL = get_config("AZURE_TARGET_STORAGE_TRUSTED_URL")
TARGET_REFINED_URL = get_config("AZURE_TARGET_STORAGE_REFINED_URL")

# Extrai nome da conta de destino (Prioridade: Env Var > URL > Default)
TARGET_ACCOUNT = get_config("TARGET_ACCOUNT") or get_config("AZURE_TARGET_ACCOUNT")

if not TARGET_ACCOUNT and TARGET_RAW_URL:
    try:
        TARGET_ACCOUNT = TARGET_RAW_URL.split("https://")[1].split(".")[0]
    except Exception:
        pass



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
                "balancacomercial",
                "empresas", "estabelecimentos", "socios", "cnaes", 
                "municipios", "naturezas", "paises", "simples", 
                "motivos", "qualificacoes", "outros"
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

# COMMAND ----------

def get_target_url(layer):
    """Retorna a URL completa (com SAS) para a camada especificada."""
    if layer == "raw": return TARGET_RAW_URL
    if layer == "trusted": return TARGET_TRUSTED_URL
    if layer == "refined": return TARGET_REFINED_URL
    return None
