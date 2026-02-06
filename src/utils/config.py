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

print(f"🔄 [Config] Tentando carregar .env de: {env_path}")
loaded = load_dotenv(dotenv_path=env_path)

if not loaded:
    print("⚠️ [Config] .env não encontrado no caminho explícito. Tentando busca padrão...")
    load_dotenv()

# --- Configurações de Storage ---

# 1. ORIGEM (Landing)
# No .env temos URLs separadas para Balança e CNPJ. Vamos tentar extrair a conta de uma delas.
BALANCA_URL = os.getenv("BALANCA_ACCOUNT_URL")
CNPJ_URL = os.getenv("CNPJ_ACCOUNT_URL")

# Extrai nome da conta (ex: https://landingbeca2026jan.blob...)
SOURCE_ACCOUNT = "landingbeca2026jan" # Fallback
if BALANCA_URL:
    SOURCE_ACCOUNT = BALANCA_URL.split("https://")[1].split(".")[0]
elif CNPJ_URL:
    SOURCE_ACCOUNT = CNPJ_URL.split("https://")[1].split(".")[0]

# Tokens específicos de Origem
SAS_TOKEN_BALANCA = os.getenv("AZURE_STORAGE_SAS_TOKEN_BALANCA")
SAS_TOKEN_CNPJ = os.getenv("AZURE_STORAGE_SAS_TOKEN_CNPJ")

# 2. DESTINO (Lakehouse: Raw, Trusted, Refined)
# No .env temos URLs completas (com SAS) para cada container
TARGET_RAW_URL = os.getenv("AZURE_TARGET_STORAGE_RAW_URL")
TARGET_TRUSTED_URL = os.getenv("AZURE_TARGET_STORAGE_TRUSTED_URL")
TARGET_REFINED_URL = os.getenv("AZURE_TARGET_STORAGE_REFINED_URL")

# Debug de Credenciais (Seguro)
print(f"ℹ️ [Config] SOURCE Account: {SOURCE_ACCOUNT}")
print(f"ℹ️ [Config] TARGET RAW URL encontrada? {'✅' if TARGET_RAW_URL else '❌'}")
print(f"ℹ️ [Config] TARGET TRUSTED URL encontrada? {'✅' if TARGET_TRUSTED_URL else '❌'}")
print(f"ℹ️ [Config] TARGET REFINED URL encontrada? {'✅' if TARGET_REFINED_URL else '❌'}")

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
                "balanco_comercial",
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
            "trusted": ["balanca_comercial", "cnpj"] 
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
