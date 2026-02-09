# Databricks notebook source
import os
import sys

# COMMAND ----------

# Adiciona o diretório 'src' ao sys.path para permitir imports de 'utils'
# Configuração robusta de caminhos (Híbrido Local/Databricks)
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

# Navega para cima até encontrar a pasta 'src'
current_dir = base_dir
while not os.path.exists(os.path.join(current_dir, 'src')) and current_dir != os.path.dirname(current_dir):
    current_dir = os.path.dirname(current_dir)

src_dir = os.path.join(current_dir, 'src')
if src_dir not in sys.path:
    sys.path.append(src_dir)

from azure.storage.blob import ContainerClient
from utils.config import STRUCTURE, get_target_url

# COMMAND ----------

def ensure_container_accessible(container_url, container_name):
    """Verifica se o container é acessível via URL SAS."""
    try:
        container_client = ContainerClient.from_container_url(container_url)
        # Tenta listar apenas 1 blob para validar acesso
        blobs = container_client.list_blobs(results_per_page=1)
        next(blobs, None) 
        print(f"    ✅ Container '{container_name}' acessível.")
        return container_client
    except Exception as e:
        print(f"    ❌ Erro ao acessar container '{container_name}': {e}")
        if "AuthenticationFailed" in str(e):
             print("       (Verifique se o token SAS expirou ou está incorreto)")
        return None

def setup_and_validate_targets():
    """Valida a estrutura de pastas e containers de destino (Raw, Trusted, Refined)."""
    
    print(f"--- 🛠️ Inicializando/Validando Estrutura de Targets ---")
    
    # Camadas a validar
    layers = ["raw", "trusted", "refined"]
    
    for layer in layers:
        layer_config = STRUCTURE.get(layer)
        if not layer_config: continue
        
        print(f"\n🏗️  Verificando Camada: {layer.upper()} ({layer_config['description']})")
        
        container_url = get_target_url(layer)
        if not container_url:
            print(f"  ❌ URL não configurada no .env para a camada {layer}")
            continue

        # Validar Container
        # Como temos URL SAS específica por container (definida no .env),
        # iteramos sobre os containers definidos no config (geralmente é 1:1 com a camada)
        for container_name in layer_config.get("containers", []):
            # Nota: Assume-se que a URL do .env corresponde ao container principal da camada
            # Se houver múltiplos containers por camada, precisaria de lógica mais complexa de URL
            
            container_client = ensure_container_accessible(container_url, container_name)
            
            if container_client:
                # Validar Pastas (Estrutura interna)
                expected_folders = layer_config.get("folders", {}).get(container_name, [])
                
                if expected_folders:
                    print(f"    📂 Verificando subpastas esperadas em '{container_name}':")
                    try:
                        existing_blobs = list(container_client.list_blobs())
                        existing_prefixes = set()
                        for b in existing_blobs:
                            if '/' in b.name:
                                existing_prefixes.add(b.name.split('/')[0])
                                
                        for folder in expected_folders:
                            if folder in existing_prefixes:
                                print(f"      🔹 {folder}/ (Existe)")
                            else:
                                print(f"      🔸 {folder}/ (Pendente - será criado na ingestão)")
                    except Exception as e:
                         print(f"      ⚠️ Não foi possível listar pastas: {e}")

    print("\n✅ Validação Concluída.")

# COMMAND ----------

if __name__ == "__main__":
    setup_and_validate_targets()
