# Databricks notebook source
import os
import sys

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
from utils.config import STRUCTURE

def list_landing_blobs():
    """Valida e lista arquivos na camada Landing (Origem)."""
    landing_config = STRUCTURE["landing"]
    containers = landing_config["containers"]
    
    print(f"--- 🔍 Validando Camada Landing (Origem) ---")
    
    # Busca o nome da conta diretamente de uma das URLs configuradas (para display)
    # A lógica de conexão agora é baseada nas credenciais específicas do config
    
    for container_name in containers:
        print(f"\n📦 Container: {container_name}")
        
        # Pega o token SAS específico configurado no config.py
        sas_token = landing_config["credentials"].get(container_name)
        
        if not sas_token:
            print(f"  ⚠️ Nenhuma credencial (SAS) configurada para o container '{container_name}'.")
            continue

        try:
            # Reconstrói a URL completa usando o nome do container e o SAS Token
            # Assumindo padrão Azure: https://{account}.blob.core.windows.net/{container}?{sas}
            # Como já temos as URLs base no config (BALANCA_URL), podemos usar elas se preferir,
            # mas o SAS Token do .env já contém os parametros.
            # Vamos usar a URL base do .env + SAS Token
            
            # Recupera a URL base do .env via config (precisamos expor isso no config ou reconstruir)
            # Reconstruindo para ser mais seguro com o que temos no config
            from utils.config import SOURCE_ACCOUNT
            container_url = f"https://{SOURCE_ACCOUNT}.blob.core.windows.net/{container_name}?{sas_token}"
            
            container_client = ContainerClient.from_container_url(container_url)
            
            # Verifica se container existe listando blobs (se falhar, provavel que não exista ou sem permissão)
            blobs = container_client.list_blobs()
            
            count = 0
            print("  Arquivos encontrados:")
            for blob in blobs:
                print(f"    📄 {blob.name} ({blob.size / 1024:.2f} KB)")
                count += 1
            
            print(f"  Total de arquivos encontrados: {count}")
            
            if count == 0:
                print("    (Container vazio)")
                
        except Exception as e:
            if "AuthorizationPermissionMismatch" in str(e):
                print("  ❌ Erro de Permissão: Verifique se o token tem permissão 'List'.")
            elif "AuthenticationFailed" in str(e):
                print("  ❌ Erro de Autenticação: Verifique as credenciais.")
            elif "ContainerNotFound" in str(e): 
                print("  ❌ Container não encontrado.")
            else:
                print(f"  ❌ Erro: {str(e)}")

if __name__ == "__main__":
    list_landing_blobs()
