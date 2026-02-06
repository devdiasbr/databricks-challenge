import os
import sys
import subprocess
import logging
import time

# Adiciona diretório src ao path para importar utils
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(src_dir)
sys.path.append(src_dir)

# Tenta importar o handler de log customizado, se falhar usa stream padrão
try:
    from utils.logging_utils import TqdmLoggingHandler
except ImportError:
    class TqdmLoggingHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                print(msg)
            except Exception:
                self.handleError(record)

# Configura Logger
logger = logging.getLogger("SetupPipeline")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TqdmLoggingHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(handler)

def run_command(command, description):
    """Executa um comando no shell e verifica o status de saída."""
    logger.info(f"🚀 Iniciando: {description}")
    logger.info(f"   Comando: {command}")
    
    try:
        # Executa o comando e aguarda o término
        # check=True lança exceção se o exit code for != 0
        subprocess.run(command, shell=True, check=True, cwd=project_root)
        logger.info(f"✅ Sucesso: {description}\n")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Falha: {description}")
        logger.error(f"   Erro: O comando retornou código {e.returncode}")
        return False
    except Exception as e:
        logger.error(f"❌ Erro inesperado em {description}: {e}")
        return False

import argparse

def main():
    parser = argparse.ArgumentParser(description="Setup e Orquestração do Pipeline")
    parser.add_argument("--skip-deps", action="store_true", help="Pula a instalação de dependências")
    parser.add_argument("--skip-bronze-balanca", action="store_true", help="Pula Ingestão Bronze - Balança")
    parser.add_argument("--skip-bronze-cnpj", action="store_true", help="Pula Ingestão Bronze - CNPJ")
    parser.add_argument("--skip-silver-balanca", action="store_true", help="Pula Transformação Silver - Balança")
    parser.add_argument("--skip-silver-cnpj", action="store_true", help="Pula Transformação Silver - CNPJ")
    args = parser.parse_args()

    logger.info("🔧 === Setup e Orquestração do Pipeline === 🔧\n")
    
    start_total = time.time()

    # 1. Instalação de Dependências
    if not args.skip_deps:
        if not run_command("pip install -r requirements.txt", "Instalação de Dependências"):
            logger.error("🛑 Pipeline interrompido devido a erro nas dependências.")
            sys.exit(1)
    else:
        logger.info("⏭️ Pulando Instalação de Dependências")

    # 2. Camada Bronze (Ingestão)
    bronze_scripts = []
    if not args.skip_bronze_balanca:
        bronze_scripts.append(("src/scripts/03_ingestao_bronze_balanca.py", "Ingestão Bronze - Balança Comercial"))
    else:
        logger.info("⏭️ Pulando Ingestão Bronze - Balança")

    if not args.skip_bronze_cnpj:
        bronze_scripts.append(("src/scripts/04_ingestao_bronze_cnpj.py", "Ingestão Bronze - CNPJ"))
    else:
        logger.info("⏭️ Pulando Ingestão Bronze - CNPJ")

    for script, desc in bronze_scripts:
        cmd = f"python {script}"
        if not run_command(cmd, desc):
            logger.error(f"🛑 Pipeline interrompido na etapa Bronze: {desc}")
            sys.exit(1)

    # 3. Camada Silver (Transformação)
    silver_scripts = []
    if not args.skip_silver_balanca:
        silver_scripts.append(("src/scripts/05_transformacao_silver_balanca.py", "Transformação Silver - Balança Comercial"))
    else:
        logger.info("⏭️ Pulando Transformação Silver - Balança")

    if not args.skip_silver_cnpj:
        silver_scripts.append(("src/scripts/06_transformacao_silver_cnpj.py", "Transformação Silver - CNPJ"))
    else:
        logger.info("⏭️ Pulando Transformação Silver - CNPJ")

    for script, desc in silver_scripts:
        cmd = f"python {script}"
        if not run_command(cmd, desc):
            logger.error(f"🛑 Pipeline interrompido na etapa Silver: {desc}")
            sys.exit(1)

    duration = time.time() - start_total
    logger.info(f"🎉 Pipeline Completo Finalizado com Sucesso! Tempo total: {duration:.2f}s")

if __name__ == "__main__":
    main()
