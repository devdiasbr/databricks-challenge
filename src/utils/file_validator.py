import os
import logging
import zipfile
import csv
import shutil
import tempfile
from typing import Dict, Any, Tuple, List, Optional

# Configuração básica de log
logger = logging.getLogger(__name__)

class SmartFileLoader:
    """
    Classe utilitária inteligente para inspeção e preparação de arquivos para ingestão Spark.
    Capacidades:
    - Detecção automática de formato.
    - Descompactação de ZIPs.
    - Inferência de delimitadores CSV (, ; \t |).
    - Validação de extensões.
    """
    
    SUPPORTED_EXTENSIONS = {'.csv', '.txt', '.parquet', '.json', '.xlsx', '.zip'}
    
    def __init__(self, temp_dir: Optional[str] = None):
        """
        Args:
            temp_dir: Diretório temporário para extração. Se None, usa o do sistema.
        """
        self.temp_dir = temp_dir or tempfile.gettempdir()

    def inspect_and_prepare(self, file_path: str) -> Dict[str, Any]:
        """
        Analisa um arquivo e retorna instruções de leitura para o Spark.
        Se for ZIP, extrai e analisa o primeiro arquivo contido.
        
        Returns:
            Dict com chaves: 'format', 'path', 'options' (dict), 'is_compressed'
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        
        # 1. Tratamento de Arquivos Compactados (ZIP)
        if ext == '.zip':
            logger.info(f"📦 Arquivo ZIP detectado: {file_path}")
            extracted_files = self._extract_zip(file_path)
            if not extracted_files:
                raise ValueError(f"O arquivo ZIP {file_path} está vazio ou corrompido.")
            
            # Por simplicidade, assume ingestão do primeiro arquivo relevante encontrado
            # Em um cenário real, poderia retornar uma lista de arquivos a processar
            target_file = extracted_files[0]
            logger.info(f"   -> Processando conteúdo extraído: {target_file}")
            
            # Recursão para analisar o arquivo extraído
            result = self.inspect_and_prepare(target_file)
            result['original_zip'] = file_path
            return result

        # 2. Tratamento por Formato
        if ext in ['.csv', '.txt']:
            return self._analyze_text_file(file_path)
            
        elif ext == '.parquet':
            return {
                'format': 'parquet',
                'path': file_path,
                'options': {},
                'is_compressed': False
            }
            
        elif ext == '.json':
            return {
                'format': 'json',
                'path': file_path,
                'options': {'multiLine': True}, # Default seguro
                'is_compressed': False
            }
            
        elif ext == '.xlsx':
             # Requer biblioteca externa (com.crealytics.spark.excel ou pandas fallback)
             # Aqui configuramos para o plugin Spark-Excel padrão se disponível
            return {
                'format': 'com.crealytics.spark.excel', 
                'path': file_path,
                'options': {
                    'header': 'true', 
                    'inferSchema': 'false',
                    'dataAddress': "'Sheet1'!" # Default comum
                },
                'is_compressed': False,
                'note': 'Requer pacote Maven com.crealytics:spark-excel'
            }
            
        else:
            logger.warning(f"⚠️ Formato desconhecido ou não suportado nativamente: {ext}")
            return {
                'format': 'unknown',
                'path': file_path,
                'options': {},
                'is_compressed': False
            }

    def _extract_zip(self, zip_path: str) -> List[str]:
        """Extrai ZIP para pasta temporária e retorna caminhos dos arquivos extraídos."""
        try:
            extract_to = os.path.join(self.temp_dir, 'ingestion_staging', os.path.basename(zip_path).replace('.', '_'))
            if os.path.exists(extract_to):
                shutil.rmtree(extract_to) # Limpa extração anterior se existir
            os.makedirs(extract_to, exist_ok=True)
            
            extracted_paths = []
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
                for file_name in zip_ref.namelist():
                    full_path = os.path.join(extract_to, file_name)
                    if os.path.isfile(full_path) and not file_name.startswith('__MACOSX'):
                        extracted_paths.append(full_path)
                        
            return extracted_paths
        except zipfile.BadZipFile:
            logger.error(f"❌ Erro: Arquivo ZIP corrompido: {zip_path}")
            return []

    def _analyze_text_file(self, file_path: str) -> Dict[str, Any]:
        """Tenta detectar delimitador de arquivos de texto."""
        delimiter = ',' # Default
        try:
            # Lê apenas os primeiros 4KB para 'cheirar' o formato
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(4096)
                if not sample:
                    logger.warning(f"⚠️ Arquivo vazio: {file_path}")
                else:
                    try:
                        dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t', '|'])
                        delimiter = dialect.delimiter
                        logger.info(f"🔍 Delimitador detectado para {os.path.basename(file_path)}: '{delimiter}'")
                    except csv.Error:
                        logger.warning(f"⚠️ Não foi possível determinar delimitador automaticamente. Usando padrão ','")
        except Exception as e:
            logger.error(f"Erro ao analisar arquivo texto: {e}")

        return {
            'format': 'csv',
            'path': file_path,
            'options': {
                'header': 'true', # Pode ser ajustado depois se necessário
                'delimiter': delimiter,
                'inferSchema': 'false', # Performance default
                'encoding': 'UTF-8' # Default, mas pode precisar ser ISO-8859-1 para dados BR
            },
            'is_compressed': False
        }

# Função wrapper simples para manter compatibilidade ou uso rápido
def get_file_info(file_path: str):
    loader = SmartFileLoader()
    return loader.inspect_and_prepare(file_path)
