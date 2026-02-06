import os
import logging

# Configuração básica de log
logger = logging.getLogger(__name__)

def validate_extensions(file_list, allowed_extensions):
    """
    Valida uma lista de arquivos contra extensões permitidas.
    
    Args:
        file_list (list): Lista de nomes de arquivos ou caminhos.
        allowed_extensions (list): Lista de extensões permitidas (ex: ['.csv', '.zip', 'parquet']).
        
    Returns:
        tuple: (arquivos_validos, arquivos_invalidos)
    """
    valid_files = []
    invalid_files = []
    
    # Normaliza extensões para set, lower case, com ponto
    allowed = set()
    for ext in allowed_extensions:
        ext = ext.strip().lower()
        if not ext.startswith('.'):
            ext = '.' + ext
        allowed.add(ext)
        
    for file_path in file_list:
        # Extrai apenas a extensão para verificação
        # os.path.splitext retorna (root, .ext)
        _, ext = os.path.splitext(file_path)
        
        if ext.lower() in allowed:
            valid_files.append(file_path)
        else:
            invalid_files.append(file_path)
            
    if invalid_files:
        logger.warning(f"⚠️  Foram encontrados {len(invalid_files)} arquivos com extensão não permitida (ignorados):")
        for f in invalid_files[:5]: # Loga apenas os primeiros 5 para não poluir
            logger.warning(f"    - {f}")
        if len(invalid_files) > 5:
            logger.warning(f"    ... e mais {len(invalid_files) - 5} arquivos.")
            
    return valid_files, invalid_files

def is_valid_extension(file_path, allowed_extensions):
    """
    Verifica se um único arquivo tem extensão válida.
    """
    valid, _ = validate_extensions([file_path], allowed_extensions)
    return len(valid) > 0
