# Databricks notebook source
import unicodedata
from pyspark.sql.functions import col, trim, upper
from pyspark.sql.types import StringType

# COMMAND ----------

def normalize_column_name(col_name):
    """Padroniza nomes de colunas: minúsculas, sem acentos, sem espaços."""
    # Remove acentos
    nfkd_form = unicodedata.normalize('NFKD', col_name)
    col_name = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    
    # Minúsculas e substitui espaços/hífens por underscore
    return col_name.lower().strip().replace(' ', '_').replace('-', '_')

# COMMAND ----------

class BaseTransform:
    """
    Transformações genéricas reutilizáveis para DataFrames.
    """
    def __init__(self, df):
        self.df = df

    def rename_columns(self, mapping):
        """
        Renomeia colunas. 
        - Se mapping for dict: renomeia por chave -> valor.
        - Se mapping for list: renomeia posicionalmente.
        """
        if isinstance(mapping, dict):
            print("  🔄 [BaseTransform] Renomeando colunas via Dicionário (Key-Value)...")
            for old_col, new_col in mapping.items():
                if old_col in self.df.columns:
                    self.df = self.df.withColumnRenamed(old_col, new_col)
                    
        elif isinstance(mapping, list):
            print("  🔄 [BaseTransform] Renomeando colunas via Lista (Posicional)...")
            current_cols = self.df.columns
            # Garante que não tenta renomear mais colunas do que existem
            limit = min(len(current_cols), len(mapping))
            
            for i in range(limit):
                old_col = current_cols[i]
                new_col = mapping[i]
                # Se o nome já for igual, pula (mas normaliza depois)
                self.df = self.df.withColumnRenamed(old_col, new_col)
                
        else:
            print(f"  ⚠️ [BaseTransform] Tipo de mapeamento não suportado: {type(mapping)}")
            
        return self

    def normalize_headers(self):
        """Padroniza nomes de colunas: minúsculas, sem acentos, sem espaços."""
        print("  🛠️ [BaseTransform] Padronizando cabeçalhos (fallback)...")
        new_columns = [normalize_column_name(c) for c in self.df.columns]
        self.df = self.df.toDF(*new_columns)
        return self

    def drop_duplicates(self):
        """Remove linhas duplicadas."""
        print("  🧹 [BaseTransform] Removendo duplicados...")
        self.df = self.df.dropDuplicates()
        return self

    def treat_nulls(self):
        """Preenche nulos de forma básica (0 para numéricos, 'N/A' para strings)."""
        print("  null [BaseTransform] Tratando valores nulos...")
        self.df = self.df.fillna(0).fillna("N/A")
        return self

    def drop_foreign_columns(self):
        """Remove colunas com sufixos de outros idiomas (_ING, _ESP, etc)."""
        print("  🚫 [BaseTransform] Removendo colunas em inglês/espanhol...")
        
        # Como normalize_headers roda antes, assumimos nomes em minúsculo
        suffixes = ('_ing', '_esp', '_en', '_es', '_english', '_spanish')
        cols_to_drop = [c for c in self.df.columns if c.endswith(suffixes)]
        
        if cols_to_drop:
            print(f"    - Removendo colunas: {cols_to_drop}")
            self.df = self.df.drop(*cols_to_drop)
        return self

    def trim_strings(self):
        """Aplica trim em todas as colunas de string."""
        print("  ✂️ [BaseTransform] Aplicando TRIM em colunas de texto...")
        string_cols = [f.name for f in self.df.schema.fields if isinstance(f.dataType, StringType)]
        for c in string_cols:
            self.df = self.df.withColumn(c, trim(col(c)))
        return self
        
    def upper_strings(self):
        """Converte todas as colunas de string para maiúsculas (opcional, bom para padronização)."""
        print("  🔠 [BaseTransform] Convertendo textos para UPPERCASE...")
        string_cols = [f.name for f in self.df.schema.fields if isinstance(f.dataType, StringType)]
        for c in string_cols:
            self.df = self.df.withColumn(c, upper(col(c))) 
        return self

    def get_dataframe(self):
        return self.df
