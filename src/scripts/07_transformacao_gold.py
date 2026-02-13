import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Script de Transformação para a Camada Gold (Refined).
Consolida as dimensões e fatos para consumo final, incluindo a unificação da dimensão de geografia.
"""

# Configura Logger
logger = logging.getLogger("GoldTransformation")
logger.setLevel(logging.INFO)
if not logger.handlers:
    try:
        handler = TqdmLoggingHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
        logger.addHandler(handler)
    except Exception:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

# Configuração de caminhos
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

# Navega para cima até encontrar a pasta 'src'
project_root = base_dir
while not os.path.exists(os.path.join(project_root, 'src')) and project_root != os.path.dirname(project_root):
    project_root = os.path.dirname(project_root)

# Adiciona src ao path
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.append(src_path)

import utils.config as config
from utils.logging_utils import TqdmLoggingHandler


# Constantes de Auditoria
CAMPO_ATUALIZACAO = "data_processamento_gold"

def get_spark_session():
    """Obtém a sessão Spark ativa (Databricks)."""
    return SparkSession.builder.getOrCreate()


def adicionar_metadados(df):
    """Adiciona colunas de auditoria."""
    return df \
        .withColumn(CAMPO_ATUALIZACAO, F.current_timestamp())

def salvar_tabela_gold(df, nome_tabela, particionar_por=None, protocol="wasbs"):
    """Salva tabela na camada Gold."""
    logger.info(f"💾 Salvando {nome_tabela}...")
    path = f"{config.get_base_path('refined', 'target', protocol)}/{nome_tabela}"
    
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if particionar_por:
        writer = writer.partitionBy(*particionar_por)
    
    writer.save(path)
    logger.info(f"✅ {nome_tabela} salva com sucesso!")

def processar_gold():
    """Executa o processamento completo da camada Gold."""
    logger.info("🚀 Iniciando Transformação Camada Gold...")
    
    spark = get_spark_session()
    protocol = config.configure_spark_access(spark)
    
    # URLs base
    base_url_trusted = config.get_base_path('trusted', 'target', protocol)

    
    # ==================================================================================
    # 1. Dimensão Data (dim_data)
    # ==================================================================================
    logger.info("\n📅 Processando Dimensão Data...")
    
    try:
        # Mapeamento de Meses
        meses_dict = {
            1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
            5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
            9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
        }
        map_meses = F.create_map([F.lit(x) for i in meses_dict.items() for x in i])

        # Lê tabelas EXP e IMP
        df_exp = spark.read.format("delta").load(f"{base_url_trusted}/exp")
        df_imp = spark.read.format("delta").load(f"{base_url_trusted}/imp")
        
        # Ajuste de colunas conforme notebook (EXP usa co_ano/co_mes, IMP usa ano/mes)
        df_datas_exp = df_exp.select(F.col("co_ano").alias("ano"), F.col("co_mes").alias("mes")).distinct()
        df_datas_imp = df_imp.select(F.col("ano"), F.col("mes")).distinct()
        
        df_datas = df_datas_exp.union(df_datas_imp).distinct()
        
        df_dim_data = df_datas \
            .withColumn("data", F.to_date(F.concat_ws("-", F.col("ano"), F.col("mes"), F.lit("01")))) \
            .withColumn("sk_data", F.expr("ano * 100 + mes")) \
            .withColumn("trimestre", F.ceil(F.col("mes") / 3).cast("int")) \
            .withColumn("semestre", F.ceil(F.col("mes") / 6).cast("int")) \
            .withColumn("nome_mes", F.coalesce(map_meses[F.col("mes")], F.lit("Desconhecido"))) \
            .select("sk_data", "data", "ano", "mes", "trimestre", "semestre", "nome_mes") \
            .orderBy("sk_data")
            
        df_dim_data = adicionar_metadados(df_dim_data)
        salvar_tabela_gold(df_dim_data, "dim_data", particionar_por=["ano"], protocol=protocol)
        
    except Exception as e:
        logger.error(f"❌ Erro em dim_data: {e}")

    # -------------------------------------------------------------------------
    # 2. DIMENSÃO NCM
    # -------------------------------------------------------------------------
    logger.info("📦 Processando Dimensão NCM...")
    df_ncm_trusted = spark.read.format("delta").load(f"{base_url_trusted}/ncm")
    
    df_dim_ncm = df_ncm_trusted.select(
        F.col("co_ncm").cast("bigint").alias("sk_ncm"),
        F.col("co_ncm").alias("codigo_ncm"),
        F.coalesce(F.col("no_ncm_pt"), F.lit("Descrição não disponível")).alias("nome_ncm"),
        F.col("no_fat_agreg").alias("fator_agregado"),
        F.col("no_ppe_ppe").alias("nomenclatura_ppe")
    ).dropDuplicates(["sk_ncm"])
    
    df_dim_ncm = adicionar_metadados(df_dim_ncm)
    salvar_tabela_gold(df_dim_ncm, "dim_ncm")
    
    # -------------------------------------------------------------------------
    # 3. DIMENSÃO VIA TRANSPORTE
    # -------------------------------------------------------------------------
    logger.info("🚢 Processando Dimensão Via Transporte...")
    df_via_trusted = spark.read.format("delta").load(f"{base_url_trusted}/via")
    
    df_dim_via = df_via_trusted.select(
        F.col("co_via").cast("bigint").alias("sk_via_transporte"),
        F.col("co_via").alias("codigo_via"),
        F.coalesce(F.col("no_via"), F.lit("Via não informada")).alias("nome_via")
    ).dropDuplicates(["sk_via_transporte"])
    
    df_dim_via = adicionar_metadados(df_dim_via)
    salvar_tabela_gold(df_dim_via, "dim_via_transporte")
    
    # -------------------------------------------------------------------------
    # 4. DIMENSÃO GEOGRAFIA (UNIFICADA)
    # -------------------------------------------------------------------------
    logger.info("🌍 Processando Dimensão Geografia Unificada...")
    
    # Países
    df_paises_trusted = spark.read.format("delta").load(f"{base_url_trusted}/paises")
    df_dim_paises = df_paises_trusted.select(
        F.col("co_pais").cast("bigint").alias("sk_geografia"),
        F.col("co_pais").alias("codigo_referencia"),
        F.coalesce(F.col("no_pais"), F.lit("País Desconhecido")).alias("nome_geografia"),
        F.lit("PAIS").alias("nivel"),
        F.col("sg_pais").alias("sigla"),
        F.col("no_bloco").alias("regiao_ou_bloco")
    )

    # UFs (Extraídas das tabelas de exportação/importação)
    df_ufs_exp = df_exp.select(F.col("sg_uf_ncm").alias("uf")).distinct()
    df_ufs_imp = df_imp.select(F.col("uf_destino").alias("uf")).distinct()
    df_ufs_unicas = df_ufs_exp.union(df_ufs_imp).distinct().filter(F.col("uf").isNotNull())

    df_dim_ufs = df_ufs_unicas.select(
        (F.expr("ascii(substring(uf, 1, 1)) * 100 + ascii(substring(uf, 2, 1))")).cast("bigint").alias("sk_geografia"),
        F.col("uf").alias("codigo_referencia"),
        F.col("uf").alias("nome_geografia"), # Poderia enriquecer com nome completo se houvesse de-para
        F.lit("UF").alias("nivel"),
        F.col("uf").alias("sigla"),
        F.lit("Brasil").alias("regiao_ou_bloco")
    )

    df_dim_geografia = df_dim_paises.unionByName(df_dim_ufs)
    df_dim_geografia = adicionar_metadados(df_dim_geografia)
    salvar_tabela_gold(df_dim_geografia, "dim_geografia")

    # -------------------------------------------------------------------------
    # 5. FATO BALANÇO COMERCIAL (UNIFICADA)
    # -------------------------------------------------------------------------
    logger.info("📊 Processando Fato Balanço Comercial...")
    
    df_exp = spark.read.format("delta").load(f"{base_url_trusted}/exp") \
        .select(
            F.col("co_ncm").cast("bigint").alias("sk_ncm"),
            F.col("co_pais").cast("bigint").alias("sk_geografia"),
            F.expr("ascii(substring(coalesce(sg_uf_ncm, 'XX'), 1, 1)) * 100 + ascii(substring(coalesce(sg_uf_ncm, 'XX'), 2, 1))").cast("bigint").alias("sk_geografia_uf"),
            F.col("co_via").cast("bigint").alias("sk_via_transporte"),
            F.expr("co_ano * 100 + co_mes").cast("bigint").alias("sk_data"),
            F.lit("EXPORTACAO").alias("tipo_movimentacao"),
            F.col("vl_fob").cast("decimal(18,2)").alias("valor_fob"),
            F.col("qt_estat").cast("decimal(18,2)").alias("quantidade"),
            F.col("kg_liquido").cast("decimal(18,2)").alias("kg_liquido")
        )
    
    df_imp = spark.read.format("delta").load(f"{base_url_trusted}/imp") \
        .select(
            F.col("codigo_ncm").cast("bigint").alias("sk_ncm"),
            F.col("codigo_pais_origem").cast("bigint").alias("sk_geografia"),
            F.expr("ascii(substring(coalesce(uf_destino, 'XX'), 1, 1)) * 100 + ascii(substring(coalesce(uf_destino, 'XX'), 2, 1))").cast("bigint").alias("sk_geografia_uf"),
            F.col("codigo_via_transporte").cast("bigint").alias("sk_via_transporte"),
            F.expr("ano * 100 + mes").cast("bigint").alias("sk_data"),
            F.lit("IMPORTACAO").alias("tipo_movimentacao"),
            F.col("valor_fob_usd").cast("decimal(18,2)").alias("valor_fob"),
            F.col("quantidade_estatistica").cast("decimal(18,2)").alias("quantidade"),
            F.col("peso_liquido_kg").cast("decimal(18,2)").alias("kg_liquido")
        )
    
    df_ft_balanco = df_exp.unionByName(df_imp, allowMissingColumns=True) \
        .filter(F.col("sk_ncm").isNotNull()) \
        .filter(F.col("sk_data").isNotNull())
    
    df_ft_balanco = adicionar_metadados(df_ft_balanco)
    salvar_tabela_gold(df_ft_balanco, "ft_balanco_comercial", particionar_por=["tipo_movimentacao"])
    
    logger.info("🏁 Transformação Gold concluída com sucesso!")

if __name__ == "__main__":
    processar_gold()
