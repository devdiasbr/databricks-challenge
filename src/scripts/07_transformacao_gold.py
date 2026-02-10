# Databricks notebook source
# MAGIC %md
# MAGIC # Modelagem Gold: Star Schema
# MAGIC 
# MAGIC Criação de dimensões e tabelas fato para análise (Silver -> Gold).

# COMMAND ----------

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Configuração robusta de caminhos (Híbrido Local/Databricks)
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

# Navega para cima até encontrar a pasta 'src' para definir o project_root
project_root = base_dir
while not os.path.exists(os.path.join(project_root, 'src')) and project_root != os.path.dirname(project_root):
    project_root = os.path.dirname(project_root)

# Fallback
if not os.path.exists(os.path.join(project_root, 'src')):
    project_root = base_dir

# Adiciona src ao path para importar utils
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.append(src_path)

# Configuração do Hadoop no Windows
hadoop_home = os.path.join(project_root, 'hadoop')
if os.path.exists(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
    hadoop_bin = os.path.join(hadoop_home, 'bin')
    if hadoop_bin not in os.environ['PATH']:
        os.environ['PATH'] += os.pathsep + hadoop_bin

import utils.config as config
from utils.logging_utils import TqdmLoggingHandler

# Configura Logger
logger = logging.getLogger("Gold_Transformation")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(handler)

# COMMAND ----------

# Constantes de Auditoria
CAMPO_ATUALIZACAO = "dt_atualizacao"
USUARIO_ATUALIZACAO = "system@pipeline" # Idealmente viria de config ou env

def get_spark_session():
    """Cria e configura a sessão Spark com suporte a Delta e Azure."""
    builder = SparkSession.builder \
        .appName("Gold_Transformation_Balanca") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6,io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.speculation", "false") \
        .config("spark.hadoop.fs.azure.enable.check.access", "false") \
        .master("local[*]")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    return spark

# COMMAND ----------

def configure_azure_access(spark):
    """Configura o acesso ao Azure Blob Storage (Trusted e Refined)."""
    layers = ["trusted", "refined"]
    account = config.TARGET_ACCOUNT
    
    for layer in layers:
        url = config.get_target_url(layer)
        if not url:
            logger.warning(f"⚠️ URL para camada {layer} não encontrada.")
            continue
            
        sas_token = ""
        if "?" in url:
            sas_token = url.split("?")[1]
            
        if sas_token:
            spark.conf.set(f"fs.azure.sas.{layer}.{account}.blob.core.windows.net", sas_token)
            logger.info(f"✅ Configurado acesso SAS para {account}/{layer}")

# COMMAND ----------

def adicionar_metadados(df):
    """Adiciona colunas de auditoria."""
    return df \
        .withColumn(CAMPO_ATUALIZACAO, F.current_timestamp()) \
        .withColumn("usuario_atualizacao", F.lit(USUARIO_ATUALIZACAO))

def salvar_tabela_gold(df, nome_tabela, particionar_por=None):
    """Salva a tabela na camada Refined (Gold) em formato Delta."""
    # Constrói URL WASBS para garantir compatibilidade com driver Hadoop-Azure e SAS Tokens
    account = config.TARGET_ACCOUNT
    target_path = f"wasbs://refined@{account}.blob.core.windows.net/{nome_tabela}"
    
    logger.info(f"💾 Salvando tabela {nome_tabela} em: {target_path}")
    
    writer = df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true")
    
    if particionar_por:
        writer = writer.partitionBy(particionar_por)
        
    writer.save(target_path)
    
    # Otimização
    try:
        spark = SparkSession.getActiveSession()
        
        # Configura tamanho alvo do arquivo para OPTIMIZE (10MB)
        spark.conf.set("spark.databricks.delta.optimize.maxFileSize", config.DELTA_OPTIMIZE_FILE_SIZE)
        
        spark.sql(f"OPTIMIZE delta.`{target_path}`")
        
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        spark.sql(f"VACUUM delta.`{target_path}` RETAIN {config.DELTA_VACUUM_RETENTION_DAYS * 24} HOURS")
        logger.info(f"⚡ Tabela {nome_tabela} otimizada.")
    except Exception as e:
        logger.warning(f"⚠️ Não foi possível otimizar {nome_tabela}: {e}")

    logger.info(f"✅ Tabela {nome_tabela} criada com sucesso! Registros: {df.count():,}")

# COMMAND ----------

def processar_gold():
    logger.info("🚀 Iniciando processamento Refined (Gold)...")
    
    spark = get_spark_session()
    configure_azure_access(spark)
    
    # URLs base
    account = config.TARGET_ACCOUNT
    base_url_trusted = f"wasbs://trusted@{account}.blob.core.windows.net/balancacomercial"
    
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
        salvar_tabela_gold(df_dim_data, "dim_data", particionar_por=["ano"])
        
    except Exception as e:
        logger.error(f"❌ Erro em dim_data: {e}")

    # ==================================================================================
    # 2. Dimensão NCM (dim_ncm)
    # ==================================================================================
    logger.info("\n📦 Processando Dimensão NCM...")
    
    try:
        df_ncm_base = spark.read.format("delta").load(f"{base_url_trusted}/ncm") \
            .select(
                F.col("codigo_ncm").alias("no_cod"),
                F.col("descricao_ncm_pt").alias("descricao_ncm")
            ).dropDuplicates(["no_cod"])
            
        try:
            path_cnae = f"{base_url_trusted}/external_ncm_cnae"
            df_ncm_cnae = spark.read.format("delta").load(path_cnae) \
                .select(
                    F.col("ncm").cast("string").alias("ncm_code"),
                    F.col("descricao").alias("descricao_ncm_cnae"),
                    F.col("cnae").cast("string").alias("codigo_cnae")
                ).dropDuplicates(["ncm_code"])
                
            df_dim_ncm = df_ncm_base.join(df_ncm_cnae, df_ncm_base.no_cod == df_ncm_cnae.ncm_code, "left")
            
        except Exception:
            logger.warning("⚠️ Tabela auxiliar NCM-CNAE não encontrada em Trusted. Seguindo sem CNAE.")
            df_dim_ncm = df_ncm_base \
                .withColumn("ncm_code", F.lit(None)) \
                .withColumn("descricao_ncm_cnae", F.lit(None)) \
                .withColumn("codigo_cnae", F.lit(None))

        df_dim_ncm = df_dim_ncm \
            .withColumn("sk_ncm", F.expr("try_cast(no_cod as bigint)")) \
            .filter(F.col("sk_ncm").isNotNull()) \
            .withColumn("codigo_ncm", F.col("no_cod")) \
            .withColumn("descricao_ncm", F.coalesce(F.col("descricao_ncm"), F.lit("Não Informado"))) \
            .withColumn("cnae", F.coalesce(F.col("codigo_cnae"), F.lit("Não Informado"))) \
            .withColumn("descricao_cnae", F.coalesce(F.col("descricao_ncm_cnae"), F.lit("Não Informado"))) \
            .withColumn("setor_economico", 
                        F.when(F.col("sk_ncm").between(1000000, 14999999), "Agro – Agropecuária")
                        .when(F.col("sk_ncm").between(25000000, 27999999), "Ind.Extr. – Industria Extrativa")
                        .when(F.col("sk_ncm").between(15000000, 24999999) | 
                              F.col("sk_ncm").between(28000000, 83999999), "Ind.Transf. – Indústria Transformação")
                        .when(F.col("sk_ncm").between(84000000, 99999999), "tecnologia")
                        .otherwise("Outros")) \
            .select("sk_ncm", "codigo_ncm", "descricao_ncm", "cnae", "descricao_cnae", "setor_economico") \
            .dropDuplicates(["sk_ncm"]) \
            .orderBy("sk_ncm")

        df_dim_ncm = adicionar_metadados(df_dim_ncm)
        salvar_tabela_gold(df_dim_ncm, "dim_ncm")
        
    except Exception as e:
        logger.error(f"❌ Erro em dim_ncm: {e}")

    # ==================================================================================
    # 3. Dimensão Países (dim_paises) - Substitui dim_localidade
    # ==================================================================================
    logger.info("\n🌎 Processando Dimensão Países...")
    
    try:
        df_paises_exp = spark.read.format("delta").load(f"{base_url_trusted}/exp") \
            .select(F.col("co_pais").alias("codigo_pais")).distinct()
            
        df_paises_imp = spark.read.format("delta").load(f"{base_url_trusted}/imp") \
            .select(F.col("codigo_pais_origem").alias("codigo_pais")).distinct()
            
        df_paises_unicos = df_paises_exp.union(df_paises_imp).distinct().filter(F.col("codigo_pais").isNotNull())
        
        df_ref_paises = spark.read.format("delta").load(f"{base_url_trusted}/pais") \
            .select(
                F.col("codigo_pais"),
                F.col("codigo_pais_iso3").alias("sigla_pais"),
                F.col("nome_pais_pt").alias("nome_pais")
            )
            
        try:
            df_bloco_pais = spark.read.format("delta").load(f"{base_url_trusted}/pais_bloco") \
                .select(
                    F.col("codigo_pais").alias("codigo_pais_bloco"),
                    F.col("nome_bloco_pt").alias("bloco_economico")
                ).distinct()
        except Exception:
            logger.warning("⚠️ Tabela PAIS_BLOCO não encontrada. Seguindo sem bloco.")
            df_bloco_pais = None

        df_dim_paises = df_paises_unicos.join(df_ref_paises, "codigo_pais", "left")
        
        if df_bloco_pais:
            df_dim_paises = df_dim_paises.join(df_bloco_pais, F.col("codigo_pais") == df_bloco_pais.codigo_pais_bloco, "left")
        else:
            df_dim_paises = df_dim_paises.withColumn("bloco_economico", F.lit(None))
            
        df_dim_paises = df_dim_paises \
            .withColumn("sk_pais", F.col("codigo_pais")) \
            .withColumn("sigla_pais", F.coalesce(F.col("sigla_pais"), F.lit("N/A"))) \
            .withColumn("nome_pais", F.coalesce(F.col("nome_pais"), F.lit("Não Informado"))) \
            .withColumn("bloco_economico", F.coalesce(F.col("bloco_economico"), F.lit("Não Informado"))) \
            .select("sk_pais", "codigo_pais", "sigla_pais", "nome_pais", "bloco_economico") \
            .dropDuplicates(["sk_pais"]) \
            .orderBy("sk_pais")
            
        df_dim_paises = adicionar_metadados(df_dim_paises)
        salvar_tabela_gold(df_dim_paises, "dim_paises")
        
    except Exception as e:
        logger.error(f"❌ Erro em dim_paises: {e}")

    # ==================================================================================
    # 4. Dimensão UFs (dim_ufs)
    # ==================================================================================
    logger.info("\n🇧🇷 Processando Dimensão UFs...")
    
    try:
        # Mapeamento de UFs
        ufs_dict = {
            "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
            "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
            "GO": "Goiás", "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul",
            "MG": "Minas Gerais", "PA": "Pará", "PB": "Paraíba", "PR": "Paraná",
            "PE": "Pernambuco", "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
            "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina",
            "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins"
        }
        # Cria mapa do Spark para lookup eficiente
        map_ufs = F.create_map([F.lit(x) for i in ufs_dict.items() for x in i])

        # Mapeamento de Regiões
        regioes_map = {
            "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
            "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
            "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste", "DF": "Centro-Oeste",
            "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
            "PR": "Sul", "RS": "Sul", "SC": "Sul"
        }
        map_regioes = F.create_map([F.lit(x) for i in regioes_map.items() for x in i])

        df_ufs_exp = spark.read.format("delta").load(f"{base_url_trusted}/exp") \
            .select(F.col("sg_uf_ncm").alias("uf")).distinct()
            
        df_ufs_imp = spark.read.format("delta").load(f"{base_url_trusted}/imp") \
            .select(F.col("uf_destino").alias("uf")).distinct()
            
        df_ufs_unicas = df_ufs_exp.union(df_ufs_imp).distinct().filter(F.col("uf").isNotNull())
        
        df_dim_ufs = df_ufs_unicas \
            .withColumn("sk_uf", F.expr("ascii(substring(uf, 1, 1)) * 100 + ascii(substring(uf, 2, 1))")) \
            .withColumn("sigla_uf", F.col("uf")) \
            .withColumn("regiao", map_regioes[F.col("uf")]) \
            .withColumn("nome_uf", map_ufs[F.col("uf")]) \
            .filter(F.col("nome_uf").isNotNull()) \
            .withColumn("sk_pais", F.lit(105)) \
            .select("sk_uf", "sigla_uf", "nome_uf", "regiao", "sk_pais") \
            .dropDuplicates(["sk_uf"]) \
            .orderBy("sk_uf")
            
        df_dim_ufs = adicionar_metadados(df_dim_ufs)
        salvar_tabela_gold(df_dim_ufs, "dim_ufs")
        
    except Exception as e:
        logger.error(f"❌ Erro em dim_ufs: {e}")

    # ==================================================================================
    # 5. Dimensão Via Transporte (dim_via_transporte)
    # ==================================================================================
    logger.info("\n🚢 Processando Dimensão Via Transporte...")
    
    try:
        df_ref_via = spark.read.format("delta").load(f"{base_url_trusted}/via") \
            .select(
                F.col("codigo_via_transporte").alias("ref_co_via"),
                F.col("descricao_via_transporte").alias("nome_via")
            )
            
        df_vias_exp = spark.read.format("delta").load(f"{base_url_trusted}/exp") \
            .select(F.col("co_via").alias("codigo_via")).distinct()
            
        df_vias_imp = spark.read.format("delta").load(f"{base_url_trusted}/imp") \
            .select(F.col("codigo_via_transporte").alias("codigo_via")).distinct()
            
        df_vias_unicas = df_vias_exp.union(df_vias_imp).distinct().filter(F.col("codigo_via").isNotNull())
        
        df_dim_via = df_vias_unicas.join(df_ref_via, df_vias_unicas.codigo_via == df_ref_via.ref_co_via, "left") \
            .withColumn("sk_via_transporte", F.col("codigo_via")) \
            .withColumn("descricao_via", F.coalesce(F.col("nome_via"), F.lit("Não Informado"))) \
            .select("sk_via_transporte", "codigo_via", "descricao_via") \
            .orderBy("sk_via_transporte")
            
        df_dim_via = adicionar_metadados(df_dim_via)
        salvar_tabela_gold(df_dim_via, "dim_via_transporte")
        
    except Exception as e:
        logger.error(f"❌ Erro em dim_via_transporte: {e}")

    # ==================================================================================
    # 6. Tabela Fato (ft_balanco_comercial)
    # ==================================================================================
    logger.info("\n💰 Processando Fato Balanço Comercial...")
    
    try:
        # Exportação
        df_exp = spark.read.format("delta").load(f"{base_url_trusted}/exp") \
            .select(
                F.col("co_ncm").cast("bigint").alias("sk_ncm"),
                F.col("co_pais").cast("bigint").alias("sk_pais"),
                F.expr("ascii(substring(coalesce(sg_uf_ncm, 'XX'), 1, 1)) * 100 + ascii(substring(coalesce(sg_uf_ncm, 'XX'), 2, 1))").cast("bigint").alias("sk_uf"),
                F.col("co_via").cast("bigint").alias("sk_via_transporte"),
                F.expr("co_ano * 100 + co_mes").cast("bigint").alias("sk_data"),
                F.lit("EXPORTACAO").alias("tipo_movimentacao"),
                F.col("vl_fob").cast("decimal(18,2)").alias("valor_fob"),
                F.col("qt_estat").cast("decimal(18,2)").alias("quantidade"),
                F.col("kg_liquido").cast("decimal(18,2)").alias("kg_liquido")
            )
            
        # Importação
        df_imp = spark.read.format("delta").load(f"{base_url_trusted}/imp") \
            .select(
                F.col("codigo_ncm").cast("bigint").alias("sk_ncm"),
                F.col("codigo_pais_origem").cast("bigint").alias("sk_pais"),
                F.expr("ascii(substring(coalesce(uf_destino, 'XX'), 1, 1)) * 100 + ascii(substring(coalesce(uf_destino, 'XX'), 2, 1))").cast("bigint").alias("sk_uf"),
                F.col("codigo_via_transporte").cast("bigint").alias("sk_via_transporte"),
                F.expr("ano * 100 + mes").cast("bigint").alias("sk_data"),
                F.lit("IMPORTACAO").alias("tipo_movimentacao"),
                F.col("valor_fob_usd").cast("decimal(18,2)").alias("valor_fob"),
                F.col("quantidade_estatistica").cast("decimal(18,2)").alias("quantidade"),
                F.col("peso_liquido_kg").cast("decimal(18,2)").alias("kg_liquido")
            )
            
        df_ft_balanco = df_exp.union(df_imp) \
            .filter(F.col("sk_ncm").isNotNull()) \
            .filter(F.col("sk_data").isNotNull())
            
        # Métricas Calculadas
        df_ft_balanco = df_ft_balanco \
            .withColumn("valor_unitario", 
                        F.when(F.col("quantidade") > 0, F.round(F.col("valor_fob") / F.col("quantidade"), 4)).otherwise(F.lit(0))) \
            .withColumn("preco_kg", 
                        F.when(F.col("kg_liquido") > 0, F.round(F.col("valor_fob") / F.col("kg_liquido"), 4)).otherwise(F.lit(0))) \
            .withColumn("flag_exportacao", F.when(F.col("tipo_movimentacao") == "EXPORTACAO", 1).otherwise(0)) \
            .withColumn("flag_importacao", F.when(F.col("tipo_movimentacao") == "IMPORTACAO", 1).otherwise(0))
            
        df_ft_balanco = adicionar_metadados(df_ft_balanco)
        salvar_tabela_gold(df_ft_balanco, "ft_balanco_comercial", particionar_por=["tipo_movimentacao"])
        
    except Exception as e:
        logger.error(f"❌ Erro em ft_balanco_comercial: {e}")

    spark.stop()
    logger.info("🏁 Processamento Gold Finalizado.")

# COMMAND ----------

if __name__ == "__main__":
    processar_gold()
