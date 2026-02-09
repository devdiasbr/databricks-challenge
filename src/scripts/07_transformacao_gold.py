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
        spark.sql(f"OPTIMIZE delta.`{target_path}`")
        spark.sql(f"VACUUM delta.`{target_path}` RETAIN 168 HOURS")
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
    # Assumindo estrutura: trusted/{container}/{tabela}
    # O script 05 salvou em trusted/balancacomercial/{folder_name}
    account = config.TARGET_ACCOUNT
    base_url_trusted = f"wasbs://trusted@{account}.blob.core.windows.net/balancacomercial"
    
    # ==================================================================================
    # 1. Dimensão Data (dim_data)
    # ==================================================================================
    logger.info("\n📅 Processando Dimensão Data...")
    
    try:
        # Lê tabelas EXP e IMP (Trusted já tem nomes normalizados: ano, mes)
        df_exp = spark.read.format("delta").load(f"{base_url_trusted}/EXP_2021")
        df_imp = spark.read.format("delta").load(f"{base_url_trusted}/IMP_2021")
        
        df_datas_exp = df_exp.select("ano", "mes").distinct()
        df_datas_imp = df_imp.select("ano", "mes").distinct()
        
        df_datas = df_datas_exp.union(df_datas_imp).distinct()
        
        df_dim_data = df_datas \
            .withColumn("data", F.to_date(F.concat_ws("-", F.col("ano"), F.col("mes"), F.lit("01")))) \
            .withColumn("sk_data", F.expr("cast(ano as long) * 100 + cast(mes as long)")) \
            .withColumn("trimestre", F.ceil(F.col("mes") / 3).cast("int")) \
            .withColumn("semestre", F.ceil(F.col("mes") / 6).cast("int")) \
            .withColumn("nome_mes", 
                        F.when(F.col("mes") == 1, "Janeiro")
                         .when(F.col("mes") == 2, "Fevereiro")
                         .when(F.col("mes") == 3, "Março")
                         .when(F.col("mes") == 4, "Abril")
                         .when(F.col("mes") == 5, "Maio")
                         .when(F.col("mes") == 6, "Junho")
                         .when(F.col("mes") == 7, "Julho")
                         .when(F.col("mes") == 8, "Agosto")
                         .when(F.col("mes") == 9, "Setembro")
                         .when(F.col("mes") == 10, "Outubro")
                         .when(F.col("mes") == 11, "Novembro")
                         .when(F.col("mes") == 12, "Dezembro")
                         .otherwise("Desconhecido")) \
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
        # Trusted: NCM (codigo_ncm, descricao_ncm_pt)
        df_ncm_base = spark.read.format("delta").load(f"{base_url_trusted}/NCM") \
            .select(
                F.col("codigo_ncm").alias("no_cod"),
                F.col("descricao_ncm_pt").alias("descricao_ncm")
            ).dropDuplicates(["no_cod"])
            
        # Tentativa de carregar relacionamento CNAE (se existir em Trusted)
        # Se não existir, cria colunas vazias para manter o schema
        try:
            # Assumindo que pode ter sido ingerido com nome 'external_ncm_cnae' ou similar
            # Caso não exista, o bloco except será acionado
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

        # Transformações Finais NCM
        df_dim_ncm = df_dim_ncm \
            .withColumn("sk_ncm", F.expr("try_cast(no_cod as bigint)")) \
            .filter(F.col("sk_ncm").isNotNull()) \
            .withColumn("codigo_ncm", F.col("no_cod")) \
            .withColumn("descricao_ncm", F.coalesce(F.col("descricao_ncm"), F.lit("Não Informado"))) \
            .withColumn("cnae", F.coalesce(F.col("codigo_cnae"), F.lit("Não Informado"))) \
            .withColumn("descricao_cnae", F.coalesce(F.col("descricao_ncm_cnae"), F.lit("Não Informado"))) \
            .withColumn("setor_economico", 
                        F.when(F.col("sk_ncm").between(1000000, 9999999), "Agropecuária")
                         .when(F.col("sk_ncm").between(10000000, 24999999), "Indústria Extrativa")
                         .when(F.col("sk_ncm").between(25000000, 49999999), "Indústria de Transformação")
                         .when(F.col("sk_ncm").between(50000000, 99999999), "Outros")
                         .otherwise("Não Classificado")) \
            .select("sk_ncm", "codigo_ncm", "descricao_ncm", "cnae", "descricao_cnae", "setor_economico") \
            .dropDuplicates(["sk_ncm"]) \
            .orderBy("sk_ncm")

        df_dim_ncm = adicionar_metadados(df_dim_ncm)
        salvar_tabela_gold(df_dim_ncm, "dim_ncm")
        
    except Exception as e:
        logger.error(f"❌ Erro em dim_ncm: {e}")

    # ==================================================================================
    # 3. Dimensão Localidade (dim_localidade)
    # ==================================================================================
    logger.info("\n🌎 Processando Dimensão Localidade...")
    
    try:
        # Trusted: PAIS (codigo_pais, codigo_pais_iso3, nome_pais_pt)
        df_paises = spark.read.format("delta").load(f"{base_url_trusted}/PAIS") \
            .select(
                F.col("codigo_pais"),
                F.col("codigo_pais_iso3").alias("sigla_pais"),
                F.col("nome_pais_pt").alias("nome_pais")
            ).filter(F.col("codigo_pais").isNotNull())
            
        # UFs de Origem (EXP) e Destino (IMP)
        df_ufs_exp = spark.read.format("delta").load(f"{base_url_trusted}/EXP_2021") \
            .select(F.col("uf_origem").alias("uf")).distinct()
            
        df_ufs_imp = spark.read.format("delta").load(f"{base_url_trusted}/IMP_2021") \
            .select(F.col("uf_destino").alias("uf")).distinct()
            
        df_ufs = df_ufs_exp.union(df_ufs_imp).distinct().filter(F.col("uf").isNotNull())
        
        # Combinação País + UF
        df_paises_exp = spark.read.format("delta").load(f"{base_url_trusted}/EXP_2021") \
            .select(
                F.col("codigo_pais_destino").alias("codigo_pais"),
                F.col("uf_origem").alias("uf")
            ).distinct()
            
        df_paises_imp = spark.read.format("delta").load(f"{base_url_trusted}/IMP_2021") \
            .select(
                F.col("codigo_pais_origem").alias("codigo_pais"),
                F.col("uf_destino").alias("uf")
            ).distinct()
            
        df_localidades = df_paises_exp.union(df_paises_imp).distinct()
        
        # Blocos Econômicos (PAIS_BLOCO: codigo_pais, nome_bloco_pt)
        try:
            df_bloco_pais = spark.read.format("delta").load(f"{base_url_trusted}/PAIS_BLOCO") \
                .select(
                    F.col("codigo_pais").alias("codigo_pais_bloco"),
                    F.col("nome_bloco_pt").alias("regiao_pais")
                ).distinct()
        except Exception:
            logger.warning("⚠️ Tabela PAIS_BLOCO não encontrada. Seguindo sem bloco.")
            df_bloco_pais = None

        # Join Final
        df_dim_localidade = df_localidades.join(df_paises, "codigo_pais", "left")
        
        if df_bloco_pais:
            df_dim_localidade = df_dim_localidade.join(df_bloco_pais, df_paises.codigo_pais == df_bloco_pais.codigo_pais_bloco, "left")
        else:
            df_dim_localidade = df_dim_localidade.withColumn("regiao_pais", F.lit(None))

        df_dim_localidade = df_dim_localidade \
            .withColumn("sk_localidade", F.expr("cast(codigo_pais as long) * 1000 + ascii(coalesce(uf, 'XX'))")) \
            .withColumn("bloco_pais", F.coalesce(F.col("regiao_pais"), F.lit("Não informado"))) \
            .withColumn("regiao", 
                        F.when(F.col("uf").isin(["AC", "AP", "AM", "PA", "RO", "RR", "TO"]), "Norte")
                         .when(F.col("uf").isin(["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]), "Nordeste")
                         .when(F.col("uf").isin(["GO", "MT", "MS", "DF"]), "Centro-Oeste")
                         .when(F.col("uf").isin(["ES", "MG", "RJ", "SP"]), "Sudeste")
                         .when(F.col("uf").isin(["PR", "RS", "SC"]), "Sul")
                         .otherwise("Internacional")) \
            .select("sk_localidade", "bloco_pais", F.col("nome_pais").alias("pais"), F.coalesce(F.col("uf"), F.lit("XX")).alias("uf"), "regiao") \
            .dropDuplicates(["sk_localidade"]) \
            .orderBy("sk_localidade")
            
        df_dim_localidade = adicionar_metadados(df_dim_localidade)
        salvar_tabela_gold(df_dim_localidade, "dim_localidade")
        
    except Exception as e:
        logger.error(f"❌ Erro em dim_localidade: {e}")

    # ==================================================================================
    # 4. Dimensão Via Transporte (dim_via_transporte)
    # ==================================================================================
    logger.info("\n🚢 Processando Dimensão Via Transporte...")
    
    try:
        # Trusted: VIA (codigo_via_transporte, descricao_via_transporte)
        df_ref_via = spark.read.format("delta").load(f"{base_url_trusted}/VIA") \
            .select(
                F.col("codigo_via_transporte").alias("ref_co_via"),
                F.col("descricao_via_transporte").alias("nome_via")
            )
            
        df_vias_exp = spark.read.format("delta").load(f"{base_url_trusted}/EXP_2021") \
            .select(F.col("codigo_via_transporte").alias("codigo_via")).distinct()
            
        df_vias_imp = spark.read.format("delta").load(f"{base_url_trusted}/IMP_2021") \
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
    # 5. Tabela Fato (ft_balanco_comercial)
    # ==================================================================================
    logger.info("\n💰 Processando Fato Balanço Comercial...")
    
    try:
        # Exportação
        df_exp = spark.read.format("delta").load(f"{base_url_trusted}/EXP_2021") \
            .select(
                F.col("codigo_ncm").cast("bigint").alias("sk_ncm"),
                F.expr("cast(codigo_pais_destino as long) * 1000 + ascii(coalesce(uf_origem, 'XX'))").alias("sk_localidade"),
                F.col("codigo_via_transporte").cast("bigint").alias("sk_via_transporte"),
                F.expr("cast(ano as long) * 100 + cast(mes as long)").alias("sk_data"),
                F.lit("EXPORTACAO").alias("tipo_movimentacao"),
                F.col("valor_fob_usd").cast("decimal(18,2)").alias("valor_fob"),
                F.col("quantidade_estatistica").cast("decimal(18,2)").alias("quantidade"),
                F.col("peso_liquido_kg").cast("decimal(18,2)").alias("kg_liquido")
            )
            
        # Importação
        df_imp = spark.read.format("delta").load(f"{base_url_trusted}/IMP_2021") \
            .select(
                F.col("codigo_ncm").cast("bigint").alias("sk_ncm"),
                F.expr("cast(codigo_pais_origem as long) * 1000 + ascii(coalesce(uf_destino, 'XX'))").alias("sk_localidade"),
                F.col("codigo_via_transporte").cast("bigint").alias("sk_via_transporte"),
                F.expr("cast(ano as long) * 100 + cast(mes as long)").alias("sk_data"),
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
                        F.when(F.col("quantidade") > 0, F.col("valor_fob") / F.col("quantidade")).otherwise(F.lit(0))) \
            .withColumn("preco_kg", 
                        F.when(F.col("kg_liquido") > 0, F.col("valor_fob") / F.col("kg_liquido")).otherwise(F.lit(0))) \
            .withColumn("flag_exportacao", F.when(F.col("tipo_movimentacao") == "EXPORTACAO", 1).otherwise(0)) \
            .withColumn("flag_importacao", F.when(F.col("tipo_movimentacao") == "IMPORTACAO", 1).otherwise(0))
            
        df_ft_balanco = adicionar_metadados(df_ft_balanco)
        salvar_tabela_gold(df_ft_balanco, "ft_balanco_comercial", particionar_por=["sk_data"])
        
    except Exception as e:
        logger.error(f"❌ Erro em ft_balanco_comercial: {e}")

    spark.stop()
    logger.info("🏁 Processamento Gold Finalizado.")

if __name__ == "__main__":
    processar_gold()
