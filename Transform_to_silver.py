# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Define constantes e chaves de ambiente
storage_account_name = os.environ['storage_account_name']

# Envia ao spark
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", dbutils.secrets.get(scope="keyvault", key="keystorage"))

# Criar sessão Spark se não existir
if 'spark' not in locals():
    spark = SparkSession.builder.appName("workflow_bricks").getOrCreate()

# Criação do esquema 'silver' se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Ler os dados da tabela bronze
df_bronze = spark.table("bronze.brewery_data")

# Realizar as transformações necessárias
df_silver = \
    df_bronze.withColumnRenamed("id", "id")\
             .withColumnRenamed("name", "nome")\
             .withColumnRenamed("brewery_type", "tipo_cervejaria")\
             .withColumnRenamed("address_1", "endereco_principal")\
             .withColumnRenamed("address_2", "endereco_secundario")\
             .withColumnRenamed("address_3", "endereco_alternativo")\
             .withColumnRenamed("city", "cidade")\
             .withColumnRenamed("state_province", "provincia")\
             .withColumnRenamed("postal_code", "cep")\
             .withColumnRenamed("country", "pais")\
             .withColumnRenamed("longitude", "longitude")\
             .withColumnRenamed("latitude", "latitude")\
             .withColumnRenamed("phone", "telefone")\
             .withColumnRenamed("website_url", "site")\
             .withColumnRenamed("state", "estado")\
             .withColumnRenamed("street", "rua")\
             .dropDuplicates(["id"])\
                 

df_silver = \
    df_silver.dropDuplicates(["id"])\
             .withColumn("nome", F.regexp_replace(\
                 F.trim(\
                     F.lower(\
                         F.col("nome")\
                             )\
                                 ),"[^a-zA-Z0-9.,&\\s]+", "")\
                                     )\
             .withColumn("tipo_cervejaria", F.trim(\
                 F.lower(\
                     F.col("tipo_cervejaria")\
                         )\
                             )\
                                 )\
             .withColumn("cidade", F.trim(\
                 F.lower(\
                     F.col("cidade")\
                         )\
                             )\
                                 )\
             .withColumn("provincia", F.trim(\
                 F.lower(\
                     F.col("provincia")\
                         )\
                             )\
                                 )\
             .withColumn("pais", F.trim(\
                 F.lower(\
                     F.col("pais")\
                         )\
                             )\
                                 )\
             .withColumn("estado", F.trim(\
                 F.lower(\
                     F.col("estado")\
                         )\
                             )\
                                 )\
             .withColumn("rua", F.trim(\
                 F.lower(\
                     F.col("rua")\
                         )\
                             )\
                                 )\
             .fillna({
                'endereco_principal': '-',
                'endereco_secundario': '-',
                'endereco_alternativo': '-',
                'cidade': '-',
                'estado': '-',
                'site': '-'
            })
             

# Definir uma função para validar o formato da URL
def valida_url(url):
    if url is None:
        return False, False
    url = url.lower()
    indicador_seguranca_site = url.startswith('https://')
    valida_forma = (url.startswith('http://') or indicador_seguranca_site) and '.' in url.split('/')[2]
    return valida_forma, indicador_seguranca_site

# Registrando a função como uma UDF
url_validacao_schema = StructType([
    StructField("valida_forma", BooleanType(), nullable=False),
    StructField("indicador_seguranca_site", BooleanType(), nullable=False)
])

valida_url_udf = F.udf(valida_url, url_validacao_schema)

# Aplicar a validação e criar colunas de validação
df_silver = df_silver.withColumn("validao_site", valida_url_udf(F.col("site")))
df_silver = df_silver.withColumn("website_url_validacao", F.col("validao_site.valida_forma"))
df_silver = df_silver.withColumn("indicador_seguranca_site", F.col("validao_site.indicador_seguranca_site"))

# Definir a especificação de janela para substituição de URLs nulas
window_spec = Window.partitionBy("nome").orderBy(col("site").desc())

# Substituir URLs nulas
df_silver = df_silver.withColumn(
    "site",
    F.when(
        F.col("site").isNull(),
        F.first(F.col("site"), ignorenulls=True).over(window_spec)
    ).otherwise(F.col("site"))
)

# Ajustar URLs para garantir que estejam corretas
df_silver = df_silver.withColumn(
    "site",
    F.when(
        F.col("website_url_validacao") == False,
        F.regexp_replace(F.col("site"), "^(http|https)://", "https://")
    ).otherwise(F.col("site"))
)


# Salvar como tabela Delta na camada Prata, particionado por estado e cidade
df_silver.write\
         .format("delta")\
            .mode("overwrite")\
                .partitionBy("estado", "cidade")\
                    .saveAsTable("silver.brewery_data")

# Escrever dados transformados na camada Prata no Delta Lake
path = f"wasbs://silver@{storage_account_name}.blob.core.windows.net/silver_data"
df_silver.write.format("delta") \
                .mode("overwrite") \
                  .partitionBy("estado", "cidade") \
                      .save(path)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.brewery_data;
