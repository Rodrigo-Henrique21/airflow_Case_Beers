# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento para camada silver
# MAGIC - Realização da limpeza incial dos dados
# MAGIC - Troca do nome da colunas
# MAGIC - Ajuste sites
# MAGIC - Criação do indicador de segurança
# MAGIC - Disponinilização

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os

# Criação do esquema 'silver' se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Lendo os dados da tabela bronze
df_bronze = spark.table("bronze.brewery_data")

# transformações necessárias
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
             .withColumn("cidade", F.regexp_replace(\
                 F.trim(\
                     F.lower(\
                         F.col("cidade")\
                             )\
                                 ),"[^a-zA-Z0-9.,&\\s]+", "")\
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
             .withColumn("estado", F.regexp_replace(\
                 F.trim(\
                     F.lower(\
                         F.col("estado")\
                             )\
                                 ),"[^a-zA-Z0-9.,&\\s]+", "")\
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
            })\
             .cache()

# Definindo uma função para validar o formato da URL
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

# Aplicando a validação e criar colunas de validação
df_silver = df_silver.withColumn("validao_site", valida_url_udf(F.col("site")))
df_silver = df_silver.withColumn("website_url_validacao", F.col("validao_site.valida_forma"))
df_silver = df_silver.withColumn("indicador_seguranca_site", F.col("validao_site.indicador_seguranca_site"))

# Definindo a especificação de janela para substituição de URLs nulas
window_spec = Window.partitionBy("nome").orderBy(F.col("site").desc())

# Substituindo URLs nulas
df_silver = df_silver.withColumn(
    "site",
    F.when(
        F.col("site").isNull(),
        F.first(F.col("site"), ignorenulls=True).over(window_spec)
    ).otherwise(F.col("site"))
)

# Ajustando URLs para garantir que estejam corretas
df_silver = df_silver.withColumn(
    "site",
    F.when(
        F.col("website_url_validacao") == False,
        F.regexp_replace(F.col("site"), "^(http|https)://", "https://")
    ).otherwise(F.col("site"))
)\
                    .cache()


# Salvando como tabela Delta na camada Prata, particionado por estado e cidade
df_silver.write\
         .format("delta")\
            .mode("overwrite")\
                .partitionBy("estado", "cidade")\
                    .saveAsTable("silver.brewery_data")
