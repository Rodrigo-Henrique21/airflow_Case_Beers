# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession
import os

# Define constantes e chaves de ambiente
storage_account_name = os.environ['storage_account_name']

# Criar sessão Spark se não existir
if 'spark' not in locals():
    spark = SparkSession.builder.appName("workflow_bricks").getOrCreate()

# Criação do esquema 'gold' se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Ler dados da camada Prata
df_silver = spark.table("silver.brewery_data")

# Exemplo de agregação: Contar cervejarias por tipo e localização
df_gold = df_silver.groupBy("estado", "tipo_cervejaria").count()

# Escrever os dados agregados na tabela gold
df_gold.write\
       .format("delta")\
            .mode("overwrite")\
                .saveAsTable("gold.brewery_data")

# Escrever dados transformados na camada Prata no Delta Lake
path = f"wasbs://gold@{storage_account_name}.blob.core.windows.net/silver_data"
df_silver.write.format("delta") \
                .mode("overwrite") \
                  .partitionBy("estado", "cidade") \
                      .save(path)

