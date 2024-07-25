# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento camada gold
# MAGIC -
# MAGIC   Disponibilização do dado para datamarts BI

# COMMAND ----------

import os


# Criação do esquema 'gold' se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Lendo dados da camada Prata
df_silver = spark.table("silver.brewery_data")

# agregação: Contar cervejarias por tipo e localização
df_gold = df_silver.groupBy("estado", "tipo_cervejaria").count()

# Especificando o caminho para a tabela externa
external_table_path = f"{mount_point}/brewery_data"

# Escrevendo os dados no caminho especificado no Blob Storage
df_gold.write\
         .format("delta")\
            .mode("overwrite")\
                .partitionBy("estado", "tipo_cervejaria")\
                  .option("overwriteSchema", "true")\
                    .saveAsTable("gold.brewery_data")

