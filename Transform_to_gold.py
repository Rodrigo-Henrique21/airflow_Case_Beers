# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession
import os

# Define constantes e chaves de ambiente
storage_account_name = os.environ['storage_account_name']
container_name = "gold"
mount_point = "/mnt/gold_data"

# Verificar se o diretório já está montado
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    # Montar o Azure Blob Storage
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
        mount_point=mount_point,
        extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": dbutils.secrets.get(scope="keyvault", key="keystorage")}
    )

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

