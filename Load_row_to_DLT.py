# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession
import os

# Criar sessão Spark
spark = SparkSession.builder.appName("workflow_bricks").getOrCreate()

# Definir variáveis a partir das variáveis de ambiente
storage_account_name = os.environ['storage_account_name']
file_path = "brewery_data.csv"
mount_point = "/mnt/bronze_data"

# Verificar se o diretório já está montado
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    # Desmontar o Azure Blob Storage se já estiver montado
    dbutils.fs.unmount(mount_point)

# Montar o Azure Blob Storage
dbutils.fs.mount(
    source=f"wasbs://row@{storage_account_name}.blob.core.windows.net",
    mount_point=mount_point,
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": dbutils.secrets.get(scope="keyvault", key="keystorage")}
)

# Criação do esquema 'bronze' se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# Ler o arquivo CSV da camada Bronze
df = spark.read.option("header", "true").csv(f"{mount_point}/{file_path}")

# Salvar como uma tabela Delta
df.write.format("delta").mode("overwrite").saveAsTable("bronze.brewery_data")

# Desmontar o Azure Blob Storage
dbutils.fs.unmount(mount_point)

