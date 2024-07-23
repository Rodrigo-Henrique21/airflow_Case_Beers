# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento camada gold
# MAGIC -
# MAGIC   Disponibilização do dado para datamarts BI

# COMMAND ----------

import os

# Credenciais no Azure Key Vault e configuradas no Databricks Secrets
storage_account_name = os.environ['storage_account_name']
container_name = "gold"
mount_point = "/mnt/gold"

# Montando o armazenamento
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
        mount_point = mount_point,
        extra_configs = {
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": dbutils.secrets.get(scope="keyvault", key="keystorage")
        }
    )

# Define constantes e chaves de ambiente
storage_account_name = os.environ['storage_account_name']

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
                .partitionBy("estado", "cidade")\
                    .saveAsTable("gold.brewery_data")

