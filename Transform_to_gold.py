# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento camada gold
# MAGIC -
# MAGIC   Disponibilização do dado para datamarts BI

# COMMAND ----------

import os

# Supondo que você já tenha as credenciais no Azure Key Vault e configuradas no Databricks Secrets
storage_account_name = os.environ['storage_account_name']
container_name = "gold"
mount_point = "/mnt/gold"

# Montando o armazenamento
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = mount_point,
  extra_configs = {
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": dbutils.secrets.get(scope="my_scope", key="my_storage_key")
  }
  
# Define constantes e chaves de ambiente
storage_account_name = os.environ['storage_account_name']

# Criação do esquema 'gold' se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Ler dados da camada Prata
df_silver = spark.table("silver.brewery_data")

# Exemplo de agregação: Contar cervejarias por tipo e localização
# df_gold = df_silver.groupBy("estado", "tipo_cervejaria").count()

# Especificando o caminho para a tabela externa
external_table_path = f"{mount_point}/gold/brewery_data"

# Escrever os dados agregados na tabela gold
df_silver.write\
       .format("delta")\
       .mode("overwrite")\
       .option("path", external_table_path)\
       .saveAsTable("gold.brewery_data")


