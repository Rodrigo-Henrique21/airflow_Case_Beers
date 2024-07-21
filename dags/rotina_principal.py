from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
import os

def fetch_and_upload_brewery_data():
    # Buscar variáveis do Airflow
    azure_storage_connection_string = Variable.get('azure_storage_connection_string')
    container_name = Variable.get('azure_blob_container_name')
    blob_path = Variable.get('azure_blob_path')

    # Buscar dados da API
    url_base = "https://api.openbrewerydb.org/breweries"
    todos_os_dados = []
    pagina = 1

    while True:
        resposta = requests.get(f'{url_base}?page={pagina}&per_page=50')
        if resposta.status_code == 200:
            dados = resposta.json()
            if not dados:
                break
            todos_os_dados.extend(dados)
            pagina += 1
        else:
            print("Erro ao buscar dados da API:", resposta.status_code)
            break

    if not todos_os_dados:
        print("Nenhum dado recuperado.")
        return None

    # Criar DataFrame
    df = pd.DataFrame.from_dict(todos_os_dados)
    
    # Criar diretório 'landing_zone' se não existir
    landing_zone_path = '/landing_zone'
    os.makedirs(landing_zone_path, exist_ok=True)
    
    # Salvar CSV localmente na pasta 'landing_zone'
    local_csv_path = os.path.join(landing_zone_path, 'brewery_data.csv')
    df.to_csv(local_csv_path, index=False)
    print(f"Dados salvos localmente em: {local_csv_path}")

    # Conectar ao Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    # Fazer upload do CSV
    with open(local_csv_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print("Dados da API foram salvos no Blob Storage com sucesso!")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'execute_databricks_notebooks',
    default_args=default_args,
    description='Executa notebooks no Databricks para processar dados da Open Brewery DB',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    run_fetch_and_upload_brewery_data = DatabricksRunNowOperator(
        task_id='run_load_bronze_data',
        databricks_conn_id='databricks_default',
        notebook_task={'notebook_path': '/path/to/load_bronze_data'}
    )
        

    run_load_bronze_data = DatabricksRunNowOperator(
        task_id='run_load_bronze_data',
        databricks_conn_id='databricks_default',
        notebook_task={'notebook_path': '/path/to/load_bronze_data'}
    )

    run_transform_to_silver = DatabricksRunNowOperator(
        task_id='run_transform_to_silver',
        databricks_conn_id='databricks_default',
        notebook_task={'notebook_path': '/path/to/transform_to_silver'}
    )

    run_create_gold_aggregation = DatabricksRunNowOperator(
        task_id='run_create_gold_aggregation',
        databricks_conn_id='databricks_default',
        notebook_task={'notebook_path': '/path/to/create_gold_aggregation'}
    )

    run_load_bronze_data >> run_transform_to_silver >> run_create_gold_aggregation