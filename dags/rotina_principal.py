from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
import os

def fetch_and_upload_brewery_data():
    # Buscar variáveis do Airflow
    azure_storage_connection_string = Variable.get("azure_storage_connection_string")
    container_name = "row"
    blob_path = "brewery_data.json"

    # Buscar dados da API
    url_base = "https://api.openbrewerydb.org/breweries"
    todos_os_dados = []
    registros_por_pagina = 50

    # Verificar o número total de registros
    resposta_inicial = requests.get(f'{url_base}?per_page=1')
    if resposta_inicial.status_code == 200:
        total_registros = resposta_inicial.json()
        if 'Total-Count' in resposta_inicial.headers:
            total_registros = int(resposta_inicial.headers['Total-Count'])
            max_paginas = (total_registros // registros_por_pagina) + (1 if total_registros % registros_por_pagina > 0 else 0)
        else:
            # Método alternativo: Continuar buscando páginas até que uma página vazia seja retornada
            max_paginas = None
    else:
        print("Erro ao buscar dados da API:", resposta_inicial.status_code)
        return

    pagina = 1
    while True:
        resposta = requests.get(f'{url_base}?page={pagina}&per_page={registros_por_pagina}')
        if resposta.status_code == 200:
            dados = resposta.json()
            if not dados:
                break
            todos_os_dados.extend(dados)
            pagina += 1
            if max_paginas and pagina > max_paginas:
                break
        else:
            print(f"Erro ao buscar dados da API na página {pagina}: {resposta.status_code}")
            break

    if not todos_os_dados:
        print("Nenhum dado recuperado.")
        return None

    # Criar DataFrame
    df = pd.DataFrame.from_dict(todos_os_dados)
    
    # Criar diretório 'landing_zone' se não existir
    landing_zone_path = r"C:\Users\Murillo Correa\airflow_Case_Beers\landing_zone"
    os.makedirs(landing_zone_path, exist_ok=True)
    
    # Salvar JSON localmente na pasta 'landing_zone'
    local_json_path = os.path.join(landing_zone_path, 'brewery_data.json')
    df.to_json(local_json_path, orient='records', lines=True)
    print(f"Dados salvos localmente em: {local_json_path}")

    # Conectar ao Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    # Fazer upload do JSON
    with open(local_json_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print("Dados da API foram salvos no Blob Storage com sucesso!")


def run_databricks_job():
    # Buscar variáveis do Airflow
    databricks_instance = Variable.get("databricks_instance")
    token = Variable.get("databricks_token")
    job_id = Variable.get("databricks_job_id")

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    base_url = f'https://{databricks_instance}/api/2.0/jobs/run-now'
    payload = {
        "job_id": job_id
    }

    response = requests.post(base_url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        print(f'Job com job_id {job_id} executado com sucesso.')
    else:
        print(f'Erro ao executar job com job_id {job_id}: {response.text}')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'execute_databricks_workflow',
    default_args=default_args,
    description='Executa um workflow no Databricks para processar dados da Open Brewery DB',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    fetch_and_upload_brewery_data_task = PythonOperator(
        task_id='fetch_and_upload_brewery_data',
        python_callable=fetch_and_upload_brewery_data
    )
    
    run_databricks_workflow_task = PythonOperator(
        task_id='run_databricks_workflow',
        python_callable=run_databricks_job
    )

    fetch_and_upload_brewery_data_task >> run_databricks_workflow_task