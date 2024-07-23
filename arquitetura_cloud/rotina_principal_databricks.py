from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import timedelta
import requests
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
import os

# Definir variáveis de ambiente para Databricks
DATABRICKS_INSTANCE = Variable.get("databricks_instance")
DATABRICKS_TOKEN = Variable.get("databricks_token")

# Nome do workflow que queremos verificar ou criar
WORKFLOW_NAME = "Pipilene_api_ambev"

# URL base da API REST do Databricks
BASE_URL = f"https://{DATABRICKS_INSTANCE}/api/2.0"

# Função para verificar se um workflow existe
def verificar_workflow(workflow_name):
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    response = requests.get(f"{BASE_URL}/jobs/list", headers=headers)
    if response.status_code == 200:
        jobs = response.json().get("jobs", [])
        for job in jobs:
            if job["settings"]["name"] == workflow_name:
                return True
    return False

# Função para criar um workflow
def criar_workflow():
    email_success = Variable.get("email_failure")
    email_failure = Variable.get("email_failure")
    git_url = Variable.get("git_url")
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "name": WORKFLOW_NAME,
        "email_notifications": {
            "on_success": [email_success],
            "on_failure": [email_failure],
            "no_alert_for_skipped_runs": False
        },
        "webhook_notifications": {},
        "notification_settings": {
            "no_alert_for_skipped_runs": False,
            "no_alert_for_canceled_runs": False
        },
        "timeout_seconds": 0,
        "health": {
            "rules": [
                {
                    "metric": "RUN_DURATION_SECONDS",
                    "op": "GREATER_THAN",
                    "value": 1800
                }
            ]
        },
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "Load_Bronze_Data",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "Load_row_to_DLT",
                    "source": "GIT"
                },
                "existing_cluster_id": "0720-180306-v8yifq82",
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                },
                "webhook_notifications": {}
            },
            {
                "task_key": "Treatment_to_silver",
                "depends_on": [
                    {
                        "task_key": "Load_Bronze_Data"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "Transform_to_silver",
                    "source": "GIT"
                },
                "existing_cluster_id": "0720-180306-v8yifq82",
                "max_retries": 1,
                "min_retry_interval_millis": 3600000,
                "retry_on_timeout": True,
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                },
                "webhook_notifications": {}
            },
            {
                "task_key": "Treatment_to_gold",
                "depends_on": [
                    {
                        "task_key": "Treatment_to_silver"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "Transform_to_gold",
                    "source": "GIT"
                },
                "existing_cluster_id": "0720-180306-v8yifq82",
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                },
                "webhook_notifications": {}
            }
        ],
        "git_source": {
            "git_url": git_url,
            "git_provider": "gitHub",
            "git_branch": "Databricks_dev"
        },
        "format": "MULTI_TASK",
        "queue": {
            "enabled": True
        }
    }
    
    response = requests.post(f"{BASE_URL}/jobs/create", headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print(f"Workflow '{WORKFLOW_NAME}' criado com sucesso.")
    else:
        print(f"Erro ao criar workflow: {response.json()}")


def criar_diretorio_se_nao_existir(diretorio):
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)


def buscar_e_fazer_upload_dados_brewery():
    # Definir variáveis
    datalake_path = Variable.get("datalake_path", default_var="/opt/airflow/datalake")
    landing_zone_path = os.path.join(datalake_path, "landing_zone")

    # Criar diretórios se não existirem
    criar_diretorio_se_nao_existir(datalake_path)
    criar_diretorio_se_nao_existir(landing_zone_path)

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
    
    # Salvar JSON localmente na pasta 'landing_zone'
    local_json_path = os.path.join(landing_zone_path, 'brewery_data.json')
    df.to_json(local_json_path, orient='records', lines=True)
    print(f"Dados salvos localmente em: {local_json_path}")


# Função para executar um job no Databricks
def executar_job_databricks():
    databricks_instance = "adb-844462385794132.12.azuredatabricks.net"
    token = "dapicd6b1aa85deaf7bc282c6aa47e2b8cab-3"
    job_id = "968731268030929"

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

# Função para enviar email em caso de falha
# def enviar_email_falha(context):
#     email_to = Variable.get("email_failure")
#     subject = f"Airflow Task Failed: {context['task_instance_key_str']}"
#     html_content = f"""
#     <p>Task: {context['task_instance_key_str']} failed</p>
#     <p>Dag: {context['dag'].dag_id}</p>
#     <p>Execution Time: {context['execution_date']}</p>
#     <p>Log URL: {context['task_instance'].log_url}</p>
#     """
#     send_email(email_to, subject, html_content)

# Configurações padrão para o DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    # 'on_failure_callback': enviar_email_falha,
    'retry_delay': timedelta(minutes=120),
}

with DAG(
    'executar_workflow_databricks',
    default_args=default_args,
    description='Executa um workflow no Databricks para processar dados da Open Brewery DB',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(
        task_id='start'
    )
    
    buscar_e_fazer_upload_dados_brewery_task = PythonOperator(
        task_id='buscar_e_fazer_upload_dados_brewery',
        python_callable=buscar_e_fazer_upload_dados_brewery
    )
    
    verificar_e_criar_workflow_task = PythonOperator(
        task_id='verificar_e_criar_workflow',
        python_callable=lambda: criar_workflow() if not verificar_workflow(WORKFLOW_NAME) else print(f"Workflow '{WORKFLOW_NAME}' já existe.")
    )
    
    executar_workflow_databricks_task = PythonOperator(
        task_id='executar_workflow_databricks',
        python_callable=executar_job_databricks
    )
    
    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> buscar_e_fazer_upload_dados_brewery_task >> verificar_e_criar_workflow_task >> executar_workflow_databricks_task >> end_task
