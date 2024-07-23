from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.email import send_email_smtp
from datalake_functions import buscar_e_salvar_dados_brewery, transformar_dados_para_silver, transformar_dados_para_gold
from datetime import timedelta

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
#     send_email_smtp(email_to, subject, html_content)

# Configurações padrão para o DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=120),
    # 'on_failure_callback': enviar_email_falha,
}

with DAG(
    'processar_dados_brewery',
    default_args=default_args,
    description='Processa dados da Open Brewery DB e salva em um datalake local',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(
        task_id='start'
    )
    
    # Adicionando o sensor para verificar a disponibilidade da API
    api_available_sensor = HttpSensor(
        task_id='api_available_sensor',
        http_conn_id='open_brewery_db_api',
        endpoint='breweries',
        request_params={'per_page': 1},
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20
    )
    
    buscar_e_salvar_dados_brewery_task = PythonOperator(
        task_id='buscar_e_salvar_dados_brewery',
        python_callable=buscar_e_salvar_dados_brewery
    )
    
    transformar_dados_para_silver_task = PythonOperator(
        task_id='transformar_dados_para_silver',
        python_callable=transformar_dados_para_silver
    )
    
    transformar_dados_para_gold_task = PythonOperator(
        task_id='transformar_dados_para_gold',
        python_callable=transformar_dados_para_gold
    )
    
    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> api_available_sensor >> buscar_e_salvar_dados_brewery_task >> transformar_dados_para_silver_task >> transformar_dados_para_gold_task >> end_task
