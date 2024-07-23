import os
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
import json

# def fetch_and_upload_brewery_data():
#     # Buscar variáveis do Airflow
#     azure_storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=datelakemedallionlayer;AccountKey=ZvZJzSqezvnJVJkRx+Jzg4AQV60lJsMy5JY/2kIRFsSeXmBLsVINabiiT2ZCUFN7bYGfX/Bjzpej+AStegc/jg==;EndpointSuffix=core.windows.net"
#     container_name = "row"
#     blob_path = "brewery_data.json"

#     # Buscar dados da API
#     url_base = "https://api.openbrewerydb.org/breweries"
#     todos_os_dados = []
#     registros_por_pagina = 50

#     # Verificar o número total de registros
#     resposta_inicial = requests.get(f'{url_base}?per_page=1')
#     if resposta_inicial.status_code == 200:
#         total_registros = resposta_inicial.json()
#         if 'Total-Count' in resposta_inicial.headers:
#             total_registros = int(resposta_inicial.headers['Total-Count'])
#             max_paginas = (total_registros // registros_por_pagina) + (1 if total_registros % registros_por_pagina > 0 else 0)
#         else:
#             # Método alternativo: Continuar buscando páginas até que uma página vazia seja retornada
#             max_paginas = None
#     else:
#         print("Erro ao buscar dados da API:", resposta_inicial.status_code)
#         return

#     pagina = 1
#     while True:
#         resposta = requests.get(f'{url_base}?page={pagina}&per_page={registros_por_pagina}')
#         if resposta.status_code == 200:
#             dados = resposta.json()
#             if not dados:
#                 break
#             todos_os_dados.extend(dados)
#             pagina += 1
#             if max_paginas and pagina > max_paginas:
#                 break
#         else:
#             print(f"Erro ao buscar dados da API na página {pagina}: {resposta.status_code}")
#             break

#     if not todos_os_dados:
#         print("Nenhum dado recuperado.")
#         return None

#     # Criar DataFrame
#     df = pd.DataFrame.from_dict(todos_os_dados)
    
#     # Criar diretório 'landing_zone' se não existir
#     landing_zone_path = r"C:\Users\Murillo Correa\airflow_Case_Beers\landing_zone"
#     os.makedirs(landing_zone_path, exist_ok=True)
    
#     # Salvar JSON localmente na pasta 'landing_zone'
#     local_json_path = os.path.join(landing_zone_path, 'brewery_data.json')
#     df.to_json(local_json_path, orient='records', lines=True)
#     print(f"Dados salvos localmente em: {local_json_path}")

#     # Conectar ao Azure Blob Storage
#     blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
#     blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

#     # Fazer upload do JSON
#     with open(local_json_path, "rb") as data:
#         blob_client.upload_blob(data, overwrite=True)

#     print("Dados da API foram salvos no Blob Storage com sucesso!")

# fetch_and_upload_brewery_data()


# Configurar variáveis de autenticação e URL do Databricks
databricks_instance = "adb-844462385794132.12.azuredatabricks.net"
token = "dapicd6b1aa85deaf7bc282c6aa47e2b8cab-3"
job_id= "968731268030929"

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
}

base_url = f'https://{databricks_instance}/api/2.0/jobs/run-now'

def run_job(job_id):
    payload = {
        "job_id": job_id
    }
    response = requests.post(base_url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        print(f'Job com job_id {job_id} executado com sucesso.')
    else:
        print(f'Erro ao executar job com job_id {job_id}: {response.text}')

# Executar o job
run_job(job_id)
