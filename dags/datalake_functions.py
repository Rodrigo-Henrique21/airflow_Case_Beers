import requests
import pandas as pd
import os
import re
from airflow.models import Variable

def buscar_e_salvar_dados_brewery():
    # Definir variáveis
    datalake_path = Variable.get("datalake_path", default_var="/opt/airflow/datalake")
    landing_zone_path = os.path.join(datalake_path, "landing_zone")
    os.makedirs(landing_zone_path, exist_ok=True)

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

def transformar_dados_para_silver():
    # Definir variáveis
    datalake_path = Variable.get("datalake_path", default_var="/opt/airflow/datalake")
    landing_zone_path = os.path.join(datalake_path, "landing_zone")
    silver_zone_path = os.path.join(datalake_path, "silver")
    os.makedirs(silver_zone_path, exist_ok=True)

    # Ler dados da landing zone
    local_json_path = os.path.join(landing_zone_path, 'brewery_data.json')
    df = pd.read_json(local_json_path, lines=True)

    # Renomear colunas
    df = df.rename(columns={
        "id": "id",
        "name": "nome",
        "brewery_type": "tipo_cervejaria",
        "address_1": "endereco_principal",
        "address_2": "endereco_secundario",
        "address_3": "endereco_alternativo",
        "city": "cidade",
        "state_province": "provincia",
        "postal_code": "cep",
        "country": "pais",
        "longitude": "longitude",
        "latitude": "latitude",
        "phone": "telefone",
        "website_url": "site",
        "state": "estado",
        "street": "rua"
    }).drop_duplicates(subset=["id"])

    # Aplicar transformações
    df["nome"] = df["nome"].str.lower().str.strip().str.replace("[^a-zA-Z0-9.,&\\s]+", "", regex=True)
    df["tipo_cervejaria"] = df["tipo_cervejaria"].str.lower().str.strip()
    df["cidade"] = df["cidade"].str.lower().str.strip().str.replace("[^a-zA-Z0-9.,&\\s]+", "", regex=True)
    df["provincia"] = df["provincia"].str.lower().str.strip()
    df["pais"] = df["pais"].str.lower().str.strip()
    df["estado"] = df["estado"].str.lower().str.strip().str.replace("[^a-zA-Z0-9.,&\\s]+", "", regex=True)
    df["rua"] = df["rua"].str.lower().str.strip()
    
    df = df.fillna({
        'endereco_principal': '-',
        'endereco_secundario': '-',
        'endereco_alternativo': '-',
        'cidade': '-',
        'estado': '-',
        'site': '-'
    })

    # Validar URLs
    def valida_url(url):
        if pd.isna(url):
            return False, False
        url = url.lower()
        indicador_seguranca_site = url.startswith('https://')
        valida_forma = (url.startswith('http://') or indicador_seguranca_site) and '.' in url.split('/')[2]
        return valida_forma, indicador_seguranca_site

    url_validacao = df["site"].apply(lambda x: valida_url(x))
    df["website_url_validacao"] = url_validacao.apply(lambda x: x[0])
    df["indicador_seguranca_site"] = url_validacao.apply(lambda x: x[1])

    # Substituir URLs nulas e ajustar URLs incorretas
    df["site"] = df.groupby("nome")["site"].transform(lambda x: x.bfill().ffill())
    df["site"] = df.apply(
        lambda row: re.sub("^(http|https)://", "https://", row["site"]) if not row["website_url_validacao"] else row["site"],
        axis=1
    )

    # Salvar dados transformados na camada silver particionados por cidade e estado
    silver_parquet_path = os.path.join(silver_zone_path, 'brewery_data_silver.parquet')
    df.to_parquet(silver_parquet_path, index=False, partition_cols=["estado", "cidade"])
    print(f"Dados transformados salvos em: {silver_parquet_path}")

def transformar_dados_para_gold():
    # Definir variáveis
    datalake_path = Variable.get("datalake_path", default_var="/opt/airflow/datalake")
    silver_zone_path = os.path.join(datalake_path, "silver")
    gold_zone_path = os.path.join(datalake_path, "gold")
    os.makedirs(gold_zone_path, exist_ok=True)

    # Ler dados da silver zone
    silver_parquet_path = os.path.join(silver_zone_path, 'brewery_data_silver.parquet')
    df = pd.read_parquet(silver_parquet_path)

    # Exemplo de agregação: Contar cervejarias por tipo e localização
    df_gold = df.groupby(['estado', 'tipo_cervejaria']).size().reset_index(name='count')

    # Salvar dados transformados na camada gold
    gold_parquet_path = os.path.join(gold_zone_path, 'brewery_data_gold.parquet')
    df_gold.to_parquet(gold_parquet_path, index=False)
    print(f"Dados agregados salvos em: {gold_parquet_path}")
