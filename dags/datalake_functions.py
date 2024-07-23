import requests
import pandas as pd
import os
import re
from airflow.models import Variable

def criar_diretorio_se_nao_existir(diretorio):
    try:
        if not os.path.exists(diretorio):
            os.makedirs(diretorio)
            print(f"Diretório criado: {diretorio}")
    except Exception as e:
        print(f"Erro ao criar diretório {diretorio}: {e}")

def buscar_e_salvar_dados_brewery():
    try:
        print("----------------------------")
        print("Etapa: Iniciando busca e salvamento de dados")
        
        # Definir variáveis
        datalake_path = Variable.get("datalake_path", default_var="/opt/airflow/datalake")
        landing_zone_path = os.path.join(datalake_path, "landing_zone")

        print(f"Diretório do datalake: {datalake_path}")
        print(f"Diretório da landing zone: {landing_zone_path}")

        # Criar diretórios se não existirem
        criar_diretorio_se_nao_existir(datalake_path)
        criar_diretorio_se_nao_existir(landing_zone_path)

        # Buscar dados da API
        url_base = "https://api.openbrewerydb.org/breweries"
        todos_os_dados = []
        registros_por_pagina = 50

        print("Verificando o número total de registros")
        # Verificar o número total de registros
        resposta_inicial = requests.get(f'{url_base}?per_page=1')
        if resposta_inicial.status_code == 200:
            total_registros = resposta_inicial.json()
            max_paginas = None
        else:
            print(f"Erro ao buscar dados da API: {resposta_inicial.status_code}")
            return

        print("Iniciando busca paginada dos dados")
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

        print("Criando DataFrame a partir dos dados recuperados")
        # Criar DataFrame
        df = pd.DataFrame.from_dict(todos_os_dados)
        
        # Salvar JSON localmente na pasta 'landing_zone'
        local_json_path = os.path.join(landing_zone_path, 'brewery_data.json')
        df.to_json(local_json_path, orient='records', lines=True)
        print(f"Dados salvos localmente em: {local_json_path}")
        print("----------------------------")

    except Exception as e:
        print(f"Erro na função buscar_e_salvar_dados_brewery: {e}")

def transformar_dados_para_silver():
    try:
        print("----------------------------")
        print("Etapa: Transformação dos dados para Silver")
        
        # Definir variáveis
        datalake_path = Variable.get("datalake_path", default_var="/opt/airflow/datalake")
        landing_zone_path = os.path.join(datalake_path, "landing_zone")
        silver_zone_path = os.path.join(datalake_path, "silver")

        print(f"Diretório do datalake: {datalake_path}")
        print(f"Diretório da landing zone: {landing_zone_path}")
        print(f"Diretório da silver zone: {silver_zone_path}")

        # Criar diretórios se não existirem
        criar_diretorio_se_nao_existir(datalake_path)
        criar_diretorio_se_nao_existir(silver_zone_path)

        # Ler dados da landing zone
        local_json_path = os.path.join(landing_zone_path, 'brewery_data.json')
        df = pd.read_json(local_json_path, lines=True)
        print("Dados lidos da landing zone")

        # Renomear colunas
        print("Renomeando colunas")
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
        print("Aplicando transformações")
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
        print("Validando URLs")
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
        print("Ajustando URLs")
        df["site"] = df.groupby("nome")["site"].transform(lambda x: x.bfill().ffill())
        df["site"] = df.apply(
            lambda row: re.sub("^(http|https)://", "https://", row["site"]) if not row["website_url_validacao"] else row["site"],
            axis=1
        )

        # Salvar dados transformados na camada silver particionados por estado
        silver_parquet_path = os.path.join(silver_zone_path, 'brewery_data_silver.parquet')
        df.to_parquet(silver_parquet_path, index=False, partition_cols=["estado"])
        print(f"Dados transformados salvos em: {silver_parquet_path}")
        print("----------------------------")

    except Exception as e:
        print(f"Erro na função transformar_dados_para_silver: {e}")

def transformar_dados_para_gold():
    try:
        print("----------------------------")
        print("Etapa: Transformação dos dados para Gold")
        
        # Definir variáveis
        datalake_path = Variable.get("datalake_path", default_var="/opt/airflow/datalake")
        silver_zone_path = os.path.join(datalake_path, "silver")
        gold_zone_path = os.path.join(datalake_path, "gold")

        print(f"Diretório do datalake: {datalake_path}")
        print(f"Diretório da silver zone: {silver_zone_path}")
        print(f"Diretório da gold zone: {gold_zone_path}")

        # Criar diretórios se não existirem
        criar_diretorio_se_nao_existir(datalake_path)
        criar_diretorio_se_nao_existir(gold_zone_path)

        # Ler dados da silver zone
        silver_parquet_path = os.path.join(silver_zone_path, 'brewery_data_silver.parquet')
        df = pd.read_parquet(silver_parquet_path)
        print("Dados lidos da silver zone")

        # Exemplo de agregação: Contar cervejarias por tipo e localização
        print("Realizando agregação dos dados")
        df_gold = df.groupby(['estado', 'tipo_cervejaria']).size().reset_index(name='count')

        # Salvar dados transformados na camada gold
        gold_parquet_path = os.path.join(gold_zone_path, 'brewery_data_gold.parquet')
        df_gold.to_parquet(gold_parquet_path, index=False)
        print(f"Dados agregados salvos em: {gold_parquet_path}")
        print("----------------------------")

    except Exception as e:
        print(f"Erro na função transformar_dados_para_gold: {e}")