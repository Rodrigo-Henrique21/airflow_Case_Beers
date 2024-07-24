<img src="./references/img/Bev.png" alt="header" style="width: 850px;"/>

# Sobre readme

<div style="font-size: 14px;">

    Este readme contém a estrutura básica do projeto, incluindo instruções para clonar o repositório, fazer atualizações e configurar as informações de ambiente.

</div>

## Clonando o Projeto 📝

Para começar a trabalhar com o projeto, siga estas etapas:

1. Abra o terminal.
2. Navegue para a pasta onde você deseja clonar o projeto.
3. Execute o seguinte comando para clonar o repositório:

```bash
git clone https://github.com/Rodrigo-Henrique21/airflow_Case_Beers.git
```

## Entrar na pasta do repositório

Para mover-se até o seu repositório, você pode executar o comando:

```bash
cd airflow_Case_Beers/
```

## Atualizando o Repositório 📂

Para manter o seu repositório local atualizado com as últimas alterações, você pode executar o comando:


```bash
git pull origin master
```

## Observação 🔍

As variavéis de ambiente globais disponibilizadas pelo ambiente são acrescentadas no airflow web.

# Configuração de Ambiente ✏️ 

### Variavéis globais a serem preenchidas no Airflow  🖥️

A airflow web disponibilizara variavéis de ambiente padrão em produção, conforme abaixo:

```
não há necessidade de preenchimento das variaveis
```

# Atenção ‼️

## Instação de dependências ⚒️

1. - Docker: Certifique-se de ter o Docker instalado em sua máquina.🧱
    1. - Abra o terminal (cmd) e execute o comando.
    2. - Certifique de estar na pasta do projeto airflow_Case_Beers.
    3. - Execute o comando para subir o container contendo o airflow e as respectivas dependencias e pacotes do projeto:
    ```
    docker-compose up airflow-init
    ```
    4. - Após a finalização do comando anterior, inicie e execute os serviços definidos do docker-compose.yml:
    ```
    docker-compose up
    ```

2. - Editor de código (IDE): Qualquer editor de código para visualização.

## Estrutura do Projeto 🧩

A estrutura do projeto é a seguinte:

```
|-- airflow_Case_Beers
    |-- config
        |-- __pycache__
    |-- dags
        |-- rotina_principal.py
    |-- docs
    |-- datalake
        |-- landing_zone
            |-- brewery_data.json
        |-- silver
        |-- gold
    |-- logs
        |-- dag_id=execute_databricks_workflow
        |-- dag_id=simple_dag
        |-- dag_processor_manager
        |-- scheduler
    |-- plugins
        |-- .gitkeep
    |-- references
        |-- Bev.png
    |-- teste
    |-- venv
        |-- Include
        |-- Lib
        |-- Scripts
        |-- pyvenv.cfg
    |-- docker-compose.yml
    |-- DockerFile
    |-- README.md
```  

### Legenda 🧩🧩

| Diretório               | Descrição                                          |
|-------------------------|----------------------------------------------------|
| config                  | Diretório de configuração do projeto.              |
| dags                    | Diretório para armazenar os DAGs do Airflow.       |
| docs                    | Diretório para documentação do projeto.            |
| datalake                | Diretório para armazenar dados.                    |
| logs                    | Diretório para armazenar logs de execução.         |
| plugins                 | Diretório para plugins do Airflow..                |
| references              | Diretório para referências e materiais auxiliares. |
| venv                    | Diretório do ambiente virtual Python.              |
| docker-compose          | Arquivo de configuração do Docker Compose.         |
| DockerFile              | Arquivo de configuração do Docker.                 |
|                         |                                                    |


## Ficou com Dúvidas ❓

Confira a [documentação](airflow_Case_Beers/docs/Documentação.pdf) na raiz do projeto


