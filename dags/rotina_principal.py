from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from airflow.providers.email.operators.email import EmailOperator
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

default_args = {
    'owner': 'airflow',
    'email_on_failure': True,
    'email': 'rodrigohenrique007@hotmail.com',
    'retries': 3,
}


