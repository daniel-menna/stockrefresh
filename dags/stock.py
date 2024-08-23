from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from datetime import datetime
import sys

@dag(
    start_date=datetime(2024, 8, 10),
    schedule=None,
    catchup=False,
    tags=['stock'],
)
def stock_dag():
    bucket_name = 'stockprice-bucket'
    commodities = ['AAPL', 'GOOGL', 'AMZN']  # Defina os símbolos desejados aqui

    @task.external_python(python='/usr/local/airflow/pandas_venv/bin/python')
    def install_yfinance():
        """
        Função para instalar o pacote yfinance no ambiente virtual atual.
        """
        import subprocess  # Certifique-se de que o subprocess seja importado aqui
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance"])

    @task.external_python(python='/usr/local/airflow/pandas_venv/bin/python')
    def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
        import yfinance as yf
        ticker = yf.Ticker(simbolo)
        dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
        dados['simbolo'] = simbolo
        return dados
    
    @task()
    def concatena_dados(todos_dados):
        import pandas as pd
        return pd.concat(todos_dados)

    @task()
    def salvar_dados_csv(dados_concatenados):
        import pandas as pd
        new_file_path = 'include/datasets/stocks.csv'
        # dados_concatenados['Date'] = pd.to_datetime(dados_concatenados['Date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        dados_concatenados.to_csv(new_file_path, index=True)

    upload_stockprice_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_stockprice_csv_to_gcs',
        src='include/datasets/stocks.csv',
        dst='raw/stocks.csv',
        bucket=bucket_name,
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_stock_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_stock_dataset',
        dataset_id='stock',
        gcp_conn_id='gcp',
    )

    retail_gcs_to_raw = aql.load_file(
        task_id='stock_gcs_to_raw',
        input_file=File(
            f'gs://{bucket_name}/raw/stocks.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_stocks',
            conn_id='gcp',
            metadata=Metadata(schema='stock')
        ),
        use_native_support=True,
        native_support_kwargs={
            "encoding": "ISO_8859_1",
        }
    )

    # Instalar yfinance
    install_yfinance_task = install_yfinance()

    # Expandir a tarefa para buscar os dados de todas as commodities
    dados_commodities = buscar_dados_commodities.expand(simbolo=commodities)

    # Assegurar que buscar_dados_commodities seja executada após a instalação do yfinance
    install_yfinance_task >> dados_commodities

    # Concatenar os resultados
    dados_concatenados = concatena_dados(dados_commodities)
    
    # Salvar os dados no CSV
    salvar_dados_csv_task = salvar_dados_csv(dados_concatenados)

    # upload_stockprice_csv_to_gcs seja executada após salvar_dados_csv_task
    salvar_dados_csv_task >> upload_stockprice_csv_to_gcs

    # Create stock dataset
    create_stock_dataset >> retail_gcs_to_raw

    # Insert raw data in the dataset
    upload_stockprice_csv_to_gcs >> retail_gcs_to_raw

stock_dag()