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
    commodities = ['AAPL', 'GOOGL', 'AMZN', 'META', 'MSFT', 'NVDA', 'DELL', 'SAP', 'CRM', 'ORCL', 'IBM']

    @task.external_python(python='/usr/local/airflow/pandas_venv/bin/python')
    def install_yfinance():
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance"])

    @task.external_python(python='/usr/local/airflow/pandas_venv/bin/python')
    def buscar_dados_commodities(simbolo, periodo='1y', intervalo='1d'):
        import yfinance as yf
        ticker = yf.Ticker(simbolo)
        dados = ticker.history(period=periodo, interval=intervalo)[['Open','Close']]
        campos_infos = ['city', 'state', 'zip', 'country', 'industry', 'enterpriseValue']
        infos = {campo: ticker.info.get(campo, None) for campo in campos_infos}
        dados['simbolo'] = simbolo
        infos['simbolo'] = simbolo
        return dados, infos
    
    @task()
    def concatena_dados_preco(todos_dados):
        import pandas as pd
        dados_preco = [dados for dados, _ in todos_dados]
        return pd.concat(dados_preco)

    @task()
    def concatena_dados_infos(todos_dados):
        import pandas as pd
        dados_infos = [infos for _, infos in todos_dados]
        return pd.DataFrame(dados_infos)

    @task()
    def salvar_dados_csv(dados_concatenados, file_path):
        if file_path == 'include/datasets/stocks_info.csv' :
            dados_concatenados.to_csv(file_path, index=False)
        else:
            dados_concatenados.to_csv(file_path, index=True)

    upload_stockprice_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_stockprice_csv_to_gcs',
        src='include/datasets/stocks.csv',
        dst='raw/stocks.csv',
        bucket=bucket_name,
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    upload_stockinfo_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_stockinfo_csv_to_gcs',
        src='include/datasets/stocks_info.csv',
        dst='raw/stocks_info.csv',
        bucket=bucket_name,
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_stock_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_stock_dataset',
        dataset_id='stock',
        gcp_conn_id='gcp',
    )

    stocks_gcs_to_raw = aql.load_file(
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

    companies_gcs_to_raw = aql.load_file(
        task_id='companies_gcs_to_raw',
        input_file=File(
            f'gs://{bucket_name}/raw/stocks_info.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_companies',
            conn_id='gcp',
            metadata=Metadata(schema='stockprice-433416')
        ),
        use_native_support=True,
        native_support_kwargs={
            "encoding": "ISO_8859_1",
        }
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    # Instalar yfinance
    install_yfinance_task = install_yfinance()

    # Expandir a tarefa para buscar os dados de todas as commodities
    dados_commodities = buscar_dados_commodities.expand(simbolo=commodities)

    # Definir dependências
    install_yfinance_task >> dados_commodities

    # Concatenar os resultados
    dados_concatenados_preco = concatena_dados_preco(dados_commodities)
    dados_concatenados_infos = concatena_dados_infos(dados_commodities)
    
    # Salvar os dados no CSV
    salvar_dados_csv_task = salvar_dados_csv(dados_concatenados_preco, 'include/datasets/stocks.csv')
    salvar_infos_csv_task = salvar_dados_csv(dados_concatenados_infos, 'include/datasets/stocks_info.csv')

    # Definir dependências para upload dos arquivos CSV para o GCS
    salvar_dados_csv_task >> upload_stockprice_csv_to_gcs
    salvar_infos_csv_task >> upload_stockinfo_csv_to_gcs

    # Cria o dataset no BigQuery e insere os dados
    create_stock_dataset >> [stocks_gcs_to_raw, companies_gcs_to_raw]
    upload_stockprice_csv_to_gcs >> stocks_gcs_to_raw
    upload_stockinfo_csv_to_gcs >> companies_gcs_to_raw

    # Verificar qualidade dos dados após carregamento
    check_load_task = check_load()
    [stocks_gcs_to_raw, companies_gcs_to_raw] >> check_load_task

    # Executar transformação
    check_load_task >> transform

stock_dag()