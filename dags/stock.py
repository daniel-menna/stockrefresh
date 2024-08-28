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
from datetime import datetime, timedelta
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

    @task.external_python(python='/usr/local/airflow/pandas_venv/bin/python', execution_timeout=timedelta(minutes=30))
    def fetch_commodities_data(symbol, period='3mo', interval='1d'):
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        data = ticker.history(period=period, interval=interval)[['Open', 'Close']]
        info_fields = ['shortName', 'city', 'state', 'zip', 'country', 'industry', 'enterpriseValue']
        info = {field: ticker.info.get(field, None) for field in info_fields}
        data['symbol'] = symbol
        info['symbol'] = symbol
        return data, info
    
    @task()
    def concatenate_price_data(all_data):
        import pandas as pd
        price_data = [data for data, _ in all_data]
        return pd.concat(price_data)

    @task()
    def concatenate_info_data(all_data):
        import pandas as pd
        info_data = [info for _, info in all_data]
        return pd.DataFrame(info_data)

    @task()
    def save_data_to_csv(concatenated_data, file_path):
        if file_path == 'include/datasets/stocks_info.csv':
            concatenated_data.to_csv(file_path, index=False)
        else:
            concatenated_data.to_csv(file_path, index=True)

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

    upload_locals_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_locals_csv_to_gcs',
        src='include/datasets/locals.csv',
        dst='raw/locals.csv',
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
            metadata=Metadata(schema='stock')
        ),
        use_native_support=True,
        native_support_kwargs={
            "encoding": "ISO_8859_1",
        }
    )

    locals_gcs_to_raw = aql.load_file(
        task_id='locals_gcs_to_raw',
        input_file=File(
            f'gs://{bucket_name}/raw/locals.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_locals',
            conn_id='gcp',
            metadata=Metadata(schema='stock')
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

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )    

    # Install yfinance
    install_yfinance_task = install_yfinance()

    # Expand the task to fetch data for all commodities
    commodity_data = fetch_commodities_data.expand(symbol=commodities)

    # Define dependencies
    install_yfinance_task >> commodity_data

    # Concatenate the results
    concatenated_price_data = concatenate_price_data(commodity_data)
    concatenated_info_data = concatenate_info_data(commodity_data)
    
    # Save the data to CSV
    save_price_data_csv_task = save_data_to_csv(concatenated_price_data, 'include/datasets/stocks.csv')
    save_info_data_csv_task = save_data_to_csv(concatenated_info_data, 'include/datasets/stocks_info.csv')

    # Define dependencies for uploading the CSV files to GCS
    save_price_data_csv_task >> upload_stockprice_csv_to_gcs
    save_info_data_csv_task >> upload_stockinfo_csv_to_gcs >> upload_locals_csv_to_gcs


    # Create the dataset in BigQuery and load the data
    [upload_stockprice_csv_to_gcs, upload_stockinfo_csv_to_gcs, upload_locals_csv_to_gcs] >> create_stock_dataset
    create_stock_dataset >> [locals_gcs_to_raw, stocks_gcs_to_raw, companies_gcs_to_raw]


    # Check data quality after loading
    check_load_task = check_load()
    [stocks_gcs_to_raw, companies_gcs_to_raw, locals_gcs_to_raw] >> check_load_task

    # Execute transformations
    check_load_task >> transform
    check_transform_task = check_transform()
    transform >> check_transform_task

    # Create Data Marts
    check_transform_task >> report

stock_dag()