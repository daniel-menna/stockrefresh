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

    # Instalar yfinance
    install_yfinance_task = install_yfinance()

    # Expandir a tarefa para buscar os dados de todas as commodities
    dados_commodities = buscar_dados_commodities.expand(simbolo=commodities)

    # Assegurar que buscar_dados_commodities seja executada após a instalação do yfinance
    install_yfinance_task >> dados_commodities

    # Concatenar os resultados
    dados_concatenados = concatena_dados(dados_commodities)
    
    # Salvar os dados no CSV
    salvar_dados_csv(dados_concatenados)

stock_dag()