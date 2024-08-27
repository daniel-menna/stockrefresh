# Data Engineer Project: Stock Price Data Pipeline

![dataflow representation](/pics/dataflow.png)

Using Airflow, BigQuery, Google Cloud Storage, dbt, Soda, Metabase and Python

## Objective

The goal of this project is to create an end-to-end data pipeline from a Kaggle dataset on retail data. This involves modeling the data into fact-dimension tables, implementing data quality steps, utilizing modern data stack technologies (dbt, Soda, and Airflow), and storing the data in the cloud (Google Cloud Platform). Finally, the reporting layer is consumed in Metabase. The project is containerized via Docker and versioned on GitHub.

Tech Stack Used
- Python
- Docker and Docker-compose
- Soda.io
- Metabase
- Google Cloud Storage
- Google BigQuery
- Airflow (Astronomer version)
- dbt
- GitHub

## Before Executing the Project
To execute this project, it is necessary to follow the following steps:

### Install Docker
[Install Docker for your OS](https://docs.docker.com/desktop/)

### Install Astro CLI
[Install Astro CLI for your OS](https://www.astronomer.io/docs/astro/cli/install-cli)

### Clone this Github repository
In your terminal:

Clone the repo using Github CLI or Git CLI
```
gh repo clone daniel-menna/stockrefresh
```
or

```
git clone https://www.github.com/daniel-menna/stockrefresh.git
```
Open the folder with your code editor.

### Reinitialize the Airflow Project
Open the code editor terminal:
```
astro dev init
```
It will ask: `You are not in an empty directory. Are you sure you want to initialize a project? (y/n)` Type `y` and the project will be reinitialized.

### Build the projetc
Execute the following command in the terminal:
```
astro dev start
```
The default Airflow endpoint is [http://localhost:8080/](http://localhost:8080/)
- Default username: `admin`
- Default password: `admin`

### Create a GCP project
In your browser go to [https://console.cloud.google.com/](https://console.cloud.google.com/) and create a project, recomended something like: `airflow-stockprice`.

Copy your project ID and save it for later.