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

## Preparing everything
To run this project without any headake, it is necessary to follow few steps:

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

### Include your project ID into environment variables
Change the following files:

`.env (GCP_PROJECT_ID)`
`include\dbt\models\sources\sources.yml (database)`
`include\dbt\profiles.yml (project)`

### Create a bucket on Google Cloud Storage
With the project selected, go to [https://console.cloud.google.com/storage/browser](https://console.cloud.google.com/storage/browser) and create a Bucket. Use the name <yourname>_stockprice. And change the variable `bucket_name` `value` to your bucket name at the `dags\stock.py` file.

### Create a service accout to your project
Go to the `IAM tab`, and create the Service account with the name `airflow-stockprice`. Give `admin` access to `Google Cloud Storage` and `BigQuery`, and export the json keys. Rename the file to `service_account.json` and put inside the folder `include/gcp/` (you will have to create this folder).

### Create a new connection into Airflow
In your airflow, at the [http://localhost:8080/](http://localhost:8080/), login and go to Admin → Connections. Create a new connection and use this configs:

- id: gcp
- type: Google Cloud
- Keypath Path: `/usr/local/airflow/include/gcp/service_account.json`

Save it.

### Create your SODA account and your API keys
Go to [https://www.soda.io/](https://www.soda.io/) and click "start a trial" and create an account. Then, login and go to your profile, API Keys and create a new API key.

Copy the soda_cloud code, it will look like this:
```
soda_cloud:
  host: cloud.us.soda.io
  api_key_id: <KEY>
  api_key_secret: <SECRET>
```

And paste it in `include\soda\configuration.yml` or edit the `.env` file with the respective values. Note that, in this example the account created was in the US region, if your account is in EU region, you will have to change the "host" variable.

## Running the project
Ok, with eveything ready, you can run the project as described below:

### It is time to see Airflow brighting
With your Airflow running, go to [http://localhost:8080/](http://localhost:8080/) and click on DAGs, and click on the retail DAG. Then, start the DAG (play button on the upper right side).

It will go step by step, and if everything was followed, you will get a green execution at the end. Check in your GCP Storage account if the file was uploaded succesfully, in your BigQuery tab if the tables was been built and in your Soda dashboard if everithing is fine.

Then, move to the Metabase and build your own Dashboard. The Metabase service is on the [http://localhost:3000/](http://localhost:3000/)

### Metabase
Go to the [http://localhost:3000/](http://localhost:3000/) and create your local account. When the `Add your data` option shows up, choose `BigQuery`, and enter your details. It will ask for a `Display name`, I recomend `BigQuery_StockPrice` or something like that. The `project ID` is your `GCP Project name` (mine was stokprice). And finally the `service_account.json` the one that you saved in the `include/gcp/` folder.

Connect the database, and it's all set.

## Contact
- [Linkedin](https://www.linkedin.com/in/danielrmenna)
- [Github](https://www.github.com/daniel-menna)
- [danielmenna@gmail.com](mailto:danielmenna@gmail.com)

## References
Project based on [Mark Lamberti](https://www.youtube.com/@MarcLamberti) and [Alan Lanceloth](https://www.github.com/alanceloth) videos.
