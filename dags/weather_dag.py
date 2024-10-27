# Import necessary libraries and modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import requests
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging

# Configuration constants
KEY_VAULT_NAME = "weatherkey24"
KEY_VAULT_URI = f"https://{KEY_VAULT_NAME}.vault.azure.net/"
SECRET_NAME_API_KEY = "OpenWeatherAPIKey"
SECRET_NAME_BLOB_CONNECTION_STRING = "AzureBlobStorageConnectionString"

# Azure Blob Storage container names
CONTAINERS = {
    "json": "weatherjson",
    "csv": "weathercsv",
    "parquet": "weatherparquet",
    "logs": "weatherlogs"
}

# Paths for configuration and logging
CONFIG_FILE_PATH = "/home/gusairflow/airflow/weather_script/config_locations.json"
LOG_DIR = "/home/gusairflow/airflow/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE_NAME = f"weather_data_collector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def setup_custom_logging(**kwargs):
    """
    Initialize logging to file and console.
    """
    log_path = os.path.join(LOG_DIR, LOG_FILE_NAME)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    kwargs['ti'].xcom_push(key='log_path', value=log_path)
    return log_path

def load_locations(config_path, **kwargs):
    """
    Load location details from a JSON configuration file.
    """
    with open(config_path, 'r') as file:
        locations = json.load(file).get("locations", [])
        if not locations:
            logging.warning("No locations found in the configuration file.")
    kwargs['ti'].xcom_push(key='locations', value=locations)

def get_secrets(**kwargs):
    """
    Retrieve API key and Blob connection string from Azure Key Vault.
    """
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KEY_VAULT_URI, credential=credential)
    api_key = client.get_secret(SECRET_NAME_API_KEY).value
    blob_conn_str = client.get_secret(SECRET_NAME_BLOB_CONNECTION_STRING).value
    kwargs['ti'].xcom_push(key='api_key', value=api_key)
    kwargs['ti'].xcom_push(key='blob_conn_str', value=blob_conn_str)

def initialize_blob_service(**kwargs):
    """
    Initialize Azure Blob service client.
    """
    blob_conn_str = kwargs['ti'].xcom_pull(key='blob_conn_str')
    return blob_conn_str

def ensure_containers(**kwargs):
    """
    Ensure required containers exist in Azure Blob Storage.
    """
    blob_conn_str = kwargs['ti'].xcom_pull(key='blob_conn_str')
    blob_client = BlobServiceClient.from_connection_string(blob_conn_str)
    for container in CONTAINERS.values():
        container_client = blob_client.get_container_client(container)
        if not container_client.exists():
            container_client.create_container()

def fetch_weather_data(lat, lon, api_key):
    """
    Fetch weather data from OpenWeather API for given latitude and longitude.
    """
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def process_data_and_upload(**kwargs):
    """
    Process weather data, convert temperature to Celsius, and upload as JSON, CSV, and Parquet to Blob Storage.
    """
    try:
        # Retrieve required values from XCom
        api_key = kwargs['ti'].xcom_pull(key='api_key')
        blob_conn_str = kwargs['ti'].xcom_pull(key='blob_conn_str')
        locations = kwargs['ti'].xcom_pull(key='locations')

        # Validate that required parameters are available
        if not api_key or not blob_conn_str or not locations:
            logging.error("Missing API key, Blob connection string, or locations.")
            return

        blob_client = BlobServiceClient.from_connection_string(blob_conn_str)
        weather_records = []
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Iterate through each location to fetch weather data
        for loc in locations:
            try:
                lat, lon, city = loc.get("lat"), loc.get("lon"), loc.get("city")
                data = fetch_weather_data(lat, lon, api_key)

                # Convert temperature from Kelvin to Celsius
                temp_kelvin = data.get("main", {}).get("temp")
                temp_celsius = temp_kelvin - 273.15 if temp_kelvin is not None else None

                # Prepare weather record
                record = {
                    "city": data.get("name"),
                    "timestamp": datetime.fromtimestamp(data.get("dt"), tz=timezone.utc).isoformat(),
                    "latitude": data.get("coord", {}).get("lat"),
                    "longitude": data.get("coord", {}).get("lon"),
                    "weather_main": data.get("weather", [{}])[0].get("main"),
                    "weather_description": data.get("weather", [{}])[0].get("description"),
                    "temperature_kelvin": temp_kelvin,
                    "temperature_celsius": temp_celsius,
                    "humidity": data.get("main", {}).get("humidity"),
                    "pressure": data.get("main", {}).get("pressure"),
                    "wind_speed": data.get("wind", {}).get("speed"),
                    "wind_deg": data.get("wind", {}).get("deg"),
                    "cloudiness": data.get("clouds", {}).get("all"),
                    "visibility": data.get("visibility"),
                    "sunrise": datetime.fromtimestamp(data.get("sys", {}).get("sunrise"), tz=timezone.utc).isoformat(),
                    "sunset": datetime.fromtimestamp(data.get("sys", {}).get("sunset"), tz=timezone.utc).isoformat(),
                    "timezone": data.get("timezone"),
                    "country": data.get("sys", {}).get("country"),
                    "sunrise_unix": data.get("sys", {}).get("sunrise"),
                    "sunset_unix": data.get("sys", {}).get("sunset"),
                    "created_timestamp": datetime.now(timezone.utc).isoformat()
                }
                weather_records.append(record)

                # Upload raw JSON to Blob Storage
                json_filename = f"weather_{city}_{timestamp}.json"
                blob_client.get_blob_client(container=CONTAINERS["json"], blob=json_filename).upload_blob(json.dumps(data), overwrite=True)
            except Exception as e:
                logging.error(f"Error processing location {city}: {e}")

        # Upload CSV and Parquet if records exist
        if weather_records:
            df = pd.DataFrame(weather_records)

            # Upload CSV
            csv_content = df.to_csv(index=False)
            csv_filename = f"weather_data_{timestamp}.csv"
            blob_client.get_blob_client(container=CONTAINERS["csv"], blob=csv_filename).upload_blob(csv_content, overwrite=True)
            logging.info(f"Uploaded CSV {csv_filename} successfully.")

            # Upload Parquet
            parquet_filename = f"weather_data_{timestamp}.parquet"
            parquet_file_path = f"/tmp/{parquet_filename}"

            # Save DataFrame as Parquet locally
            df.to_parquet(parquet_file_path, index=False)

            # Upload Parquet file to Blob Storage
            with open(parquet_file_path, "rb") as data_file:
                blob_client.get_blob_client(container=CONTAINERS["parquet"], blob=parquet_filename).upload_blob(data_file, overwrite=True)
            logging.info(f"Uploaded Parquet file {parquet_filename} successfully.")
    except Exception as e:
        logging.error(f"Error in process_data_and_upload: {e}")

# Define the DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Do not wait for previous DAG runs
    'start_date': datetime(2024, 1, 1),  # Start date for the DAG
    'retries': 1,  # Number of retry attempts if a task fails
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Instantiate the DAG
dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='A pipeline for collecting and storing weather data',
    schedule_interval='@hourly',  # Run the DAG hourly
    catchup=False,  # Do not perform backfill runs
)

# Define the tasks within the DAG
setup_logging_task = PythonOperator(
    task_id='setup_logging',
    python_callable=setup_custom_logging,
    provide_context=True,
    dag=dag
)

load_locations_task = PythonOperator(
    task_id='load_locations',
    python_callable=load_locations,
    op_args=[CONFIG_FILE_PATH],
    dag=dag
)

get_secrets_task = PythonOperator(
    task_id='get_secrets',
    python_callable=get_secrets,
    dag=dag
)

initialize_blob_service_task = PythonOperator(
    task_id='initialize_blob_service',
    python_callable=initialize_blob_service,
    dag=dag
)

ensure_containers_task = PythonOperator(
    task_id='ensure_containers',
    python_callable=ensure_containers,
    dag=dag
)

process_data_and_upload_task = PythonOperator(
    task_id='process_data_and_upload',
    python_callable=process_data_and_upload,
    dag=dag
)

# Define the sequence of task execution
setup_logging_task >> load_locations_task >> get_secrets_task >> initialize_blob_service_task >> ensure_containers_task >> process_data_and_upload_task
