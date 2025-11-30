from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
import sys
import os
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.models.param import Param

# Add scripts folder to path so we can import the ETL function
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
from weather_etl import run_weather_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def get_aws_credentials():
    conn = BaseHook.get_connection('aws_s3_conn')
    return {
        'access_key': conn.login,  # In Airflow 'Login' field maps to Access Key
        'secret_key': conn.password # 'Password' field maps to Secret Key
    }

def etl_wrapper(**kwargs):
    # 1. Fetch API Key from Airflow Variables
    api_key = Variable.get("openweather_api_key")
    
    # 2. Fetch AWS Creds from Airflow Connections
    aws_creds = get_aws_credentials()
    
    # 3. Define Bucket Name
    bucket_name = "weather-data-pipeline-apoorv"


    # 4. DETERMINE CITY NAME
    # Check if the user passed a configuration JSON when triggering (e.g. {"city": "New Delhi"})
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf and 'city' in dag_run.conf:
        city = dag_run.conf['city']
        print(f"Using city from Manual Trigger configuration: {city}")
    else:
        # Fallback to Airflow Variable if no manual input
        city = Variable.get("weather_city_default", default_var="Gurugram")
        print(f"Using default city from Variables: {city}") 

    # 5. Pass everything to the main script
    run_weather_etl(
        api_key=api_key,
        access_key=aws_creds['access_key'],
        secret_key=aws_creds['secret_key'],
        bucket_name=bucket_name,
        city=city
    )


with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Weather ETL pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,

    # THIS SECTION CREATES THE UI FORM
    params={
        "city": Param("London", type="string", description="Enter City Name"),
    }

) as dag:

    # Task 1: Run the Python ETL Script
    extract_transform_load_s3 = PythonOperator(
        task_id='extract_weather_to_s3',
        python_callable=etl_wrapper
    )

    # Task 2: Load data from S3 to Snowflake
    # We are using the SnowflakeOperator (requires connection setup in UI)
    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql="""
            COPY INTO weather_db.weather_schema.weather_data 
            FROM @weather_db.weather_schema.weather_stage
            FILE_FORMAT = (FORMAT_NAME = weather_db.weather_schema.csv_format)
            ON_ERROR = 'CONTINUE';
        """,
    )

    extract_transform_load_s3 >> load_to_snowflake

