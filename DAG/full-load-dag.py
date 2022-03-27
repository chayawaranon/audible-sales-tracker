from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.utils.dates import days_ago

import pandas as pd
import requests

PROJECT_ID = "simple-elt-pipeline-345206"
MYSQL_CONNECTION = "mysql_audible_data" 
REGION = "asia-east2"
ZONE = "asia-east2-a"

PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": "cluster_sp"},
   "pyspark_job": {"main_python_file_uri": "gs://asia-east2-simple-pipeline-8aa38b84-bucket/data/pre-processing.py"}
   }

def get_data_from_mysql():
    # get connection
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # select table
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    # save to gcs
    audible_data.to_csv("/home/airflow/gcs/data/raw-data/audible_data.csv", index=False)
    audible_transaction.to_csv("/home/airflow/gcs/data/raw-data/audible_data_transaction.csv", index=False)
    print(f"saved successfully!")


with DAG(
    "etl-pipeline",
    start_date=days_ago(1),
    schedule_interval="@once",
) as dag:

    t1 = PythonOperator(
        task_id="get_data_from_db",
        python_callable=get_data_from_mysql
    )

    t2 = DataProcPySparkOperator(
       task_id="processing_data", 
       main="gs://asia-east2-simple-pipeline-8aa38b84-bucket/data/pre-processing.py", 
       region=REGION,
       cluster_name="cluster-sp", 
    )

    t3 = BashOperator(
        task_id="load_to_bq",
        bash_command="bq load \
                    --source_format=PARQUET \
                    --time_partitioning_field timestamp \
                    --autodetect audible_dataset.audible_data \
                    gs://asia-east2-simple-pipeline-8aa38b84-bucket/data/tranformed-data/parquet/*.parquet"
    )

    t1 >> t2 >> t3

