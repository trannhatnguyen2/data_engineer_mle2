import os
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from minio import Minio
import urllib3

# from scripts.elt_pipeline import extract_load_to_datalake, transform_data

default_args = {
    "owner": "t.nhatnguyen",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def create_bucket():
    config = {
        "bucket_name": "delta",
        "minio_endpoint": "http://localhost:9000",
        "minio_username": "minio_access_key",
        "minio_password": "minio_secret_key",
    }

    http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
    urllib3.disable_warnings()

    minio_client = Minio(
        "minio:9000",
        access_key=config["minio_username"],
        secret_key=config["minio_password"],
        secure=False,
        http_client = http_client
    )

    found = minio_client.bucket_exists(bucket_name=config["bucket_name"])
    if not found:
        minio_client.make_bucket(bucket_name=config["bucket_name"])
    else:
        print(f"Bucket {config['bucket_name']} already exists, skip creating!")


with DAG("test_pipeline", start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args) as dag:
    system_maintenance_task = BashOperator(
        task_id="system_maintenance_task",
        bash_command='echo "Install some pypi libs..."',
    )

    create_bucket = PythonOperator(
        task_id="create_bucket",
        python_callable=create_bucket
    )

system_maintenance_task >> create_bucket







# def create_bucket():
#     from minio import Minio
#     from scripts.helpers import load_cfg

#     CFG_FILE = "/opt/airflow/config/datalake.yaml"

#     cfg = load_cfg(CFG_FILE)
#     datalake_cfg = cfg["datalake"]
#     nyc_data_cfg = cfg["nyc_data"]

#     # Create a client with the MinIO server
#     minio_client = Minio(
#         endpoint=datalake_cfg["endpoint"],
#         access_key=datalake_cfg["access_key"],
#         secret_key=datalake_cfg["secret_key"],
#         secure=False,
#     )

#     # Create bucket if not exist
#     found = minio_client.bucket_exists(bucket_name=datalake_cfg["bucket_name"])
#     if not found:
#         minio_client.make_bucket(bucket_name=datalake_cfg["bucket_name"])
#     else:
#         print(f"Bucket {datalake_cfg['bucket_name']} already exists, skip creating!")


# with DAG("test_pipeline", start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args) as dag:
#     system_maintenance_task = BashOperator(
#         task_id="system_maintenance_task",
#         # bash_command='apt-get update && apt-get upgrade -y'
#         bash_command='echo "Install some pypi libs..."',
#     )

#     create_bucket = PythonOperator(
#         task_id="create_bucket",
#         python_callable=create_bucket
#     )

# system_maintenance_task >> create_bucket