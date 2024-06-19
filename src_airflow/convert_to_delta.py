import sys
import os
import warnings
import traceback
import logging
import time
from minio import Minio

sys.path.append("/opt/airflow/dags/scripts/")
from helpers import load_cfg

from pyspark import SparkConf, SparkContext
from delta import *

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
BASE_PATH = "/opt/airflow/"

CFG_FILE = BASE_PATH + "config/datalake_airflow.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

# Create a client with the MinIO server
minio_client = Minio(
    endpoint=datalake_cfg["endpoint"],
    access_key=datalake_cfg["access_key"],
    secret_key=datalake_cfg["secret_key"],
    secure=False,
)
###############################################


###############################################
# Utils
###############################################
def create_bucket(bucket_name):
    """
        Create bucket if not exist
    """
    try:
        found = minio_client.bucket_exists(bucket_name=bucket_name)
        if not found:
            minio_client.make_bucket(bucket_name=bucket_name)
        else:
            print(f"Bucket {bucket_name} already exists, skip creating!")
    except Exception as err:
        print(f"Error: {err}")
        return []


def list_parquet_files(bucket_name, prefix=""):
    """
        Function to list all Parquet files in a bucket (MinIO)
    """
    try:
        # List all objects in the bucket with the given prefix
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        # Filter and collect Parquet file names
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
        
        return parquet_files
    except Exception as err:
        print(f"Error: {err}")
        return []
###############################################


###############################################
# PySpark
###############################################
def create_spark_session():
    """
        Create the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    try:
        builder = SparkSession.builder \
                    .appName("Converting to Delta Lake") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws-3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"]).getOrCreate()

        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def load_minio_config(spark_context: SparkContext):
    """
        Establish the necessary configurations to access to MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio_access_key")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio_secret_key")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.warning(f"MinIO config could not be created successfully due to exception: {e}")


def main_convert():
    # start_time = time.time()

    # spark = create_spark_session()
    # load_minio_config(spark.sparkContext)

    # # Create bucket 'delta'
    # cfg = load_cfg(CFG_FILE)
    # datalake_cfg = cfg["datalake"]
    # create_bucket(datalake_cfg['bucket_name_3'])

    # for file in list_parquet_files(datalake_cfg['bucket_name_2'], prefix=datalake_cfg['folder_name']):
    #     print(file)

    #     path = f"s3a://{datalake_cfg['bucket_name_2']}/" + file
    #     logging.info(f"Reading parquet file: {file}")

    #     df = spark.read.parquet(path)

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.config("spark.executor.memory", "4g") \
                            .config(
                                "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4"
                            )
                            .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
                            .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
                            .config("spark.hadoop.fs.s3a.endpoint", "minio:9000")
                            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                            .config("spark.hadoop.fs.s3a.path.style.access", "true")
                            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                            .appName("Read Parquet file")
                            .getOrCreate()
            )

    file_path = f's3a://raw/batch/yellow_tripdata_2022-03.parquet'

    df = spark.read.parquet(file_path)
    df.show()
    print(df.count())
    df.printSchema()