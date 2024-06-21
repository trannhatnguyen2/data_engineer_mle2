import os
import pandas as pd

# YEARS = ["2022", "2023"]
# DATA_PATH = "data/"
# DATA_CHECK_PATH = "check_data/"

# for year in YEARS:
#     year_path = os.path.join(DATA_PATH, year)
#     output_year_path = os.path.join(DATA_CHECK_PATH, year)

#     for file in os.listdir(year_path):
#         if file.endswith(".parquet"):
#             df = pd.read_parquet(os.path.join(year_path, file))

#             df.to_csv(os.path.join(output_year_path, file.replace(".parquet", ".csv")), index=False)

#             print("Saved successfully file: " + os.path.join(output_year_path, year))


# from pyspark.sql import SparkSession

# spark = (SparkSession.builder.master("local[*]").appName("Get Started").getOrCreate())

# df = spark.read.parquet("data/2020/yellow_tripdata_2022-05.parquet")

# df.show()
# df.printSchema()

# df = pd.read_parquet("data/2020/green_tripdata_2022-01.parquet")

# print(df.isnull().values.any())
# print(df.isnull().sum())

# {"aws_access_key_id": "minio_access_key", "aws_secret_key": "minio_secret_key", "endpoint_url": "http:localhost:9000"}


# import s3fs

# # Configuration for Minio
# minio_endpoint = "http://localhost:9000"
# access_key = "minio_access_key"
# secret_key = "minio_secret_key"
# bucket_name = "processed"
# parquet_file_key = "test/yellow_tripdata_2022-12.parquet"

# s3_fs = s3fs.S3FileSystem(
#     anon=False,
#     key=access_key,
#     secret=secret_key,
#     client_kwargs={'endpoint_url': minio_endpoint}
# )

# # Create the full S3 path to the parquet file
# file_path = f"s3://{bucket_name}/{parquet_file_key}"

# # Read the parquet file into a Pandas DataFrame
# df = pd.read_parquet(file_path, filesystem=s3_fs, engine='pyarrow')

# # df.to_parquet("s3://delta/test/test.parquet", filesystem=s3_fs, engine='pyarrow')

# # Display the DataFrame
# print(df)



# from pyspark.sql import SparkSession

# spark = (SparkSession.builder.config("spark.executor.memory", "4g") \
#                         .config(
#                             "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.2"
#                         )
#                         .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
#                         .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
#                         .config("spark.hadoop.fs.s3a.endpoint", "localhost:9000")
#                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
#                         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#                         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#                         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#                         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
#                         .appName("Read Parquet file")
#                         .getOrCreate()
#         )

# file_name = 'yellow_tripdata_2022-02.parquet'
# file_path = f's3a://datalake/batch/yellow_tripdata_2022-02.parquet'

# df = spark.read.parquet(file_path)
# # df.show()
# print(df.count())
# df.printSchema()

from pyarrow.parquet import ParquetFile
import pyarrow as pa 

pf = ParquetFile("./data/2024/yellow_tripdata_2024-01.parquet") 
first_ten_rows = next(pf.iter_batches(batch_size = 100)) 
df = pa.Table.from_batches([first_ten_rows]).to_pandas() 
print(df)

for _, row in df.iterrows():
    print(row)