import os
from glob import glob
from minio import Minio
from helpers import load_cfg

CFG_FILE = "./config/datalake.yaml"
YEARS = ["2020", "2021", "2022", "2023"]

def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    # Create a client with the MinIO server
    client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )

    # Create bucket if not exist
    found = client.bucket_exists(bucket_name=datalake_cfg["bucket_name"])
    if not found:
        client.make_bucket(bucket_name=datalake_cfg["bucket_name"])
    else:
        print(f"Bucket {datalake_cfg['bucket_name']} already exists, skip creating!")

    for year in YEARS:
        # Upload files
        all_fps = glob(os.path.join(nyc_data_cfg["folder_path"], year, "*.parquet"))

        for fp in all_fps:
            print(f"Uploading {fp}")
            client.fput_object(
                bucket_name=datalake_cfg["bucket_name"],
                object_name=os.path.join(datalake_cfg["folder_name"], os.path.basename(fp)),
                file_path=fp,
            )

if __name__ == "__main__":
    main()