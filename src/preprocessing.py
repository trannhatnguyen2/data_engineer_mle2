import os
import pandas as pd

DATA_PATH = "data/"
YEARS = ["2022"]
TAXI_LOOKUP_PATH = "src/data/taxi_lookup.csv"


def drop_column(df, file):
    """
        Drop columns 'store_and_fwd_flag'
    """
    if "store_and_fwd_flag" in df.columns:
        df = df.drop(columns=["store_and_fwd_flag"])
        print("Dropped column store_and_fwd_flag from file: " + file)
    else:
        print("Column store_and_fwd_flag not found in file: " + file)

    return df


def merge_taxi_zone(df, file):
    """
        Merge dataset with taxi zone lookup
    """
    df_lookup = pd.read_csv(TAXI_LOOKUP_PATH)

    if "pickup_latitude" not in df.columns:
        # merge for pickup locations
        df = df.merge(df_lookup, left_on="pulocationid", right_on="LocationID")
        df = df.drop(columns=["LocationID", "Borough", "service_zone", "zone"])
        df = df.rename(columns={
            "latitude" : "pickup_latitude",
            "longitude" : "pickup_longitude"
        })
    
    if "dropoff_latitude" not in df.columns:
        # merge for pickup locations
        df = df.merge(df_lookup, left_on="dolocationid", right_on="LocationID")
        df = df.drop(columns=["LocationID", "Borough", "service_zone", "zone"])
        df = df.rename(columns={
            "latitude" : "dropoff_latitude",
            "longitude" : "dropoff_longitude"
        })

    if "Unnamed: 0_x" in df.columns:
        # drop rows with missing values
        df = df.drop(columns=['Unnamed: 0_x']).dropna()
    
    if "Unnamed: 0_y" in df.columns:
        df = df.drop(columns=['Unnamed: 0_y']).dropna()

    print("Merged data from file: " + file)

    return df


def transform_data(df, file):
    """
    Green:
        Rename column: lpep_pickup_datetime, lpep_dropoff_datetime, ehail_fee
        Drop: trip_type
    Yellow:
        Rename column: tpep_pickup_datetime, tpep_dropoff_datetime, airport_fee
    """
    
    if file.startswith("green"):
        # rename columns
        df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
                "ehail_fee": "fee"
            },
            inplace=True
        )

        # drop column
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)

    elif file.startswith("yellow"):
        # rename columns
        df.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "airport_fee": "fee"
            },
            inplace=True
        )

    # fix data type in columns 'payment_type'
    df["payment_type"] = df["payment_type"].fillna(0)
    if "payment_type" in df.columns:
        df["payment_type"] = df["payment_type"].astype(int)

    # drop column 'fee'
    if "fee" in df.columns:
        df.drop(columns=["fee"], inplace=True)
                
    # Remove missing data
    df = df.dropna()
    df = df.reindex(sorted(df.columns), axis=1)
    
    print("Transformed data from file: " + file)

    return df


if __name__ == "__main__":
    for year in YEARS:
        year_path = os.path.join(DATA_PATH, year)
        for file in os.listdir(year_path):
            if file.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(year_path, file), engine='pyarrow')

                # lower case all columns
                df.columns = map(str.lower, df.columns)

                df = drop_column(df, file)
                df = merge_taxi_zone(df, file)
                df = transform_data(df, file)

                # save to parquet file
                df.to_parquet(os.path.join(year_path, file), index=False, engine='pyarrow')

                print("Finished preprocessing data in file: " + file)
                print("==========================================================================================")

                
