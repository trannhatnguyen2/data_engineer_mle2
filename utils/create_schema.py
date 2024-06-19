import os
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient
load_dotenv(".env")


def main():

    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    create_staging_schema = """CREATE SCHEMA IF NOT EXISTS staging;"""

    create_dw_schema = """CREATE SCHEMA IF NOT EXISTS dw;"""

    try:
        pc.execute_query(create_staging_schema)
        pc.execute_query(create_dw_schema)
    except Exception as e:
        print(f"Failed to create schema with error: {e}")


if __name__ == "__main__":
    main()