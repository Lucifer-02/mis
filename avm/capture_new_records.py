#!usr/bin/python3

import sys
import logging
import os
from pathlib import Path
from datetime import date

import pandas as pd
import cx_Oracle


def get_new_records(table_name: str, record_creation_date: str) -> pd.DataFrame:
    # Connect to Oracle
    USER = "data_stage"
    PASSWORD = "mis2024"
    DSN = "0.0.0.0:1521/MIS_STG"

    with cx_Oracle.connect(
        user=USER, password=PASSWORD, dsn=DSN
    ) as connection:
        logging.info("Connection successful")
        QUERY = f"""SELECT * FROM {USER}.{table_name} WHERE DATE8 = '{record_creation_date}'"""
        logging.info(f"Starting getting new records for table {table_name} at {record_creation_date}...")
        df  = pd.read_sql(QUERY, con=connection)
        logging.info(f"Finished getting new records for table {table_name} at {record_creation_date}.")
        return df


def save_result(table_name: str, partition: date, content: pd.DataFrame):
    path = BASE_PATH / Path(f"mis_stage/{table_name}/{partition}.csv")
    if not path.parent.exists():
        path.parent.mkdir(parents=True)

    content.to_csv(path, index=False)
    logging.info(f"Saved result to {path}.")

def main():
    RECORD_CREATION_DATE = sys.argv[1]
    TABLE_NAME = sys.argv[2]

    try:
        df = get_new_records(table_name=TABLE_NAME, record_creation_date=RECORD_CREATION_DATE)
        save_result(table_name=TABLE_NAME, partition=RECORD_CREATION_DATE, content=df)
    except:
        logging.exception("Getting new records failed!")


if __name__ == "__main__":

    BASE_PATH = Path('/home/oracle/hoangnlv')

    logging.basicConfig(
        filename= Path('/home/oracle/hoangnlv/trigger.log'),
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    os.environ['ORACLE_HOME'] = '/opt/oracle/product/19c/dbhome_1'

    main()
