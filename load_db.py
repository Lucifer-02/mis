import logging
from datetime import datetime
import os
from pathlib import Path
from typing import List

import oracledb
import pandas as pd


def test_fetch_new_records(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM etl.my_table")
        rows = cursor.fetchall()
        for row in rows:
            print("New Record:", row)


def get_all_records(
    conn: oracledb.Connection,
    table_name: str,
    schema: str,
    timestamp: datetime,
    save_dir: Path,
    debug: bool,
) -> Path:

    QUERY = f"""SELECT * FROM {schema}.{table_name}"""
    return _get_records(
        conn=conn,
        table_name=table_name,
        sql=QUERY,
        timestamp=timestamp,
        save_dir=save_dir,
        debug=debug,
    )


def _get_records(
    conn: oracledb.Connection,
    table_name: str,
    sql: str,
    timestamp: datetime,
    save_dir: Path,
    debug: bool = True,
    chunksize=400000,
) -> Path:
    assert save_dir.is_dir()

    TEMP_DIR = save_dir / Path("temp")

    logging.info(f"Query: {sql}")

    ts = timestamp.strftime("%Y%m%dT%H%M%S")

    temp_files = []
    # avoid out of memory
    chunks = pd.read_sql(
        sql=sql,
        con=conn,
        chunksize=chunksize,
        dtype_backend="pyarrow",  # use pyarrow for better type resolution
    )
    for i, chunk in enumerate(chunks):
        file_name = TEMP_DIR / Path(f"{table_name}_{ts}_{i}.parquet")
        chunk.to_parquet(file_name)
        temp_files.append(file_name)

    logging.info(f"There are {temp_files} chunks.")

    save = save_dir / Path(f"{table_name}_{ts}.parquet")
    combine = pd.concat(
        [
            pd.read_parquet(file, dtype_backend="pyarrow", engine="pyarrow")
            for file in temp_files
        ],
        ignore_index=True,
    )
    logging.info("Combined all chunks.")
    combine.to_parquet(save, engine="pyarrow", coerce_timestamps="ms")
    logging.info(f"Saved {save}")

    if not debug:
        for file in temp_files:
            os.remove(file)

    return save


def get_new_records(
    conn: oracledb.Connection,
    table_name: str,
    time_column: str,
    time_value: str,
    timestamp: datetime,
    schema: str,
    save_dir: Path,
    debug: bool = True,
    chunksize=400000,
) -> Path:
    assert save_dir.is_dir()

    logging.info("Connection successful")
    QUERY = f"SELECT * FROM {schema}.{table_name} WHERE {time_column} = '{time_value}'"
    logging.info(f"Query: {QUERY}")
    logging.info(
        f"Starting getting new records for table {schema}.{table_name} at {time_value}..."
    )

    return _get_records(
        conn=conn,
        table_name=table_name,
        sql=QUERY,
        timestamp=timestamp,
        save_dir=save_dir,
        chunksize=chunksize,
        debug=debug,
    )


def fetch_new_records(
    conn: oracledb.Connection, ids: list, full_table_name: str
) -> pd.DataFrame:
    assert len(ids) > 0

    # Create placeholders like :1, :2, :3, ...
    placeholders = ", ".join([f":{i+1}" for i in range(len(ids))])
    QUERY = f"""
        SELECT *
        FROM {full_table_name}
        WHERE ROWID IN ({placeholders})
    """
    logging.info(f"Query: {QUERY}")

    df = pd.read_sql(QUERY, con=conn, params=ids)

    return df


def get_all_tables(conn: oracledb.Connection, schema: str) -> List[str]:
    assert schema.isupper()

    cursor = conn.cursor()

    # Query to get table names
    cursor.execute(
        f"""
        SELECT table_name 
        FROM all_tables 
        WHERE owner = :schema
        """,
        schema=schema,
    )

    table_names = [row[0] for row in cursor.fetchall()]

    return table_names
