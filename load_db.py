import logging
import os
from pathlib import Path
from typing import List

import oracledb
import pandas as pd


def test_fetch_new_records(conn: oracledb.Connection) -> None:
    """
    Fetch and print all records from a test table.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM etl.my_table")
            rows = cursor.fetchall()
            for row in rows:
                print("New Record:", row)
    except Exception as e:
        logging.error(f"Error fetching new records: {e}")


def get_all_records(
    conn: oracledb.Connection,
    table_name: str,
    schema: str,
    timestamp: str,
    save_dir: Path,
    debug: bool,
) -> Path:
    """
    Fetch all records from a table and save them as a Parquet file.
    """
    QUERY = f"SELECT * FROM {schema}.{table_name}"
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
    timestamp: str,
    save_dir: Path,
    debug: bool = True,
    chunksize: int = 200000,
) -> Path:
    """
    Fetch records in chunks, save them as Parquet files, and optionally combine them.
    """
    assert save_dir.is_dir(), f"Save directory {save_dir} does not exist."

    TEMP_DIR = save_dir / "temp"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    logging.debug(f"Executing Query: {sql}")

    temp_files = []
    chunks = pd.read_sql(
        sql=sql,
        con=conn,
        chunksize=chunksize,
        dtype_backend="pyarrow",
    )
    for i, chunk in enumerate(chunks):
        file_name = TEMP_DIR / f"{table_name}_{timestamp}_{i}.parquet"
        chunk.to_parquet(file_name)
        temp_files.append(file_name)

    logging.info(f"Generated {len(temp_files)} chunk files.")

    save_path = save_dir / f"{table_name}_{timestamp}.parquet"

    combined = pd.concat(
        [pd.read_parquet(file, dtype_backend="pyarrow") for file in temp_files],
        ignore_index=True,
    )
    logging.info(combined.info())
    combined.to_parquet(save_path, engine="pyarrow", coerce_timestamps="ms")

    if not debug:
        for file in temp_files:
            os.remove(file)

    return save_path


def get_new_records(
    conn: oracledb.Connection,
    table_name: str,
    time_column: str,
    time_value: str,
    timestamp: str,
    schema: str,
    save_dir: Path,
    debug: bool = True,
    chunksize: int = 200000,
) -> Path:
    """
    Fetch new records based on a time column and save them as a Parquet file.
    """
    assert save_dir.is_dir(), f"Save directory {save_dir} does not exist."

    QUERY = f"SELECT * FROM {schema}.{table_name} WHERE {time_column} = {time_value}"
    logging.info(f"Fetching new records for {schema}.{table_name} at {time_value}.")

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
    conn: oracledb.Connection, ids: List[str], full_table_name: str
) -> pd.DataFrame:
    """
    Fetch records by ROWID.
    """
    assert ids, "ID list cannot be empty."

    placeholders = ", ".join([f":{i+1}" for i in range(len(ids))])
    QUERY = f"SELECT * FROM {full_table_name} WHERE ROWID IN ({placeholders})"

    try:
        df = pd.read_sql(QUERY, con=conn, params=ids)
        return df
    except Exception as e:
        logging.error(f"Error fetching records by ROWID: {e}")
        raise


def get_all_tables(conn: oracledb.Connection, schema: str) -> List[str]:
    """
    Get all table names for a given schema.
    """
    assert schema.isupper(), "Schema name must be uppercase."

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name 
                FROM all_tables 
                WHERE owner = :schema
                """,
                schema=schema,
            )
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error fetching table names: {e}")
        raise


def get_new_records1(
    conn: oracledb.Connection, key_col: str, last_key: str, full_table_name: str
) -> pd.DataFrame:
    """
    Fetch new records based on a key column and last key value.
    """
    QUERY = f"SELECT * FROM {full_table_name} WHERE {key_col} > :last_key"

    try:
        df = pd.read_sql(QUERY, con=conn, params={"last_key": last_key})
        return df
    except Exception as e:
        logging.error(f"Error fetching new records by key: {e}")
        raise
