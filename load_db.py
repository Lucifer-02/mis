import logging
from pathlib import Path
from typing import List
import os
from dataclasses import dataclass

import oracledb
import pandas as pd
import dotenv
import sqlalchemy

from utils import get_env_var


@dataclass
class OracledbConfig:
    """Dataclass to hold OracleDB connection configuration."""

    user: str
    password: str
    port: int
    host: str
    service: str
    # Optional: Add lib_dir if thick mode is used and requires a specific path
    client_lib_dir: str | None = None

    @staticmethod
    def load_config() -> "OracledbConfig":
        dotenv.load_dotenv()

        return OracledbConfig(
            user=get_env_var("DB_USER"),
            password=get_env_var("DB_PASSWORD"),
            host=get_env_var("DB_HOST"),
            port=int(get_env_var("DB_PORT")),
            service=get_env_var("DB_SERVICE"),
            client_lib_dir=os.getenv(
                "ORACLE_CLIENT_LIB_DIR"
            ),  # Assuming ORACLE_LIB_DIR env var for lib_dir
        )

    @staticmethod
    def connect(config: "OracledbConfig") -> oracledb.Connection:
        """Establishes and returns an OracleDB connection."""
        connection = None

        try:
            oracledb.init_oracle_client(lib_dir=config.client_lib_dir)
            logging.info("Oracle Client thick mode enabled.")
        except Exception as e:
            logging.error(
                f"Oracle Client thick mode initialization failed: {e}. Falling back to thin mode. To enable thick mode, set environment variable ORACLE_CLIENT_LIB_DIR"
            )
            raise e

        try:
            connection = oracledb.connect(
                user=config.user,
                password=config.password,
                host=config.host,
                port=config.port,
                service_name=config.service,
                events=True,  # Enable events for CQN
            )
            logging.info("Successfully connected to Oracle Database.")
            return connection
        except oracledb.Error as e:
            logging.error(f"Database connection error: {e}")
            raise e


def create_engine(config: OracledbConfig) -> sqlalchemy.Engine:

    return sqlalchemy.create_engine(
        f"oracle+oracledb://{config.user}:{config.password}@{config.host}:{config.port}/?service_name={config.service}",
    )


def get_all_records(
    conn: oracledb.Connection,
    table_name: str,
    schema: str,
    save_dir: Path,
    save_name: str,
) -> List[Path]:
    """
    Fetch all records from a table and save them as a Parquet file.
    """
    QUERY = f"SELECT * FROM {schema}.{table_name}"
    return _get_records(
        conn=conn,
        table_name=table_name,
        sql=QUERY,
        save_dir=save_dir,
        save_name=save_name,
    )


def _get_records(
    conn: oracledb.Connection,
    table_name: str,
    sql: str,
    save_dir: Path,
    save_name: str,
    chunksize: int = 200000,
) -> List[Path]:
    """
    Fetch records in chunks, save them as Parquet files, and optionally combine them.
    """
    assert save_dir.is_dir(), f"Save directory {save_dir} does not exist."

    logging.debug(f"Executing Query: {sql}")

    chunk_files: List[Path] = []
    chunks = pd.read_sql(
        sql=sql,
        con=conn,
        chunksize=chunksize,
        dtype_backend="pyarrow",
    )

    for i, chunk in enumerate(chunks):
        if chunk.empty:
            logging.warning(f"Empty chunk of table {table_name}.")

        chunk_file = save_dir / f"{save_name}_chunk{i}.parquet"
        chunk.to_parquet(
            chunk_file,
            engine="pyarrow",
            compression="zstd",  # fix crash on windows server vmware, https://github.com/apache/arrow/issues/25326
            coerce_timestamps="ms",  # fix conflict timestamp type oracledb and spark
        )
        chunk_files.append(chunk_file)
        logging.info(f"Written {chunk_file}.")

    logging.info(f"Written {len(chunk_files)} chunk files.")

    return chunk_files


def get_new_records(
    conn: oracledb.Connection,
    table_name: str,
    time_column: str,
    time_value: str,
    schema: str,
    save_dir: Path,
    save_name: str,
    chunksize: int = 200000,
) -> List[Path]:
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
        save_dir=save_dir,
        save_name=save_name,
        chunksize=chunksize,
    )


def fetch_records_by_ids(
    conn: oracledb.Connection, ids: List[str], full_table_name: str
) -> pd.DataFrame:
    """
    Fetch records by ROWID.
    """
    assert ids, "ID list cannot be empty."

    placeholders = ", ".join([f":{i+1}" for i in range(len(ids))])
    QUERY = f"SELECT * FROM {full_table_name} WHERE ROWID IN ({placeholders})"

    df = pd.read_sql(QUERY, con=conn, params=ids)
    return df


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


def get_new_records_linear(
    conn, key_col: str, last_key: str, full_table_name: str
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
