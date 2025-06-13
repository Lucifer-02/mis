import logging
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List

import dotenv
import oracledb
import pandas as pd

import load_db
import onelake
import poll
from utils import format_timestamp, get_env_var


class Label(Enum):
    INSERT = auto()
    RERUN = auto()
    TRUNCATE = auto()
    FAILED = auto()
    OTHER = auto()


@dataclass
class SoiFlag:
    surrogate_key: int
    update_time: datetime
    table_name: str
    time_column: str
    time_value: str
    rerun_flag: bool
    status: bool


def parse_flag(flag: Dict) -> SoiFlag:
    return SoiFlag(
        surrogate_key=int(flag["surrogate_key"]),
        update_time=datetime.strptime(str(flag["update_time"]), "%Y-%m-%d %H:%M:%S.%f"),
        table_name=flag["table_name"],
        time_column=flag["time_column"],
        time_value=flag["time_value"],
        rerun_flag=bool(flag["rerun_flag"]),
        status=bool(flag["status"]),
    )


def assign_label(flag: SoiFlag) -> Label:
    logging.info(f"Flag: {flag}")

    if not flag.status:
        return Label.FAILED

    if flag.rerun_flag and flag.status:
        return Label.RERUN

    if flag.status and flag.time_column is None and flag.time_value is None:
        return Label.TRUNCATE

    if (
        flag.status
        and flag.time_column is not None
        and flag.time_value is not None
        and not flag.rerun_flag
    ):
        return Label.INSERT

    logging.error(f"Not support this flag: {flag}!!!")
    return Label.OTHER


def parse_flags(flags: pd.DataFrame) -> List[SoiFlag]:
    logging.info(flags)
    result = []
    for flag in flags.to_dict(orient="records"):
        result.append(parse_flag(flag))

    return result


def handle_insert(
    conn: oracledb.Connection, flag: SoiFlag, schema: str, save_dir: Path
) -> None:
    logging.info("Handling INSERT data.")

    before_num_files = count_files_from_chunks(
        [f for f in save_dir.glob(f"{flag.table_name}*") if f.is_file()]
    )

    logging.info("Getting new records...")
    saves = load_db.get_new_records(
        conn=conn,
        time_column=flag.time_column,
        time_value=flag.time_value,
        table_name=flag.table_name,
        schema=schema,
        save_dir=save_dir,
        save_name=f"{flag.table_name}_{format_timestamp(flag.update_time)}",
    )
    logging.info(f"Successfully load data to {saves}.")

    after_num_files = count_files_from_chunks(
        [f for f in save_dir.glob(f"{flag.table_name}*") if f.is_file()]
    )
    assert before_num_files + 1 == after_num_files, (
        f"After insert new data for table {flag.table_name}, number of file must increse by 1, actually is before: {before_num_files}, after: {after_num_files}."
    )

    for save in saves:
        success_default = onelake.upload_file(
            config=onelake.OnelakeConfig.load_config(),
            local_file=save,
            target_file=Path(save.name),
        )

        if success_default:
            logging.info("Upload successfully.")
        else:
            logging.error(f"Upload file {save} failed.")


def handle_truncate(
    conn: oracledb.Connection, flag: SoiFlag, schema: str, save_dir: Path
) -> None:
    logging.info("Handling TRUNCATE data.")

    before_num_files = count_files_from_chunks(
        [f for f in save_dir.glob(f"{flag.table_name}*") if f.is_file()]
    )
    assert before_num_files == 1, (
        f"Table {flag.table_name} is slow change table, without time column, so must only 1 file(not chunks)."
    )

    # TODO: delete old files first
    remove_chunks(
        filename=f"{flag.table_name}",
        save_dir=save_dir,
    )

    logging.info("Getting new records...")
    saves = load_db.get_all_records(
        conn=conn,
        table_name=flag.table_name,
        schema=schema,
        save_dir=save_dir,
        save_name=f"{flag.table_name}_{format_timestamp(flag.update_time)}",
    )
    after_num_files = count_files_from_chunks(
        [f for f in save_dir.glob(f"{flag.table_name}*") if f.is_file()]
    )
    assert before_num_files == after_num_files, (
        f"Number of file table {flag.table_name} before and after must equal."
    )

    # TODO: upload overwrite


def remove_chunks(filename: str, save_dir: Path) -> None:
    assert save_dir.is_dir()

    files = [f for f in save_dir.glob(f"{filename}*") if f.is_file()]
    assert len(files) > 0, "Must have written before"

    logging.info(f"Old files for file name {filename}: {files}")

    for file in files:
        logging.info(f"Removing old file {file}.")
        os.remove(file)


def count_files_from_chunks(file_paths: List[Path]):
    """
    Counts the number of unique files.

    Args:
        file_paths (list): A list of pathlib.Path objects, where each object represents
                          a filename (e.g., Path("a_1_chunk0.txt")).

    Returns:
        int: The total number of unique files.
    """
    unique_files = set()
    for file_path in file_paths:
        # Get the filename as a string from the pathlib.Path object
        filename = file_path.name

        # Find the last occurrence of '_chunk'
        chunk_index = filename.rfind("_chunk")
        if chunk_index != -1:
            # Extract the base filename before '_chunk'
            base_filename = filename[:chunk_index]
            unique_files.add(base_filename)
        else:
            # If '_chunk' is not found, treat the whole filename as a unique file
            unique_files.add(filename)
    return len(unique_files)


def handle_rerun(
    conn: oracledb.Connection,
    flag: SoiFlag,
    schema: str,
    save_dir: Path,
) -> None:
    logging.info(f"Handling RERUN data for table {flag.table_name}.")

    # TODO: get lastest insert flag
    sql = f"""
        SELECT * from {schema}.FLAG_TBL 
        WHERE TIME_COLUMN = :time_column and TIME_VALUE = :time_value and RERUN_FLAG = :rerun_flag
        ORDER BY SURROGATE_KEY DESC
        FETCH FIRST 1 ROWS ONLY
    """

    params = {
        "time_column": flag.time_column,
        "time_value": flag.time_value,
        "rerun_flag": 0,
    }
    df = pd.read_sql(sql=sql, con=conn, params=params)
    assert len(df) > 0, "Must have an instert before"

    logging.info(f"Current table: {df}.")

    prev_flag = parse_flags(df)[0]
    logging.info(prev_flag)

    before_num_files = count_files_from_chunks(
        [f for f in save_dir.glob(f"{flag.table_name}*") if f.is_file()]
    )

    # TODO: delete existing files
    filename = f"{prev_flag.table_name}_{format_timestamp(prev_flag.update_time)}"
    remove_chunks(
        filename=filename,
        save_dir=save_dir,
    )

    # write new data to files
    logging.info("Getting new records...")
    saves = load_db.get_new_records(
        conn=conn,
        time_column=flag.time_column,
        time_value=flag.time_value,
        table_name=flag.table_name,
        schema=schema,
        save_dir=save_dir,
        save_name=filename,
    )
    logging.info(f"Successfully load data to {saves}.")

    after_num_files = count_files_from_chunks(
        [f for f in save_dir.glob(f"{flag.table_name}*") if f.is_file()]
    )
    assert before_num_files == after_num_files, (
        "Number of file table {flag.table_name} before and after must equal."
    )


def handle_target_tables(
    tables: pd.DataFrame,
    *,
    conn: oracledb.Connection,
    schema: str,
    save_dir: Path,
) -> None:
    if set(tables.columns) != {
        "surrogate_key",
        "update_time",
        "table_name",
        "time_column",
        "time_value",
        "rerun_flag",
        "status",
    }:
        logging.error(f"Not valid columns actual: {tables.columns}")
        raise

    for flag in parse_flags(tables):
        try:
            label = assign_label(flag)
            if label == Label.INSERT:
                handle_insert(conn=conn, flag=flag, schema=schema, save_dir=save_dir)

            if label == Label.RERUN:
                handle_rerun(conn=conn, flag=flag, schema=schema, save_dir=save_dir)

            if label == Label.TRUNCATE:
                handle_truncate(conn=conn, flag=flag, schema=schema, save_dir=save_dir)

        except KeyError as ke:
            logging.error(
                f"Look like invalid columns, current columns {tables.columns}:", ke
            )
        except Exception as e:
            logging.error(
                f"Failed to handle table {flag.table_name}, {e}, skipping...",
                stack_info=True,
                exc_info=True,
            )


def upload_all():
    SCHEMA = get_env_var("SCHEMA")
    SAVE_DIR = get_env_var("SAVE_DIR")

    db_config = load_db.OracledbConfig.load_config()

    connection = db_config.connect(db_config)
    assert connection is not None

    table_names = load_db.get_all_tables(conn=connection, schema=SCHEMA)
    logging.info(f"All table in schema {SCHEMA}: {table_names}.")

    for table_name in table_names:
        saves = load_db.get_all_records(
            conn=connection,
            schema=SCHEMA,
            table_name=table_name,
            save_dir=Path(SAVE_DIR),
            save_name=f"{table_name}_{format_timestamp(datetime.now())}",
        )
        for save in saves:
            success_default = onelake.upload_file(
                config=onelake.OnelakeConfig.load_config(),
                local_file=save,
                target_file=Path(save.name),
            )

            if success_default:
                logging.info("Upload successfully.")


def upload_full_table(table_name: str):
    SCHEMA = get_env_var("SCHEMA")
    SAVE_DIR = get_env_var("SAVE_DIR")

    db_config = load_db.OracledbConfig.load_config()

    connection = db_config.connect(db_config)
    assert connection is not None

    logging.info(f"Loading table: {table_name}")
    saves = load_db.get_all_records(
        conn=connection,
        schema=SCHEMA,
        table_name=table_name,
        save_dir=Path(SAVE_DIR),
        save_name=f"{table_name}_{format_timestamp(datetime.now())}",
    )

    for save in saves:
        success_default = onelake.upload_file(
            config=onelake.OnelakeConfig.load_config(),
            local_file=save,
            target_file=Path(save.name),
        )

        if success_default:
            logging.info("Upload successfully.")


def poll_track():
    dotenv.load_dotenv()

    TABLE_TO_MONITOR = get_env_var("TABLE_TO_MONITOR")
    SCHEMA = get_env_var("SCHEMA")
    SAVE_DIR = get_env_var("SAVE_DIR")

    assert Path(SAVE_DIR).is_dir()

    db_config = load_db.OracledbConfig.load_config()
    logging.debug(f"DB config: {db_config}")

    connection = None

    try:
        engine = load_db.create_engine(db_config)
        poll.start_monitoring(
            conn=engine.connect(),
            key_col="surrogate_key",
            full_table_name=f"{SCHEMA}.{TABLE_TO_MONITOR}",
            last_check_file=Path("state.json"),
            handle_func=handle_target_tables,
            schema=SCHEMA,
            save_dir=Path(SAVE_DIR),
        )
    except Exception as e:
        logging.error(f"Error: {e}", stack_info=True, exc_info=True)
    except KeyboardInterrupt:
        logging.info("Exiting.")
    finally:
        if connection:
            try:
                connection.close()
                logging.info("Database connection closed.")
            except oracledb.Error as e:
                logging.error(f"Error closing connection: {e}")


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        filename="tracking.log",
        format="%(asctime)s-%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    dotenv.load_dotenv()

    # upload_all()
    poll_track()


if __name__ == "__main__":
    main()
