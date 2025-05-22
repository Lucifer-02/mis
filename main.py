import logging
import dotenv
from pathlib import Path
from datetime import datetime

import oracledb
import pandas as pd

from cqn import OracledbConfig, CQNHandler
from utils import get_env_var
import load_db
import upload


def handle_target_tables(
    conn: oracledb.Connection, tables: pd.DataFrame, schema: str, save_dir: Path
) -> None:
    if set(tables.columns) == {
        "SURROGATE_KEY",
        "UPDATE_TIME",
        "TABLE_NAME",
        "TIME_COLUMN",
        "TIME_VALUE",
        "RERUN_FLAG",
        "STATUS",
    }:
        logging.error("Not valid columns in flag table.")
        exit()

    for _, row in tables.iterrows():
        logging.info(row)

        table_name = ""

        try:
            table_name = str(row["TABLE_NAME"])
            time_column = str(row["TIME_COLUMN"])
            time_value = str(row["TIME_VALUE"])
            logging.info(row["UPDATE_TIME"])
            timestamp = datetime.strptime(str(row["UPDATE_TIME"]), "%Y-%m-%d %H:%M:%S")

            logging.info("Getting new records...")
            saved = load_db.get_new_records(
                conn=conn,
                time_column=time_column,
                time_value=time_value,
                table_name=table_name,
                schema=schema,
                timestamp=timestamp,
                save_dir=save_dir,
            )
            logging.info(f"Successfully load data to {saved}.")

            # Pass the config to test_upload
            # resp = upload.test_upload(local_file_to_upload=saved)
            # logging.info(resp)

        except KeyError as ke:
            logging.error(f"look like invalid columns:", ke)
        except Exception as e:
            logging.error(
                f"Failed to handle table {table_name}, {e}, skipping...",
                stack_info=True,
                exc_info=True,
            )


def handle_changes(
    conn: oracledb.Connection, message: oracledb.Message, schema: str, save_dir: Path
) -> None:
    for table in message.tables:
        logging.info(f"Table: {table.name}")
        ids = [row.rowid for row in table.rows]
        assert conn is not None
        assert table.name is not None

        try:
            df = load_db.fetch_new_records(
                conn=conn,
                ids=ids,
                full_table_name=table.name,
            )
            assert len(df) > 0
            logging.info(df)
            logging.info("---------------------")
            handle_target_tables(conn=conn, tables=df, schema=schema, save_dir=save_dir)

        except oracledb.Error as e:
            logging.exception(
                f"Failed with table {table}: {e}, skipping...",
                stack_info=True,
                exc_info=True,
            )


def upload_all():

    SCHEMA = get_env_var("SCHEMA")
    SAVE_DIR = get_env_var("SAVE_DIR")

    db_config = OracledbConfig.load_config()

    connection = db_config.connect(db_config)
    assert connection is not None

    table_names = load_db.get_all_tables(conn=connection, schema=SCHEMA)
    logging.info(f"All table in schema {SCHEMA}: {table_names}.")

    for table_name in table_names:
        save = load_db.get_all_records(
            conn=connection,
            schema=SCHEMA,
            table_name=table_name,
            timestamp=datetime.now(),
            save_dir=Path(SAVE_DIR),
            debug=False,
        )
        upload.test_upload(save)


def track():

    TABLE_TO_MONITOR = get_env_var("TABLE_TO_MONITOR")
    SCHEMA = get_env_var("SCHEMA")
    SAVE_DIR = get_env_var("SAVE_DIR")

    assert Path(SAVE_DIR).is_dir()

    db_config = OracledbConfig.load_config()

    connection = None

    try:
        connection = db_config.connect(db_config)

        cqn_handler = CQNHandler(
            conn=connection,
            handle_func=handle_changes,
            save_dir=Path(SAVE_DIR),
            schema=SCHEMA,
        )
        cqn_handler.setup_subscription(SCHEMA, TABLE_TO_MONITOR)
        cqn_handler.wait_for_notifications()

    except oracledb.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Error: {e}")
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
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    dotenv.load_dotenv()

    # upload_all()
    track()


if __name__ == "__main__":
    main()
