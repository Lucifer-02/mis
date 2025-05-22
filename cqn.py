import logging
import os
import dotenv
from dataclasses import dataclass
import time
from typing import Callable

import oracledb

from utils import get_env_var


class CQNHandler:
    def __init__(self, conn: oracledb.Connection, handle_func: Callable, **kwargs):
        self.conn: oracledb.Connection = conn
        self.handle_func: Callable = handle_func
        self.kwargs: dict = kwargs
        self.subscription: oracledb.subscr.Subscription | None = None
        logging.info("CQNHandler initialized.")

    def handle_notification(self, message: oracledb.Message):
        """Callback function to handle CQN messages."""

        logging.info("Notification Received:")
        logging.info(f"Type: {message.type}")
        logging.info(f"Database Name: {message.dbname}")
        logging.info(f"Transaction ID: {message.txid}")

        if message.type == oracledb.EVENT_OBJCHANGE:
            logging.info("Object Change Event Details:")
            # Call the provided handle_func with necessary arguments
            self.handle_func(conn=self.conn, message=message, **self.kwargs)

        logging.info("-" * 20)

    def setup_subscription(self, schema: str, table_to_monitor: str):
        """Sets up the CQN subscription."""
        try:
            self.subscription = self.conn.subscribe(
                callback=self.handle_notification,
                operations=oracledb.OPCODE_ALLOPS,
                qos=oracledb.SUBSCR_QOS_ROWIDS | oracledb.SUBSCR_QOS_RELIABLE,
            )

            logging.info(f"Subscribed to changes on table: {schema}.{table_to_monitor}")
            logging.info("Waiting for insert notifications...")
            logging.info("Press Ctrl+C to exit.")

            self.subscription.registerquery(
                f"SELECT * FROM {schema}.{table_to_monitor}"
            )

        except oracledb.Error as e:
            logging.error(f"Database error during subscription setup: {e}")
            self.subscription = None
        except Exception as e:
            logging.error(f"Error during subscription setup: {e}")
            self.subscription = None

    def wait_for_notifications(self):
        """Waits for incoming CQN notifications."""
        if self.subscription:
            while self.subscription:
                time.sleep(1)
        else:
            logging.warning("Subscription not set up.")


@dataclass
class OracledbConfig:
    """Dataclass to hold OracleDB connection configuration."""

    db_user: str
    db_password: str
    db_dsn: str
    # Optional: Add lib_dir if thick mode is used and requires a specific path
    client_lib_dir: str | None = None

    @staticmethod
    def load_config() -> "OracledbConfig":
        # Load environment variables from .env file
        dotenv.load_dotenv()

        return OracledbConfig(
            db_user=get_env_var("DB_USER"),
            db_password=get_env_var("DB_PASSWORD"),
            db_dsn=get_env_var("DB_DSN"),
            client_lib_dir=os.getenv(
                "ORACLE_CLIENT_LIB_DIR"
            ),  # Assuming ORACLE_LIB_DIR env var for lib_dir
        )

    @staticmethod
    def connect(config: "OracledbConfig") -> oracledb.Connection:
        """Establishes and returns an OracleDB connection."""
        connection = None
        try:
            try:
                oracledb.init_oracle_client(lib_dir=config.client_lib_dir)
                logging.info("Oracle Client thick mode enabled.")
            except Exception as e:
                logging.error(
                    f"Oracle Client thick mode initialization failed: {e}. Falling back to thin mode. To enable thick mode, set environment variable ORACLE_CLIENT_LIB_DIR"
                )

            connection = oracledb.connect(
                user=config.db_user,
                password=config.db_password,
                dsn=config.db_dsn,
                events=True,  # Enable events for CQN
            )
            logging.info("Successfully connected to Oracle Database.")
            return connection
        except oracledb.Error as e:
            logging.error(f"Database connection error: {e}")
            raise e
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise e
