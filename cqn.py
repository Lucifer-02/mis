import logging
import os
import dotenv
from dataclasses import dataclass
import time
from typing import Callable

import oracledb

from utils import get_env_var


class CQNHandler:
    """
    Handles Oracle Database Continuous Query Notification (CQN) setup and processing.

    This class manages the lifecycle of an Oracle CQN subscription, registers a
    query to monitor for changes, and dispatches incoming database change
    notifications to a user-defined handler function.
    """

    def __init__(self, conn: oracledb.Connection, handle_func: Callable, **kwargs):
        """
        Initializes the CQNHandler.

        Args:
            conn: An active oracledb.Connection object with events=True enabled.
                  This connection is used for establishing and managing the
                  CQN subscription.
            handle_func: A callable (function) that will be executed when a
                         relevant database change notification is received. This
                         function should accept 'conn', 'message', and any
                         keyword arguments passed in **kwargs.
            **kwargs: Additional keyword arguments to pass to the handle_func
                      when it is called.
        """
        self.conn: oracledb.Connection = conn
        self.handle_func: Callable = handle_func
        self.kwargs: dict = kwargs
        self.subscription: oracledb.subscr.Subscription | None = None
        logging.info("CQNHandler initialized.")

    def handle_notification(self, message: oracledb.Message):
        """
        Callback function invoked by the Oracle Database client when a CQN
        message is received for the registered subscription.

        This function logs basic message details and, if the message indicates
        an object change (like INSERT, UPDATE, DELETE), it calls the user-provided
        `handle_func` to process the change.

        Args:
            message: An oracledb.Message object containing details about the
                     database event that triggered the notification.
        """

        logging.info("Notification Received:")
        logging.info(f"Type: {message.type}")
        logging.info(f"Database Name: {message.dbname}")
        logging.info(f"Transaction ID: {message.txid}")

        # Check if the message indicates an object change (like INSERT, UPDATE, DELETE)
        if message.type == oracledb.EVENT_OBJCHANGE:
            logging.info("Object Change Event Details:")
            # Call the provided handle_func with necessary arguments (connection, message, and stored kwargs)
            self.handle_func(conn=self.conn, message=message, **self.kwargs)

        logging.info("-" * 20)

    def setup_subscription(self, schema: str, table_to_monitor: str):
        """
        Sets up the CQN subscription on the provided database connection
        for the specified table.

        Args:
            schema: The database schema (owner) of the table to monitor.
            table_to_monitor: The name of the table to watch for changes.

        Sets the self.subscription attribute upon successful subscription creation.
        Logs errors if subscription setup fails.
        """
        try:
            # Subscribe to database events
            self.subscription = self.conn.subscribe(
                callback=self.handle_notification,  # Specify the callback function
                operations=oracledb.OPCODE_ALLOPS,  # Monitor all operations (INSERT, UPDATE, DELETE, ALTER)
                qos=oracledb.SUBSCR_QOS_ROWIDS | oracledb.SUBSCR_QOS_RELIABLE,
                # QoS Flags:
                # SUBSCR_QOS_ROWIDS: Request ROWIDs of the changed rows in the notification message.
                # SUBSCR_QOS_RELIABLE: Request reliable message delivery (messages are queued if client is down).
            )

            logging.info(f"Subscribed to changes on table: {schema}.{table_to_monitor}")
            logging.info("Waiting for insert notifications...")
            logging.info("Press Ctrl+C to exit.")

            # Register a query with the subscription. Notifications will be sent
            # for changes affecting rows that would be returned by this query.
            # Using SELECT * FROM table registers for changes on the entire table.
            self.subscription.registerquery(
                f"SELECT * FROM {schema}.{table_to_monitor}"
            )

        except oracledb.Error as e:
            logging.error(f"Database error during subscription setup: {e}")
            self.subscription = None  # Ensure subscription is None on failure
        except Exception as e:
            logging.error(f"Error during subscription setup: {e}")
            self.subscription = None  # Ensure subscription is None on failure

    def wait_for_notifications(self):
        """
        Enters a blocking loop to keep the application alive and allow the
        CQN subscription to receive notifications.

        The subscription mechanism operates asynchronously in the background.
        This method simply prevents the main script from exiting while
        waiting for callbacks to occur. It sleeps briefly to avoid high CPU usage.
        """
        if self.subscription:
            # Loop indefinitely while the subscription object exists
            while self.subscription:
                time.sleep(1)  # Sleep to prevent the loop from consuming excessive CPU
        else:
            logging.warning("Subscription not set up. Cannot wait for notifications.")


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
