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




def cqn_track():

    TABLE_TO_MONITOR = get_env_var("TABLE_TO_MONITOR")
    SCHEMA = get_env_var("SCHEMA")
    SAVE_DIR = get_env_var("SAVE_DIR")

    assert Path(SAVE_DIR).is_dir()

    db_config = OracledbConfig.load_config()
    logging.info(db_config)

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
        logging.error(f"Database error: {e}", stack_info=True, exc_info=True)
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

def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s-%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    dotenv.load_dotenv()

    cqn_track()
