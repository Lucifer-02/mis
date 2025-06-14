import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

import load_db


def load_last_record_id(last_check_file: Path) -> str:
    """Load the last processed record ID from file"""

    if not last_check_file.is_file():
        logging.info(f"{last_check_file} not exist.")

    with open(last_check_file, "r") as f:
        data = json.load(f)
        index = data["last_record_id"]
        assert index is not None
        return index


def save_last_record_id(record_id: str, last_check_file: Path):
    """Save the last processed record ID to file"""

    data = {"last_record_id": record_id, "last_update": datetime.now().isoformat()}
    with open(last_check_file, "w") as f:
        json.dump(data, f)
        logging.info("Saved to id file.")


def start_monitoring(
    conn,
    key_col: str,
    full_table_name: str,
    poll_interval: int = 30,
    last_check_file: Path = Path("state.json"),
    *,
    handle_func: Callable,
    **kwargs,
):
    """Start the monitoring loop"""
    logging.info("Starting Oracle tracking table monitor...")

    while True:
        try:
            last_key = load_last_record_id(last_check_file=last_check_file)
            new_records = load_db.get_new_records_linear(
                conn=conn,
                key_col=key_col,
                last_key=last_key,
                full_table_name=full_table_name,
            )

            if len(new_records) > 0:
                logging.info(f"Found {len(new_records)} new records: {new_records}")
                kwargs["conn"] = conn
                handle_func(new_records, **kwargs)

                save_last_record_id(
                    record_id=str(new_records.iloc[-1][key_col]),
                    last_check_file=last_check_file,
                )
            else:
                logging.debug("No new records found")

            time.sleep(poll_interval)

        except KeyboardInterrupt:
            logging.info("Monitor stopped by user")
            break
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            time.sleep(poll_interval)
