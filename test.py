import logging
from pathlib import Path
from datetime import datetime

import dotenv

from cqn import OracledbConfig
from utils import get_env_var
import load_db


def main():
    dotenv.load_dotenv()

    SCHEMA = get_env_var("SCHEMA")

    db_config = OracledbConfig.load_config()

    connection = db_config.connect(db_config)
    assert connection is not None

    df = load_db.get_new_records1(
        conn=connection,
        key_col="SURROGATE_KEY",
        last_key=153,
        full_table_name=f"{SCHEMA}.FLAG_TBL",
    )
    print(df)


if __name__ == "__main__":
    main()
