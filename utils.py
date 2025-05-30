import os
from datetime import datetime


def format_timestamp(timestamp: datetime) -> str:
    return timestamp.strftime("%Y%m%dT%H%M%S%f")


def get_env_var(var_name: str) -> str:
    """Fetch an environment variable and log an error if missing."""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable {var_name} is missing!")
    return value
