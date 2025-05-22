import io
import logging
import os
from dataclasses import dataclass
from typing import Dict, Any
from pathlib import Path

import dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeFileClient,
)
import pandas as pd


def _get_env_var(var_name: str) -> str:
    """Fetch an environment variable and log an error if missing."""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable {var_name} is missing!")
    return value


# Define a dataclass to hold OneLake configuration
@dataclass
class OnelakeConfig:
    """Dataclass to hold OneLake connection and location configuration."""

    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str  # Corresponds to the filesystem name in ADLS Gen2
    lakehouse_id: str
    onelake_endpoint: str = (
        "https://onelake.dfs.fabric.microsoft.com"  # Default endpoint
    )

    @staticmethod
    def load_config() -> "OnelakeConfig":
        # Load environment variables from .env file
        dotenv.load_dotenv()

        return OnelakeConfig(
            tenant_id=_get_env_var("AZURE_TENANT_ID"),
            client_id=_get_env_var("AZURE_CLIENT_ID"),
            client_secret=_get_env_var("AZURE_CLIENT_SECRET"),
            workspace_id=_get_env_var("FABRIC_WORKSPACE_ID"),
            lakehouse_id=_get_env_var("FABRIC_LAKEHOUSE_ID"),
            onelake_endpoint=_get_env_var("ONELAKE_ENDPOINT"),
        )


def df_to_bytes(df: pd.DataFrame) -> bytes:
    """Converts a Pandas DataFrame to bytes in Parquet format."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    return buffer.read()


def upload_file_to_onelake(
    config: OnelakeConfig,  # Accept the configuration as a dataclass instance
    target_file: Path,
    local_file: Path,
    overwrite: bool = True,
    chunk_size: int = 1024 * 1024 * 4,  # Default to 4MB chunks
) -> Dict[str, Any]:
    """
    Uploads a local file to a specified path in Microsoft Fabric OneLake (via ADLS Gen2 API).

    Args:
        config: An instance of OnelakeConfig containing connection and location details.
        target_file_name: The desired name for the file in OneLake.
        local_file_path: The path to the local file to upload.
        overwrite: Whether to overwrite the file if it already exists (defaults to True).
        chunk_size: The size of chunks (in bytes) to use when uploading the file.
                    Larger chunks can improve performance for large files but use more memory.
                    Defaults to 4MB (4 * 1024 * 1024).

    Returns:
        True if the upload was successful, False otherwise.
    """
    # Use values from the config dataclass
    target_full_path = f"{config.lakehouse_id}.Lakehouse/Files/{target_file}"

    logging.info(
        f"Attempting to upload '{local_file}' to OneLake path: '{config.workspace_id}/{target_full_path}'"
    )

    # Authenticate using client secret
    credential = ClientSecretCredential(
        tenant_id=config.tenant_id,
        client_id=config.client_id,
        client_secret=config.client_secret,
    )
    logging.info("Authentication credential created.")
    # Create DataLakeServiceClient
    service_client = DataLakeServiceClient(
        account_url=config.onelake_endpoint,  # Use endpoint from config
        credential=credential,
    )
    logging.info(
        f"DataLakeServiceClient created for endpoint: {config.onelake_endpoint}"
    )

    # Get the file system client (workspace)
    file_system_client = service_client.get_file_system_client(
        file_system=config.workspace_id  # Use workspace_id from config
    )
    logging.info(f"FileSystemClient created for workspace: {config.workspace_id}")

    # Get the file client for the target path
    file_client: DataLakeFileClient = file_system_client.get_file_client(
        target_full_path
    )
    logging.info(f"DataLakeFileClient created for path: {target_full_path}")

    # Read the local file content
    if not os.path.exists(local_file):
        logging.error(f"Local file not found at '{local_file}'")

    with open(local_file, "rb") as f:
        file_content = f.read()

    logging.info(
        f"Read {len(file_content)} bytes from '{local_file}'. Starting upload with chunk size {chunk_size} bytes..."
    )

    # Upload the data
    resp = file_client.upload_data(
        data=file_content,
        overwrite=overwrite,
        length=len(file_content),
        chunk_size=chunk_size,
    )

    logging.info(
        f"Upload successful a file with size {len(file_content)/1024}KB to {target_file}!"
    )
    return resp


def test_upload(local_file_to_upload: Path):

    target_file_name = local_file_to_upload

    success_default = upload_file_to_onelake(
        config=OnelakeConfig.load_config(),
        local_file=local_file_to_upload,
        target_file=target_file_name,
    )

    if success_default:
        logging.info("Upload successfully.")
    else:
        logging.error("Upload failed.")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    test_upload(Path("./uv.lock"))


# Example usage:
if __name__ == "__main__":
    main()
