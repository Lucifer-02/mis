import io
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import dotenv
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import (
    DataLakeFileClient,
    DataLakeServiceClient,
)

from utils import get_env_var


# Define a dataclass to hold OneLake configuration
@dataclass
class OnelakeConfig:
    """
    Dataclass to hold Microsoft Fabric OneLake connection and location configuration.

    Attributes:
        tenant_id: The Azure Active Directory tenant ID.
        client_id: The Azure Active Directory client ID (application ID).
        client_secret: The Azure Active Directory client secret.
        workspace_id: The ID of the Fabric workspace (corresponds to the filesystem name in ADLS Gen2).
        lakehouse_id: The ID of the Fabric Lakehouse.
        onelake_endpoint: The endpoint URL for OneLake (defaults to "https://onelake.dfs.fabric.microsoft.com").
    """

    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str  # Corresponds to the filesystem name in ADLS Gen2
    lakehouse_id: str
    onelake_endpoint: str = field(
        default="https://onelake.dfs.fabric.microsoft.com"
    )  # Default endpoint

    @staticmethod
    def load_config() -> "OnelakeConfig":
        """
        Loads OneLake configuration from environment variables.

        It first loads variables from a .env file if present, then reads the required
        variables.

        Returns:
            An instance of OnelakeConfig populated with values from environment variables.

        Raises:
            ValueError: If any required environment variable is missing.
        """
        # Load environment variables from .env file
        dotenv.load_dotenv()

        return OnelakeConfig(
            tenant_id=get_env_var("AZURE_TENANT_ID"),
            client_id=get_env_var("AZURE_CLIENT_ID"),
            client_secret=get_env_var("AZURE_CLIENT_SECRET"),
            workspace_id=get_env_var("FABRIC_WORKSPACE_ID"),
            lakehouse_id=get_env_var("FABRIC_LAKEHOUSE_ID"),
            onelake_endpoint=get_env_var("ONELAKE_ENDPOINT"),
        )


def df_to_bytes(df: pd.DataFrame) -> bytes:
    """
    Converts a Pandas DataFrame to bytes in Parquet format.

    Args:
        df: The Pandas DataFrame to convert.

    Returns:
        A bytes object containing the DataFrame data in Parquet format.
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    return buffer.read()


def _get_datalake_file_client(config: OnelakeConfig, target_file: Path) -> DataLakeFileClient:
    """
    Helper function to get a DataLakeFileClient instance, handling authentication and client creation.
    """
    target_full_path = f"{config.lakehouse_id}.Lakehouse/Files/{target_file}"

    logging.info(
        f"Attempting to access OneLake path: '{config.workspace_id}/{target_full_path}'"
    )

    credential = ClientSecretCredential(
        tenant_id=config.tenant_id,
        client_id=config.client_id,
        client_secret=config.client_secret,
    )
    logging.info("Authentication credential created.")

    service_client = DataLakeServiceClient(
        account_url=config.onelake_endpoint,
        credential=credential,
    )
    logging.info(
        f"DataLakeServiceClient created for endpoint: {config.onelake_endpoint}"
    )

    file_system_client = service_client.get_file_system_client(
        file_system=config.workspace_id
    )
    logging.info(f"FileSystemClient created for workspace: {config.workspace_id}")

    file_client: DataLakeFileClient = file_system_client.get_file_client(
        target_full_path
    )
    logging.info(f"DataLakeFileClient created for path: {target_full_path}")
    return file_client


def upload_file(
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
        target_file: The desired path and name for the file in OneLake relative to the Lakehouse Files root.
        local_file: The path to the local file to upload.
        overwrite: Whether to overwrite the file if it already exists (defaults to True).
        chunk_size: The size of chunks (in bytes) to use when uploading the file.
                    Larger chunks can improve performance for large files but use more memory.
                    Defaults to 4MB (4 * 1024 * 1024).

    Returns:
        A dictionary containing the response from the upload operation.
        Returns an empty dictionary or logs an error if the local file is not found.
    """
    logging.info(
        f"Attempting to upload '{local_file}' to OneLake path: '{config.workspace_id}/{config.lakehouse_id}.Lakehouse/Files/{target_file}'"
    )

    file_client = _get_datalake_file_client(config, target_file)

    # Read the local file content
    if not os.path.exists(local_file):
        logging.error(f"Local file not found at '{local_file}'")
        return {}  # Return empty dict or raise error if file not found

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
        f"Upload successful a file with size {len(file_content) / 1024}KB to {target_file}!"
    )
    return resp


def append_file(
    config: OnelakeConfig,
    target_file: Path,
    data_to_append: bytes,
    offset: int,
) -> Dict[str, Any]:
    """
    Appends data to an existing file in Microsoft Fabric OneLake (via ADLS Gen2 API).

    Args:
        config: An instance of OnelakeConfig containing connection and location details.
        target_file: The desired path and name for the file in OneLake relative to the Lakehouse Files root.
        data_to_append: The bytes data to append to the file.
        offset: The offset in bytes from the beginning of the file where the data will be appended.
                This should typically be the current length of the file to append at the end.

    Returns:
        A dictionary containing the response from the append operation.
    """
    logging.info(
        f"Attempting to append data to OneLake path: '{config.workspace_id}/{config.lakehouse_id}.Lakehouse/Files/{target_file}' starting at offset {offset}"
    )

    file_client = _get_datalake_file_client(config, target_file)

    # Append the data
    file_client.append_data(
        data=data_to_append,
        offset=offset,
        length=len(data_to_append),
    )
    logging.info(f"Appended {len(data_to_append)} bytes to '{target_file}'.")

    # Flush the data to commit changes
    resp = file_client.flush_data(offset + len(data_to_append))
    logging.info(f"Flush successful for '{target_file}'.")

    return resp

def delete_file(
    config: OnelakeConfig,
    target_file: Path,
) -> Dict[str, Any]:
    """
    Deletes a file from Microsoft Fabric OneLake (via ADLS Gen2 API).

    Args:
        config: An instance of OnelakeConfig containing connection and location details.
        target_file: The path and name of the file in OneLake relative to the Lakehouse Files root.

    Returns:
        A dictionary containing the response from the delete operation.
    """
    logging.info(
        f"Attempting to delete OneLake path: '{config.workspace_id}/{config.lakehouse_id}.Lakehouse/Files/{target_file}'"
    )
    file_client = _get_datalake_file_client(config, target_file)
    resp = file_client.delete_file()
    logging.info(f"Deletion successful for '{target_file}'.")
    return resp
