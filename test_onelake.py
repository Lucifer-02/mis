import unittest
from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path
import pandas as pd
import io

from onelake import OnelakeConfig, append_file, upload_file, df_to_bytes

class TestOnelakeFunctions(unittest.TestCase):

    # Common mocks for DataLakeServiceClient and ClientSecretCredential
    def setUp(self):
        self.mock_config = OnelakeConfig(
            tenant_id="test_tenant_id",
            client_id="test_client_id",
            client_secret="test_client_secret",
            workspace_id="test_workspace_id",
            lakehouse_id="test_lakehouse_id",
            onelake_endpoint="https://test.dfs.fabric.microsoft.com"
        )
        self.mock_file_client = MagicMock()
        self.mock_file_system_client = MagicMock()
        self.mock_file_system_client.get_file_client.return_value = self.mock_file_client
        self.mock_service_client_patch = patch('onelake.DataLakeServiceClient')
        self.mock_credential_patch = patch('onelake.ClientSecretCredential')

        self.MockDataLakeServiceClient = self.mock_service_client_patch.start()
        self.MockClientSecretCredential = self.mock_credential_patch.start()

        self.MockDataLakeServiceClient.return_value.get_file_system_client.return_value = self.mock_file_system_client

    def tearDown(self):
        self.mock_service_client_patch.stop()
        self.mock_credential_patch.stop()

    def _assert_common_client_calls(self, mock_config, target_file):
        self.MockClientSecretCredential.assert_called_once_with(
            tenant_id=mock_config.tenant_id,
            client_id=mock_config.client_id,
            client_secret=mock_config.client_secret
        )
        self.MockDataLakeServiceClient.assert_called_once_with(
            account_url=mock_config.onelake_endpoint,
            credential=self.MockClientSecretCredential.return_value
        )
        self.mock_file_system_client.get_file_system_client.assert_called_once_with(
            file_system=mock_config.workspace_id
        )
        self.mock_file_system_client.get_file_client.assert_called_once_with(
            f"{mock_config.lakehouse_id}.Lakehouse/Files/{target_file}"
        )

    def test_append_file_success(self):
        target_file = Path("test_directory/test_file.txt")
        data_to_append = b"This is some appended data."
        offset = 100 # Example offset

        # Call the function under test
        append_file(self.mock_config, target_file, data_to_append, offset)

        # Assertions for common client initialization
        self._assert_common_client_calls(self.mock_config, target_file)

        # Assertions specific to append_file
        self.mock_file_client.append_data.assert_called_once_with(
            data=data_to_append,
            offset=offset,
            length=len(data_to_append)
        )
        self.mock_file_client.flush_data.assert_called_once_with(
            offset + len(data_to_append)
        )

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open, read_data=b"mock file content")
    def test_upload_file_success(self, mock_builtin_open, mock_os_path_exists):
        target_file = Path("test_directory/new_file.parquet")
        local_file = Path("local_data.parquet")
        overwrite = True
        chunk_size = 4 * 1024 * 1024
        file_content = b"mock file content"

        # Call the function under test
        upload_file(self.mock_config, target_file, local_file, overwrite, chunk_size)

        # Assertions for common client initialization
        self._assert_common_client_calls(self.mock_config, target_file)

        # Assertions specific to upload_file
        mock_os_path_exists.assert_called_once_with(local_file)
        mock_builtin_open.assert_called_once_with(local_file, "rb")
        self.mock_file_client.upload_data.assert_called_once_with(
            data=file_content,
            overwrite=overwrite,
            length=len(file_content),
            chunk_size=chunk_size
        )

    @patch('os.path.exists', return_value=False)
    def test_upload_file_local_file_not_found(self, mock_os_path_exists):
        target_file = Path("test_directory/new_file.parquet")
        local_file = Path("non_existent_file.parquet")

        # Call the function under test
        result = upload_file(self.mock_config, target_file, local_file)

        # Assertions
        mock_os_path_exists.assert_called_once_with(local_file)
        self.assertEqual(result, {}) # Expect an empty dict when file not found
        self.MockDataLakeServiceClient.assert_not_called() # No attempt to connect to OneLake

    def test_df_to_bytes(self):
        data = {'col1': [1, 2], 'col2': ['A', 'B']}
        df = pd.DataFrame(data)

        # Mock pandas to_parquet to control its output
        with patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            mock_buffer = io.BytesIO()
            # Simulate to_parquet writing to the buffer
            mock_to_parquet.side_effect = lambda buffer, index, engine: buffer.write(b"mock_parquet_data")

            result_bytes = df_to_bytes(df)

            mock_to_parquet.assert_called_once_with(unittest.mock.ANY, index=False, engine="pyarrow")
            self.assertEqual(result_bytes, b"mock_parquet_data")

    @patch('dotenv.load_dotenv')
    @patch('onelake.get_env_var')
    def test_onelake_config_load_config_success(self, mock_get_env_var, mock_load_dotenv):
        # Configure mock_get_env_var to return specific values
        mock_get_env_var.side_effect = {
            "AZURE_TENANT_ID": "mock_tenant",
            "AZURE_CLIENT_ID": "mock_client",
            "AZURE_CLIENT_SECRET": "mock_secret",
            "FABRIC_WORKSPACE_ID": "mock_workspace",
            "FABRIC_LAKEHOUSE_ID": "mock_lakehouse",
            "ONELAKE_ENDPOINT": "mock_endpoint"
        }.get

        config = OnelakeConfig.load_config()

        mock_load_dotenv.assert_called_once()
        self.assertEqual(config.tenant_id, "mock_tenant")
        self.assertEqual(config.client_id, "mock_client")
        self.assertEqual(config.client_secret, "mock_secret")
        self.assertEqual(config.workspace_id, "mock_workspace")
        self.assertEqual(config.lakehouse_id, "mock_lakehouse")
        self.assertEqual(config.onelake_endpoint, "mock_endpoint")

    @patch('dotenv.load_dotenv')
    @patch('onelake.get_env_var', side_effect=ValueError("Missing env var"))
    def test_onelake_config_load_config_missing_env_var(self, mock_get_env_var, mock_load_dotenv):
        with self.assertRaises(ValueError):
            OnelakeConfig.load_config()

        mock_load_dotenv.assert_called_once()
        mock_get_env_var.assert_called() # Ensure it was called at least once before raising
