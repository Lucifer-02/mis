import logging
import os
import tempfile
import unittest
import uuid
from pathlib import Path

from onelake import (
    OnelakeConfig,
    _get_datalake_file_client,
    append_file,
    delete_file,
    upload_file,
)


class IntegrationTestOnelake(unittest.TestCase):
    """
    Integration tests for OneLake file operations (upload, append, delete).
    These tests require actual Azure/OneLake credentials configured in environment variables.
    """

    @classmethod
    def setUpClass(cls):
        """Load OneLake configuration from environment variables once for all tests."""
        try:
            cls.config = OnelakeConfig.load_config()
            logging.info(
                "OneLake configuration loaded successfully for integration tests."
            )
        except ValueError as e:
            cls.config = None
            logging.error(
                f"Failed to load OneLake configuration: {e}. Integration tests will be skipped."
            )
            raise unittest.SkipTest(f"OneLake configuration missing: {e}")

    def setUp(self):
        """Prepare for each test, ensuring config is loaded."""
        if self.config is None:
            raise unittest.SkipTest("OneLake configuration not loaded. Skipping test.")
        self.test_dir = f"integration_tests/{uuid.uuid4()}"  # Use a unique directory for each test run

    def tearDown(self):
        """Clean up files created in OneLake after each test."""
        if self.config:
            # List all files under the test_dir and delete them
            # Note: This is a simplified cleanup. A more robust solution might list and delete
            # files within the directory if the OneLake API supported directory listing directly.
            # For this test, we assume we know the files we created.
            pass  # We will delete specific files in the tests themselves.

    def _create_temp_local_file(self, content: bytes) -> Path:
        """Helper to create a temporary local file with given content."""
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, "wb") as f:
            f.write(content)
        return Path(path)

    def _read_file_from_onelake(self, target_file: Path) -> bytes:
        """Helper to read file content from OneLake for verification."""
        file_client = _get_datalake_file_client(self.config, target_file)
        try:
            download_stream = file_client.download_file()
            return download_stream.readall()
        except Exception as e:
            logging.error(f"Failed to read file {target_file} from OneLake: {e}")
            return b""

    def test_01_upload_and_delete_file(self):
        """Tests uploading a file to OneLake and then deleting it."""
        file_name = "test_upload_file.txt"
        target_file_path = Path(f"{self.test_dir}/{file_name}")
        local_content = b"This is content for the upload test file."
        local_file = self._create_temp_local_file(local_content)

        try:
            logging.info(f"Uploading file: {local_file} to {target_file_path}")
            upload_response = upload_file(
                self.config, target_file_path, local_file, overwrite=True
            )
            self.assertIsNotNone(upload_response)
            self.assertIn(
                "etag", upload_response
            )  # Check for a common success indicator

            # Verify content after upload
            uploaded_content = self._read_file_from_onelake(target_file_path)
            self.assertEqual(uploaded_content, local_content)
            logging.info(f"Successfully uploaded and verified file: {target_file_path}")

        finally:
            # Clean up: delete the file from OneLake
            logging.info(f"Deleting file: {target_file_path}")
            delete_response = delete_file(self.config, target_file_path)
            self.assertIsNotNone(
                delete_response
            )  # Check if delete operation was acknowledged
            os.remove(local_file)
            logging.info(f"Successfully deleted file from OneLake: {target_file_path}")

    def test_02_append_to_file(self):
        """Tests appending data to an existing file in OneLake."""
        file_name = "test_append_file.txt"
        target_file_path = Path(f"{self.test_dir}/{file_name}")

        initial_content = b"Initial content for append test."
        append_content = b"Appended new content."
        combined_content = initial_content + append_content

        local_initial_file = self._create_temp_local_file(initial_content)

        try:
            # 1. Upload initial file
            logging.info(f"Uploading initial file for append test: {target_file_path}")
            upload_file(
                self.config, target_file_path, local_initial_file, overwrite=True
            )
            uploaded_initial_content = self._read_file_from_onelake(target_file_path)
            self.assertEqual(uploaded_initial_content, initial_content)
            logging.info("Initial file uploaded and verified.")

            # 2. Append data
            initial_file_size = len(uploaded_initial_content)
            logging.info(
                f"Appending data to {target_file_path} at offset {initial_file_size}"
            )
            append_response = append_file(
                self.config, target_file_path, append_content, initial_file_size
            )
            self.assertIsNotNone(append_response)
            self.assertIn(
                "etag", append_response
            )  # Check for a common success indicator

            # 3. Verify content after append
            appended_file_content = self._read_file_from_onelake(target_file_path)
            self.assertEqual(appended_file_content, combined_content)
            logging.info(
                f"Successfully appended and verified content to: {target_file_path}"
            )

        finally:
            # Clean up: delete the file from OneLake
            logging.info(f"Deleting file after append test: {target_file_path}")
            delete_file(self.config, target_file_path)
            os.remove(local_initial_file)
            logging.info(f"Successfully deleted file from OneLake: {target_file_path}")


if __name__ == "__main__":
    # Set up logging for better visibility during tests
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    unittest.main()
