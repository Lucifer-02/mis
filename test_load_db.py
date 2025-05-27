import unittest
from unittest.mock import MagicMock, patch
import oracledb
import pandas as pd
from pathlib import Path
import os
from unittest.mock import MagicMock, patch, call

from load_db import _get_records # Absolute import based on path

# Assume TestCase class is already defined and imported

class TestLoadDb(unittest.TestCase): # Assuming this class exists

    @patch('load_db.logging')
    @patch('load_db.combine_chunks')
    @patch('load_db.os.remove')
    @patch('load_db.pd') # Patch pandas to mock read_sql and DataFrame.to_parquet
    def test__get_records_success_debug_true(self, MockPandas, MockOsRemove, MockCombineChunks, MockLogging):
        """Test _get_records successfully fetches and combines chunks with debug=True."""
        # Setup mocks
        mock_conn = MagicMock()
        table_name = "TEST_TABLE"
        sql = "SELECT * FROM TEST_TABLE"
        timestamp = "20231027100000"
        save_dir = MagicMock(spec=Path) # Mock the save_dir Path object passed in
        save_dir.is_dir.return_value = True

        mock_temp_dir = MagicMock(spec=Path)
        mock_chunk_file1 = MagicMock(spec=Path)
        mock_chunk_file2 = MagicMock(spec=Path)
        mock_final_save_path = MagicMock(spec=Path)

        # Configure side_effect for save_dir.__truediv__ to return temp_dir then final_save_path
        save_dir.__truediv__.side_effect = [mock_temp_dir, mock_final_save_path]
        # Configure side_effect for temp_dir.__truediv__ to return chunk file paths
        mock_temp_dir.__truediv__.side_effect = [mock_chunk_file1, mock_chunk_file2]

        # Mock DataFrames for chunks
        mock_chunk1 = MagicMock(spec=pd.DataFrame)
        mock_chunk2 = MagicMock(spec=pd.DataFrame)
        mock_chunks_iterator = iter([mock_chunk1, mock_chunk2])

        # Configure pd.read_sql to return the iterator
        MockPandas.read_sql.return_value = mock_chunks_iterator

        # Call the function
        result_path = _get_records(
            conn=mock_conn,
            table_name=table_name,
            sql=sql,
            timestamp=timestamp,
            save_dir=save_dir,
            debug=True, # Debug is True
            chunksize=100000 # Example chunksize
        )

        # Assertions
        save_dir.is_dir.assert_called_once()
        save_dir.__truediv__.assert_any_call("temp")
        mock_temp_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        MockLogging.debug.assert_called_once_with(f"Executing Query: {sql}")

        MockPandas.read_sql.assert_called_once_with(
            sql=sql,
            con=mock_conn,
            chunksize=100000,
            dtype_backend="pyarrow",
        )

        # Check chunk processing calls
        mock_temp_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}_0.parquet")
        mock_chunk1.to_parquet.assert_called_once_with(mock_chunk_file1)
        mock_temp_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}_1.parquet")
        mock_chunk2.to_parquet.assert_called_once_with(mock_chunk_file2)

        MockLogging.info.assert_any_call(f"Generated 2 chunk files.")

        save_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}.parquet")
        MockCombineChunks.assert_called_once_with([mock_chunk_file1, mock_chunk_file2], mock_final_save_path)

        MockOsRemove.assert_not_called() # debug=True, so no removal

        self.assertEqual(result_path, mock_final_save_path) # Check return value

    @patch('load_db.logging')
    @patch('load_db.combine_chunks')
    @patch('load_db.os.remove')
    @patch('load_db.pd') # Patch pandas
    def test__get_records_success_debug_false(self, MockPandas, MockOsRemove, MockCombineChunks, MockLogging):
        """Test _get_records successfully fetches and combines chunks with debug=False (removes temp files)."""
        # Setup mocks
        mock_conn = MagicMock()
        table_name = "TEST_TABLE"
        sql = "SELECT * FROM TEST_TABLE"
        timestamp = "20231027100000"
        save_dir = MagicMock(spec=Path)
        save_dir.is_dir.return_value = True

        mock_temp_dir = MagicMock(spec=Path)
        mock_chunk_file1 = MagicMock(spec=Path)
        mock_chunk_file2 = MagicMock(spec=Path)
        mock_final_save_path = MagicMock(spec=Path)

        save_dir.__truediv__.side_effect = [mock_temp_dir, mock_final_save_path]
        mock_temp_dir.__truediv__.side_effect = [mock_chunk_file1, mock_chunk_file2]

        mock_chunk1 = MagicMock(spec=pd.DataFrame)
        mock_chunk2 = MagicMock(spec=pd.DataFrame)
        mock_chunks_iterator = iter([mock_chunk1, mock_chunk2])
        MockPandas.read_sql.return_value = mock_chunks_iterator

        # Call the function
        result_path = _get_records(
            conn=mock_conn,
            table_name=table_name,
            sql=sql,
            timestamp=timestamp,
            save_dir=save_dir,
            debug=False, # Debug is False
            chunksize=100000
        )

        # Assertions (similar to above, but check os.remove)
        save_dir.is_dir.assert_called_once()
        save_dir.__truediv__.assert_any_call("temp")
        mock_temp_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        MockLogging.debug.assert_called_once_with(f"Executing Query: {sql}")

        MockPandas.read_sql.assert_called_once_with(
            sql=sql,
            con=mock_conn,
            chunksize=100000,
            dtype_backend="pyarrow",
        )

        mock_temp_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}_0.parquet")
        mock_chunk1.to_parquet.assert_called_once_with(mock_chunk_file1)
        # Assuming the file structure allows this import
        # If not, adjust the import path accordingly
        from load_db import (
            _get_records,
            get_all_records,
            get_new_records,
            fetch_new_records,
            get_all_tables,
            get_new_records1,
            combine_chunks,
        )

        # Assume TestCase class is already defined and imported

        class TestLoadDb(unittest.TestCase): # Assuming this class exists

            # Existing tests for _get_records and get_all_tables would go here
            # ... (previous tests) ...

            @patch('load_db._get_records')
            def test_get_all_records(self, MockGetRecords):
                """Test get_all_records calls _get_records with correct parameters."""
                mock_conn = MagicMock()
                table_name = "TEST_TABLE"
                schema = "TEST_SCHEMA"
                timestamp = "20231027100000"
                save_dir = MagicMock(spec=Path)
                debug = True

                expected_sql = f"SELECT * FROM {schema}.{table_name}"
                mock_return_path = MagicMock(spec=Path)
                MockGetRecords.return_value = mock_return_path

                result = get_all_records(mock_conn, table_name, schema, timestamp, save_dir, debug)

                MockGetRecords.assert_called_once_with(
                    conn=mock_conn,
                    table_name=table_name,
                    sql=expected_sql,
                    timestamp=timestamp,
                    save_dir=save_dir,
                    debug=debug,
                )
                self.assertEqual(result, mock_return_path)

            @patch('load_db._get_records')
            @patch('load_db.logging')
            def test_get_new_records(self, MockLogging, MockGetRecords):
                """Test get_new_records calls _get_records with correct parameters."""
                mock_conn = MagicMock()
                table_name = "TEST_TABLE"
                time_column = "LAST_UPDATE_TIME"
                time_value = "2023-10-27 10:00:00"
                timestamp = "20231027100000"
                schema = "TEST_SCHEMA"
                save_dir = MagicMock(spec=Path)
                debug = False
                chunksize = 150000 # Override default

                expected_sql = f"SELECT * FROM {schema}.{table_name} WHERE {time_column} = :time_value"
                mock_return_path = MagicMock(spec=Path)
                MockGetRecords.return_value = mock_return_path

                result = get_new_records(
                    conn=mock_conn,
                    table_name=table_name,
                    time_column=time_column,
                    time_value=time_value,
                    timestamp=timestamp,
                    schema=schema,
                    save_dir=save_dir,
                    debug=debug,
                    chunksize=chunksize,
                )

                MockLogging.info.assert_called_once_with(f"Fetching new records for {schema}.{table_name} at {time_value}.")
                MockGetRecords.assert_called_once_with(
                    conn=mock_conn,
                    table_name=table_name,
                    sql=expected_sql,
                    timestamp=timestamp,
                    save_dir=save_dir,
                    chunksize=chunksize,
                    debug=debug,
                )
                self.assertEqual(result, mock_return_path)

            @patch('load_db.pd.read_sql')
            @patch('load_db.logging')
            def test_fetch_new_records_by_rowid_success(self, MockLogging, MockReadSql):
                """Test fetch_new_records fetches records by ROWID successfully."""
                mock_conn = MagicMock()
                ids = ["AAAA1aAAFAAAABaAAA", "AAAA1aAAFAAAABaAAB"]
                full_table_name = "TEST_SCHEMA.TEST_TABLE"
                mock_df = MagicMock(spec=pd.DataFrame)
                MockReadSql.return_value = mock_df

                result_df = fetch_new_records(mock_conn, ids, full_table_name)

                expected_query = f"SELECT * FROM {full_table_name} WHERE ROWID IN (:1, :2)"
                MockReadSql.assert_called_once_with(expected_query, con=mock_conn, params=ids)
                MockLogging.error.assert_not_called()
                self.assertEqual(result_df, mock_df)

            def test_fetch_new_records_by_rowid_empty_ids(self):
                """Test fetch_new_records raises AssertionError for empty ID list."""
                mock_conn = MagicMock()
                ids = []
                full_table_name = "TEST_SCHEMA.TEST_TABLE"

                with self.assertRaises(AssertionError) as cm:
                    fetch_new_records(mock_conn, ids, full_table_name)

                self.assertIn("ID list cannot be empty.", str(cm.exception))

            @patch('load_db.pd.read_sql')
            @patch('load_db.logging')
            def test_fetch_new_records_by_rowid_error(self, MockLogging, MockReadSql):
                """Test fetch_new_records handles database error during fetch."""
                mock_conn = MagicMock()
                ids = ["AAAA1aAAFAAAABaAAA"]
                full_table_name = "TEST_SCHEMA.TEST_TABLE"
                mock_db_error = oracledb.DatabaseError("Simulated ROWID fetch error")
                MockReadSql.side_effect = mock_db_error

                with self.assertRaises(oracledb.DatabaseError) as cm:
                    fetch_new_records(mock_conn, ids, full_table_name)

                self.assertIn("Simulated ROWID fetch error", str(cm.exception))
                expected_query = f"SELECT * FROM {full_table_name} WHERE ROWID IN (:1)"
                MockReadSql.assert_called_once_with(expected_query, con=mock_conn, params=ids)
                MockLogging.error.assert_called_once_with(f"Error fetching records by ROWID: {mock_db_error}")

            @patch('load_db.pd.read_sql')
            @patch('load_db.logging')
            def test_get_new_records_by_key_success(self, MockLogging, MockReadSql):
                """Test get_new_records1 fetches records by key successfully."""
                mock_conn = MagicMock()
                key_col = "ID"
                last_key = "12345"
                full_table_name = "TEST_SCHEMA.TEST_TABLE"
                mock_df = MagicMock(spec=pd.DataFrame)
                MockReadSql.return_value = mock_df

                result_df = get_new_records1(mock_conn, key_col, last_key, full_table_name)

                expected_query = f"SELECT * FROM {full_table_name} WHERE {key_col} > :last_key"
                MockReadSql.assert_called_once_with(expected_query, con=mock_conn, params={"last_key": last_key})
                MockLogging.error.assert_not_called()
                self.assertEqual(result_df, mock_df)

            @patch('load_db.pd.read_sql')
            @patch('load_db.logging')
            def test_get_new_records_by_key_error(self, MockLogging, MockReadSql):
                """Test get_new_records1 handles database error during fetch."""
                mock_conn = MagicMock()
                key_col = "ID"
                last_key = "12345"
                full_table_name = "TEST_SCHEMA.TEST_TABLE"
                mock_db_error = oracledb.DatabaseError("Simulated key fetch error")
                MockReadSql.side_effect = mock_db_error

                with self.assertRaises(oracledb.DatabaseError) as cm:
                    get_new_records1(mock_conn, key_col, last_key, full_table_name)

                self.assertIn("Simulated key fetch error", str(cm.exception))
                expected_query = f"SELECT * FROM {full_table_name} WHERE {key_col} > :last_key"
                MockReadSql.assert_called_once_with(expected_query, con=mock_conn, params={"last_key": last_key})
                MockLogging.error.assert_called_once_with(f"Error fetching new records by key: {mock_db_error}")

            @patch('load_db.pd.read_parquet')
            @patch('load_db.pd.concat')
            @patch('load_db.logging')
            def test_combine_chunks_success(self, MockLogging, MockConcat, MockReadParquet):
                """Test combine_chunks successfully combines files."""
                temp_files = [MagicMock(spec=Path), MagicMock(spec=Path)]
                save_path = MagicMock(spec=Path)

                mock_df1 = MagicMock(spec=pd.DataFrame)
                mock_df2 = MagicMock(spec=pd.DataFrame)
                MockReadParquet.side_effect = [mock_df1, mock_df2]

                mock_combined_df = MagicMock(spec=pd.DataFrame)
                MockConcat.return_value = mock_combined_df

                combine_chunks(temp_files, save_path)

                MockReadParquet.assert_has_calls([
                    call(temp_files[0], dtype_backend="pyarrow", engine="pyarrow"),
                    call(temp_files[1], dtype_backend="pyarrow", engine="pyarrow"),
                ])
                MockConcat.assert_called_once_with([mock_df1, mock_df2], ignore_index=True)
                mock_combined_df.to_parquet.assert_called_once_with(save_path, engine="pyarrow", coerce_timestamps="ms")
                MockLogging.info.assert_called_once_with(f"Combined chunks saved to {save_path}")
                MockLogging.error.assert_not_called()

            @patch('load_db.pd.read_parquet')
            @patch('load_db.pd.concat')
            @patch('load_db.logging')
            def test_combine_chunks_read_error(self, MockLogging, MockConcat, MockReadParquet):
                """Test combine_chunks handles error during reading parquet files."""
                temp_files = [MagicMock(spec=Path), MagicMock(spec=Path)]
                save_path = MagicMock(spec=Path)

                mock_read_error = IOError("Simulated read error")
                MockReadParquet.side_effect = mock_read_error

                with self.assertRaises(IOError) as cm:
                    combine_chunks(temp_files, save_path)

                self.assertIn("Simulated read error", str(cm.exception))
                MockReadParquet.assert_called_once_with(temp_files[0], dtype_backend="pyarrow", engine="pyarrow") # Error on first read
                MockConcat.assert_not_called()
                MockLogging.info.assert_not_called()
                MockLogging.error.assert_called_once_with(f"Error combining chunks: {mock_read_error}")

            @patch('load_db.pd.read_parquet')
            @patch('load_db.pd.concat')
            @patch('load_db.logging')
            def test_combine_chunks_concat_error(self, MockLogging, MockConcat, MockReadParquet):
                """Test combine_chunks handles error during concatenation."""
                temp_files = [MagicMock(spec=Path), MagicMock(spec=Path)]
                save_path = MagicMock(spec=Path)

                mock_df1 = MagicMock(spec=pd.DataFrame)
                mock_df2 = MagicMock(spec=pd.DataFrame)
                MockReadParquet.side_effect = [mock_df1, mock_df2]

                mock_concat_error = Exception("Simulated concat error")
                MockConcat.side_effect = mock_concat_error

                with self.assertRaises(Exception) as cm:
                    combine_chunks(temp_files, save_path)

                self.assertIn("Simulated concat error", str(cm.exception))
                MockReadParquet.assert_has_calls([
                    call(temp_files[0], dtype_backend="pyarrow", engine="pyarrow"),
                    call(temp_files[1], dtype_backend="pyarrow", engine="pyarrow"),
                ])
                MockConcat.assert_called_once_with([mock_df1, mock_df2], ignore_index=True) # Error on concat
                MockLogging.info.assert_not_called()
                MockLogging.error.assert_called_once_with(f"Error combining chunks: {mock_concat_error}")

            @patch('load_db.pd.read_parquet')
            @patch('load_db.pd.concat')
            @patch('load_db.logging')
            def test_combine_chunks_write_error(self, MockLogging, MockConcat, MockReadParquet):
                """Test combine_chunks handles error during writing the combined file."""
                temp_files = [MagicMock(spec=Path), MagicMock(spec=Path)]
                save_path = MagicMock(spec=Path)

                mock_df1 = MagicMock(spec=pd.DataFrame)
                mock_df2 = MagicMock(spec=pd.DataFrame)
                MockReadParquet.side_effect = [mock_df1, mock_df2]

                mock_combined_df = MagicMock(spec=pd.DataFrame)
                MockConcat.return_value = mock_combined_df

                mock_write_error = IOError("Simulated write error")
                mock_combined_df.to_parquet.side_effect = mock_write_error

                with self.assertRaises(IOError) as cm:
                    combine_chunks(temp_files, save_path)

                self.assertIn("Simulated write error", str(cm.exception))
                MockReadParquet.assert_has_calls([
                    call(temp_files[0], dtype_backend="pyarrow", engine="pyarrow"),
                    call(temp_files[1], dtype_backend="pyarrow", engine="pyarrow"),
                ])
                MockConcat.assert_called_once_with([mock_df1, mock_df2], ignore_index=True)
                mock_combined_df.to_parquet.assert_called_once_with(save_path, engine="pyarrow", coerce_timestamps="ms") # Error on write
                MockLogging.info.assert_not_called()
                MockLogging.error.assert_called_once_with(f"Error combining chunks: {mock_write_error}")

            # Add the existing tests for _get_records and get_all_tables here if needed
            # ... (paste the previous tests here) ...

        if __name__ == '__main__':
            unittest.main()

        # Call the function (test with debug=False to check removal)
        result_path = _get_records(
            conn=mock_conn,
            table_name=table_name,
            sql=sql,
            timestamp=timestamp,
            save_dir=save_dir,
            debug=False, # Debug is False
            chunksize=100000
        )

        # Assertions
        save_dir.is_dir.assert_called_once()
        save_dir.__truediv__.assert_any_call("temp")
        mock_temp_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        MockLogging.debug.assert_called_once_with(f"Executing Query: {sql}")

        MockPandas.read_sql.assert_called_once_with(
            sql=sql,
            con=mock_conn,
            chunksize=100000,
            dtype_backend="pyarrow",
        )

        mock_temp_dir.__truediv__.assert_called_once_with(f"{table_name}_{timestamp}_0.parquet")
        mock_chunk1.to_parquet.assert_called_once_with(mock_chunk_file1)

        MockLogging.info.assert_any_call(f"Generated 1 chunk file.")

        save_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}.parquet")
        MockCombineChunks.assert_called_once_with([mock_chunk_file1], mock_final_save_path)

        # Check os.remove calls
        MockOsRemove.assert_called_once_with(mock_chunk_file1)

        self.assertEqual(result_path, mock_final_save_path)

    @patch('load_db.logging')
    @patch('load_db.combine_chunks')
    @patch('load_db.os.remove')
    @patch('load_db.pd') # Patch pandas
    def test__get_records_save_dir_not_dir_assertion(self, MockPandas, MockOsRemove, MockCombineChunks, MockLogging):
        """Test _get_records raises AssertionError if save_dir is not a directory."""
        # Setup mocks
        mock_conn = MagicMock()
        table_name = "TEST_TABLE"
        sql = "SELECT * FROM TEST_TABLE"
        timestamp = "20231027100000"
        save_dir = MagicMock(spec=Path)
        save_dir.is_dir.return_value = False # save_dir is NOT a directory

        # Call the function and assert AssertionError
        with self.assertRaises(AssertionError) as cm:
             _get_records(
                conn=mock_conn,
                table_name=table_name,
                sql=sql,
                timestamp=timestamp,
                save_dir=save_dir,
                debug=True,
                chunksize=100000
            )

        self.assertIn("Save directory", str(cm.exception))
        save_dir.is_dir.assert_called_once() # Check assertion is checked

        # Ensure no further calls were made
        save_dir.__truediv__.assert_not_called()
        MockPandas.read_sql.assert_not_called()
        MockCombineChunks.assert_not_called()
        MockOsRemove.assert_not_called()
        MockLogging.debug.assert_not_called() # No logging before assertion

    @patch('load_db.logging')
    @patch('load_db.combine_chunks')
    @patch('load_db.os.remove')
    @patch('load_db.pd') # Patch pandas
    def test__get_records_read_sql_error(self, MockPandas, MockOsRemove, MockCombineChunks, MockLogging):
        """Test _get_records handles exception during pd.read_sql."""
        # Setup mocks
        mock_conn = MagicMock()
        table_name = "TEST_TABLE"
        sql = "SELECT * FROM TEST_TABLE"
        timestamp = "20231027100000"
        save_dir = MagicMock(spec=Path)
        save_dir.is_dir.return_value = True

        mock_temp_dir = MagicMock(spec=Path)
        save_dir.__truediv__.return_value = mock_temp_dir # Mock save_dir / "temp"

        # Configure pd.read_sql to raise an exception
        mock_db_error = oracledb.DatabaseError("Simulated read_sql error")
        MockPandas.read_sql.side_effect = mock_db_error

        # Call the function and assert the exception is re-raised
        with self.assertRaises(oracledb.DatabaseError) as cm:
             _get_records(
                conn=mock_conn,
                table_name=table_name,
                sql=sql,
                timestamp=timestamp,
                save_dir=save_dir,
                debug=True,
                chunksize=100000
            )

        self.assertIn("Simulated read_sql error", str(cm.exception))

        # Assert calls up to the error point
        save_dir.is_dir.assert_called_once()
        save_dir.__truediv__.assert_any_call("temp")
        mock_temp_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        MockLogging.debug.assert_called_once_with(f"Executing Query: {sql}")
        MockPandas.read_sql.assert_called_once_with(
            sql=sql,
            con=mock_conn,
            chunksize=100000,
            dtype_backend="pyarrow",
        )

        # Ensure subsequent calls were NOT made
        MockCombineChunks.assert_not_called()
        MockOsRemove.assert_not_called()
        MockLogging.error.assert_called_once_with(f"Error during record fetching: {mock_db_error}")


    @patch('load_db.logging')
    @patch('load_db.combine_chunks')
    @patch('load_db.os.remove')
    @patch('load_db.pd') # Patch pandas
    def test__get_records_to_parquet_error(self, MockPandas, MockOsRemove, MockCombineChunks, MockLogging):
        """Test _get_records handles exception during chunk.to_parquet."""
        # Setup mocks
        mock_conn = MagicMock()
        table_name = "TEST_TABLE"
        sql = "SELECT * FROM TEST_TABLE"
        timestamp = "20231027100000"
        save_dir = MagicMock(spec=Path)
        save_dir.is_dir.return_value = True

        mock_temp_dir = MagicMock(spec=Path)
        save_dir.__truediv__.return_value = mock_temp_dir # Mock save_dir / "temp"

        # Mock DataFrames for chunks
        mock_chunk1 = MagicMock(spec=pd.DataFrame)
        mock_chunk2 = MagicMock(spec=pd.DataFrame) # This chunk will cause the error
        mock_chunks_iterator = iter([mock_chunk1, mock_chunk2])
        MockPandas.read_sql.return_value = mock_chunks_iterator

        # Mock chunk file paths
        mock_chunk_file1 = MagicMock(spec=Path)
        mock_chunk_file2 = MagicMock(spec=Path)
        mock_temp_dir.__truediv__.side_effect = [mock_chunk_file1, mock_chunk_file2]

        # Configure mock_chunk2.to_parquet to raise an exception
        mock_io_error = IOError("Simulated parquet write error")
        mock_chunk2.to_parquet.side_effect = mock_io_error

        # Call the function and assert the exception is re-raised
        with self.assertRaises(IOError) as cm:
             _get_records(
                conn=mock_conn,
                table_name=table_name,
                sql=sql,
                timestamp=timestamp,
                save_dir=save_dir,
                debug=True,
                chunksize=100000
            )

        self.assertIn("Simulated parquet write error", str(cm.exception))

        # Assert calls up to the error point
        save_dir.is_dir.assert_called_once()
        save_dir.__truediv__.assert_any_call("temp")
        mock_temp_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        MockLogging.debug.assert_called_once_with(f"Executing Query: {sql}")
        MockPandas.read_sql.assert_called_once_with(
            sql=sql,
            con=mock_conn,
            chunksize=100000,
            dtype_backend="pyarrow",
        )
        mock_temp_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}_0.parquet")
        mock_chunk1.to_parquet.assert_called_once_with(mock_chunk_file1)
        mock_temp_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}_1.parquet")
        mock_chunk2.to_parquet.assert_called_once_with(mock_chunk_file2) # This call raises the error

        # Ensure subsequent calls were NOT made
        MockCombineChunks.assert_not_called()
        MockOsRemove.assert_not_called()
        MockLogging.error.assert_called_once_with(f"Error during record fetching: {mock_io_error}")

    @patch('load_db.logging')
    @patch('load_db.combine_chunks')
    @patch('load_db.os.remove')
    @patch('load_db.pd') # Patch pandas
    def test__get_records_combine_chunks_error(self, MockPandas, MockOsRemove, MockCombineChunks, MockLogging):
        """Test _get_records handles exception during combine_chunks."""
        # Setup mocks
        mock_conn = MagicMock()
        table_name = "TEST_TABLE"
        sql = "SELECT * FROM TEST_TABLE"
        timestamp = "20231027100000"
        save_dir = MagicMock(spec=Path)
        save_dir.is_dir.return_value = True

        mock_temp_dir = MagicMock(spec=Path)
        mock_chunk_file1 = MagicMock(spec=Path)
        mock_chunk_file2 = MagicMock(spec=Path)
        mock_final_save_path = MagicMock(spec=Path)

        save_dir.__truediv__.side_effect = [mock_temp_dir, mock_final_save_path]
        mock_temp_dir.__truediv__.side_effect = [mock_chunk_file1, mock_chunk_file2]

        mock_chunk1 = MagicMock(spec=pd.DataFrame)
        mock_chunk2 = MagicMock(spec=pd.DataFrame)
        mock_chunks_iterator = iter([mock_chunk1, mock_chunk2])
        MockPandas.read_sql.return_value = mock_chunks_iterator

        # Configure combine_chunks to raise an exception
        mock_combine_error = Exception("Simulated combine error")
        MockCombineChunks.side_effect = mock_combine_error

        # Call the function and assert the exception is re-raised
        with self.assertRaises(Exception) as cm:
             _get_records(
                conn=mock_conn,
                table_name=table_name,
                sql=sql,
                timestamp=timestamp,
                save_dir=save_dir,
                debug=True,
                chunksize=100000
            )

        self.assertIn("Simulated combine error", str(cm.exception))

        # Assert calls up to the error point
        save_dir.is_dir.assert_called_once()
        save_dir.__truediv__.assert_any_call("temp")
        mock_temp_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        MockLogging.debug.assert_called_once_with(f"Executing Query: {sql}")
        MockPandas.read_sql.assert_called_once_with(
            sql=sql,
            con=mock_conn,
            chunksize=100000,
            dtype_backend="pyarrow",
        )
        mock_temp_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}_0.parquet")
        mock_chunk1.to_parquet.assert_called_once_with(mock_chunk_file1)
        mock_temp_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}_1.parquet")
        mock_chunk2.to_parquet.assert_called_once_with(mock_chunk_file2)
        MockLogging.info.assert_any_call(f"Generated 2 chunk files.")
        save_dir.__truediv__.assert_any_call(f"{table_name}_{timestamp}.parquet")
        MockCombineChunks.assert_called_once_with([mock_chunk_file1, mock_chunk_file2], mock_final_save_path) # This call raises the error

        # Ensure subsequent calls were NOT made
        MockOsRemove.assert_not_called()
        MockLogging.error.assert_called_once_with(f"Error during record fetching: {mock_combine_error}")