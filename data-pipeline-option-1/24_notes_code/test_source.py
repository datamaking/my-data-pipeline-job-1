import unittest
from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession, DataFrame
from source.base_source import BaseSource
from source.source_factory import SourceFactory
from source.file_source import FileSource


class TestSourceFactory(unittest.TestCase):
    """Test cases for SourceFactory."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a mock SparkSession
        self.mock_spark = MagicMock(spec=SparkSession)
        
        # Create a mock config
        self.mock_config = {"key": "value"}
    
    @patch("source.source_factory.FileSource")
    def test_create_file_source(self, mock_file_source):
        """Test creating a file source."""
        # Create a source
        SourceFactory.create_source("file", self.mock_spark, self.mock_config)
        
        # Assert that the correct source was created
        mock_file_source.assert_called_once_with(self.mock_spark, self.mock_config)
    
    @patch("source.source_factory.RDBMSSource")
    def test_create_rdbms_source(self, mock_rdbms_source):
        """Test creating an RDBMS source."""
        # Create a source
        SourceFactory.create_source("rdbms", self.mock_spark, self.mock_config)
        
        # Assert that the correct source was created
        mock_rdbms_source.assert_called_once_with(self.mock_spark, self.mock_config)
    
    def test_create_invalid_source(self):
        """Test creating an invalid source."""
        # Assert that creating an invalid source raises an exception
        with self.assertRaises(ValueError):
            SourceFactory.create_source("invalid", self.mock_spark, self.mock_config)


class TestFileSource(unittest.TestCase):
    """Test cases for FileSource."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a mock SparkSession
        self.mock_spark = MagicMock(spec=SparkSession)
        
        # Create a mock DataFrame
        self.mock_df = MagicMock(spec=DataFrame)
        
        # Create a mock reader
        self.mock_reader = MagicMock()
        self.mock_reader.option.return_value = self.mock_reader
        self.mock_reader.load.return_value = self.mock_df
        
        # Configure the mock SparkSession
        self.mock_spark.read.format.return_value = self.mock_reader
        
        # Create a config
        self.config = {
            "file_paths": ["/path/to/file1.csv", "/path/to/file2.csv"],
            "file_format": "csv",
            "options": {"header": "true", "delimiter": ","}
        }
        
        # Create a file source
        self.file_source = FileSource(self.mock_spark, self.config)
    
    @patch("os.path.exists")
    def test_validate_connection(self, mock_exists):
        """Test validating the connection."""
        # Configure the mock
        mock_exists.return_value = True
        
        # Validate the connection
        result = self.file_source.validate_connection()
        
        # Assert that the connection is valid
        self.assertTrue(result)
        
        # Assert that os.path.exists was called for each file path
        mock_exists.assert_any_call("/path/to/file1.csv")
        mock_exists.assert_any_call("/path/to/file2.csv")
    
    @patch("os.path.exists")
    def test_validate_connection_invalid(self, mock_exists):
        """Test validating an invalid connection."""
        # Configure the mock
        mock_exists.return_value = False
        
        # Validate the connection
        result = self.file_source.validate_connection()
        
        # Assert that the connection is invalid
        self.assertFalse(result)
    
    def test_read_single_file(self):
        """Test reading a single file."""
        # Configure the file source to have a single file path
        self.file_source.file_paths = ["/path/to/file.csv"]
        
        # Read the file
        result = self.file_source.read()
        
        # Assert that the result is the mock DataFrame
        self.assertEqual(result, self.mock_df)
        
        # Assert that the reader was configured correctly
        self.mock_spark.read.format.assert_called_once_with("csv")
        self.mock_reader.option.assert_any_call("header", "true")
        self.mock_reader.option.assert_any_call("delimiter", ",")
        self.mock_reader.load.assert_called_once_with("/path/to/file.csv")
    
    def test_read_multiple_files(self):
        """Test reading multiple files."""
        # Configure the mock reader to return multiple DataFrames
        mock_df1 = MagicMock(spec=DataFrame)
        mock_df2 = MagicMock(spec=DataFrame)
        self.mock_reader.load.side_effect = [mock_df1, mock_df2]
        
        # Configure the mock DataFrames for union
        mock_df1.union.return_value = self.mock_df
        
        # Read the files
        result = self.file_source.read()
        
        # Assert that the result is the mock DataFrame
        self.assertEqual(result, self.mock_df)
        
        # Assert that the reader was configured correctly
        self.mock_spark.read.format.assert_called_with("csv")
        self.mock_reader.load.assert_any_call("/path/to/file1.csv")
        self.mock_reader.load.assert_any_call("/path/to/file2.csv")
        mock_df1.union.assert_called_once_with(mock_df2)
    
    def test_read_no_file_paths(self):
        """Test reading with no file paths."""
        # Configure the file source to have no file paths
        self.file_source.file_paths = []
        
        # Assert that reading with no file paths raises an exception
        with self.assertRaises(ValueError):
            self.file_source.read()


if __name__ == "__main__":
    unittest.main()