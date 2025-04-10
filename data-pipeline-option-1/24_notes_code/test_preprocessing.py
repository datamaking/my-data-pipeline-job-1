import unittest
from unittest.mock import patch, MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from preprocessing.base_preprocessor import BasePreprocessor
from preprocessing.preprocessing_factory import PreprocessingFactory
from preprocessing.html_parser import HTMLParser
from preprocessing.data_cleaner import DataCleaner


class TestPreprocessingFactory(unittest.TestCase):
    """Test cases for PreprocessingFactory."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a mock config
        self.mock_config = {"key": "value"}
    
    @patch("preprocessing.preprocessing_factory.HTMLParser")
    def test_create_html_parser(self, mock_html_parser):
        """Test creating an HTML parser."""
        # Create a preprocessor
        PreprocessingFactory.create_preprocessor("html_parser", self.mock_config)
        
        # Assert that the correct preprocessor was created
        mock_html_parser.assert_called_once_with(self.mock_config)
    
    @patch("preprocessing.preprocessing_factory.DataCleaner")
    def test_create_data_cleaner(self, mock_data_cleaner):
        """Test creating a data cleaner."""
        # Create a preprocessor
        PreprocessingFactory.create_preprocessor("data_cleaner", self.mock_config)
        
        # Assert that the correct preprocessor was created
        mock_data_cleaner.assert_called_once_with(self.mock_config)
    
    def test_create_invalid_preprocessor(self):
        """Test creating an invalid preprocessor."""
        # Assert that creating an invalid preprocessor raises an exception
        with self.assertRaises(ValueError):
            PreprocessingFactory.create_preprocessor("invalid", self.mock_config)
    
    @patch("preprocessing.preprocessing_factory.HTMLParser")
    @patch("preprocessing.preprocessing_factory.DataCleaner")