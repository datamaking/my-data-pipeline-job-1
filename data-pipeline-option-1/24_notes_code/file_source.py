import os
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame

from .base_source import BaseSource


class FileSource(BaseSource):
    """Source for reading data from files."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the file source.
        
        Args:
            spark: SparkSession instance
            config: Source configuration with file paths and options
        """
        super().__init__(spark, config)
        self.file_paths = config.get("file_paths", [])
        self.file_format = config.get("file_format", "csv")
        self.options = config.get("options", {})
    
    def read(self) -> DataFrame:
        """
        Read data from the file source.
        
        Returns:
            DataFrame: PySpark DataFrame containing the file data
        """
        if not self.file_paths:
            raise ValueError("No file paths provided")
        
        # Create a reader with options
        reader = self.spark.read.format(self.file_format)
        for key, value in self.options.items():
            reader = reader.option(key, value)
        
        # Read all files and union them if multiple files
        if len(self.file_paths) == 1:
            return reader.load(self.file_paths[0])
        else:
            dataframes = [reader.load(file_path) for file_path in self.file_paths]
            return self._union_dataframes(dataframes)
    
    def validate_connection(self) -> bool:
        """
        Validate that all file paths exist and are accessible.
        
        Returns:
            bool: True if all files exist and are accessible, False otherwise
        """
        for file_path in self.file_paths:
            # For local file system
            if file_path.startswith("file://") or not any(file_path.startswith(prefix) for prefix in ["hdfs://", "s3://", "gs://"]):
                local_path = file_path.replace("file://", "")
                if not os.path.exists(local_path):
                    return False
            # For HDFS, S3, etc., we'll need to check differently
            # This is a simplified check that assumes if the path format is correct, it's valid
            else:
                if not file_path:
                    return False
        
        return True
    
    def _union_dataframes(self, dataframes: List[DataFrame]) -> DataFrame:
        """
        Union multiple dataframes.
        
        Args:
            dataframes: List of dataframes to union
            
        Returns:
            DataFrame: Unioned dataframe
        """
        if not dataframes:
            raise ValueError("No dataframes provided for union operation")
        
        result_df = dataframes[0]
        for df in dataframes[1:]:
            result_df = result_df.union(df)
        
        return result_df