import os
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame

from .base_target import BaseTarget


class FileTarget(BaseTarget):
    """Target for writing data to files."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the file target.
        
        Args:
            config: Target configuration with file output details
        """
        super().__init__(config)
        self.output_dir = config.get("output_dir")
        self.file_format = config.get("file_format", "parquet")
        self.mode = config.get("mode", "overwrite")
        self.partition_by = config.get("partition_by", [])
        self.options = config.get("options", {})
    
    def write(self, df: DataFrame) -> None:
        """
        Write data to the file target.
        
        Args:
            df: Input dataframe to write
        """
        if not self.output_dir:
            raise ValueError("No output directory provided")
        
        # Create a writer with options
        writer = df.write.format(self.file_format).mode(self.mode)
        
        # Add options
        for key, value in self.options.items():
            writer = writer.option(key, value)
        
        # Add partitioning if specified
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        
        # Write the data
        writer.save(self.output_dir)
    
    def validate_connection(self) -> bool:
        """
        Validate that the output directory exists and is writable.
        
        Returns:
            bool: True if the output directory is valid, False otherwise
        """
        if not self.output_dir:
            return False
        
        # For local file system
        if self.output_dir.startswith("file://") or not any(self.output_dir.startswith(prefix) for prefix in ["hdfs://", "s3://", "gs://"]):
            local_dir = self.output_dir.replace("file://", "")
            
            # Create directory if it doesn't exist
            if not os.path.exists(local_dir):
                try:
                    os.makedirs(local_dir, exist_ok=True)
                except Exception as e:
                    print(f"Error creating directory: {str(e)}")
                    return False
            
            # Check if directory is writable
            if not os.access(local_dir, os.W_OK):
                return False
        
        # For HDFS, S3, etc., we'll need to check differently
        # This is a simplified check that assumes if the path format is correct, it's valid
        return True