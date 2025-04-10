from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from .base_source import BaseSource
from .file_source import FileSource
from .rdbms_source import RDBMSSource
from .hive_source import HiveSource
from .nosql_source import NoSQLSource
from .vector_db_source import VectorDBSource


class SourceFactory:
    """
    Factory for creating source instances.
    Implements the Factory Pattern.
    """
    
    @staticmethod
    def create_source(source_type: str, spark: SparkSession, config: Dict[str, Any]) -> BaseSource:
        """
        Create a source instance based on the source type.
        
        Args:
            source_type: Type of source to create
            spark: SparkSession instance
            config: Source configuration
            
        Returns:
            BaseSource: Instance of a source class
            
        Raises:
            ValueError: If the source type is not supported
        """
        if source_type == "file":
            return FileSource(spark, config)
        elif source_type == "rdbms":
            return RDBMSSource(spark, config)
        elif source_type == "hive":
            return HiveSource(spark, config)
        elif source_type == "nosql":
            return NoSQLSource(spark, config)
        elif source_type == "vector_db":
            return VectorDBSource(spark, config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")