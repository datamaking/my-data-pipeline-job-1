from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
from pyspark.sql import SparkSession, DataFrame


class BaseSource(ABC):
    """Base class for all data sources."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the base source.
        
        Args:
            spark: SparkSession instance
            config: Source configuration
        """
        self.spark = spark
        self.config = config
    
    @abstractmethod
    def read(self) -> DataFrame:
        """
        Read data from the source.
        
        Returns:
            DataFrame: PySpark DataFrame containing the source data
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate the connection to the source.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        pass
    
    def join_dataframes(self, dataframes: List[DataFrame], join_columns: List[str], join_type: str = "inner") -> DataFrame:
        """
        Join multiple dataframes.
        
        Args:
            dataframes: List of dataframes to join
            join_columns: Columns to join on
            join_type: Type of join (inner, outer, left, right)
            
        Returns:
            DataFrame: Joined dataframe
        """
        if not dataframes:
            raise ValueError("No dataframes provided for join operation")
        
        if len(dataframes) == 1:
            return dataframes[0]
        
        result_df = dataframes[0]
        for df in dataframes[1:]:
            result_df = result_df.join(df, on=join_columns, how=join_type)
        
        return result_df