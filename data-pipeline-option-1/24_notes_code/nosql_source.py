from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame

from .base_source import BaseSource


class NoSQLSource(BaseSource):
    """Source for reading data from NoSQL databases (e.g., MongoDB)."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the NoSQL source.
        
        Args:
            spark: SparkSession instance
            config: Source configuration with NoSQL connection details
        """
        super().__init__(spark, config)
        self.connection_uri = config.get("connection_uri")
        self.database = config.get("database")
        self.collections = config.get("collections", [])
        self.queries = config.get("queries", [])
        self.options = config.get("options", {})
    
    def read(self) -> DataFrame:
        """
        Read data from the NoSQL source.
        
        Returns:
            DataFrame: PySpark DataFrame containing the NoSQL data
        """
        if not self.collections:
            raise ValueError("No collections provided")
        
        dataframes = []
        
        # Read from collections with optional queries
        for i, collection in enumerate(self.collections):
            # Get the query for this collection if available
            query = self.queries[i] if i < len(self.queries) else None
            df = self._read_collection(collection, query)
            dataframes.append(df)
        
        # If there's only one dataframe, return it
        if len(dataframes) == 1:
            return dataframes[0]
        
        # If there are join columns specified, join the dataframes
        if "join_columns" in self.config and dataframes:
            join_columns = self.config.get("join_columns")
            join_type = self.config.get("join_type", "inner")
            return self.join_dataframes(dataframes, join_columns, join_type)
        
        # Otherwise, return the first dataframe
        return dataframes[0] if dataframes else None
    
    def validate_connection(self) -> bool:
        """
        Validate the connection to the NoSQL database.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        try:
            # Try to read a small amount of data to validate the connection
            if self.collections:
                options = {
                    "uri": self.connection_uri,
                    "database": self.database,
                    "collection": self.collections[0]
                }
                options.update(self.options)
                
                self.spark.read.format("mongo").options(**options).limit(1).load()
            
            return True
        except Exception as e:
            print(f"Connection validation failed: {str(e)}")
            return False
    
    def _read_collection(self, collection: str, query: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Read data from a NoSQL collection.
        
        Args:
            collection: Collection name
            query: Query document (for MongoDB)
            
        Returns:
            DataFrame: PySpark DataFrame containing the collection data
        """
        options = {
            "uri": self.connection_uri,
            "database": self.database,
            "collection": collection
        }
        
        # Add query if provided
        if query:
            options["pipeline"] = str(query)
        
        # Add any additional options
        options.update(self.options)
        
        # Read the collection
        return self.spark.read.format("mongo").options(**options).load()