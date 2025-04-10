from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame

from .base_source import BaseSource


class HiveSource(BaseSource):
    """Source for reading data from Hive tables."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the Hive source.
        
        Args:
            spark: SparkSession instance
            config: Source configuration with Hive connection details
        """
        super().__init__(spark, config)
        self.metastore_uri = config.get("metastore_uri")
        self.database = config.get("database")
        self.tables = config.get("tables", [])
        self.queries = config.get("queries", [])
        self.options = config.get("options", {})
        
        # Configure Spark to use Hive metastore
        if self.metastore_uri:
            self.spark.conf.set("hive.metastore.uris", self.metastore_uri)
    
    def read(self) -> DataFrame:
        """
        Read data from the Hive source.
        
        Returns:
            DataFrame: PySpark DataFrame containing the Hive data
        """
        if not self.tables and not self.queries:
            raise ValueError("No tables or queries provided")
        
        dataframes = []
        
        # Read from tables
        for table in self.tables:
            df = self._read_table(table)
            dataframes.append(df)
        
        # Read from queries
        for query in self.queries:
            df = self._read_query(query)
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
        Validate the connection to Hive.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        try:
            # Try to execute a simple query to validate the connection
            self.spark.sql("SHOW DATABASES").limit(1).collect()
            return True
        except Exception as e:
            print(f"Connection validation failed: {str(e)}")
            return False
    
    def _read_table(self, table: str) -> DataFrame:
        """
        Read data from a Hive table.
        
        Args:
            table: Table name
            
        Returns:
            DataFrame: PySpark DataFrame containing the table data
        """
        # Construct the fully qualified table name
        full_table_name = f"{self.database}.{table}" if self.database else table
        
        # Create a reader with options
        reader = self.spark.read
        for key, value in self.options.items():
            reader = reader.option(key, value)
        
        # Read the table
        return reader.table(full_table_name)
    
    def _read_query(self, query: str) -> DataFrame:
        """
        Read data from a HQL query.
        
        Args:
            query: HQL query
            
        Returns:
            DataFrame: PySpark DataFrame containing the query result
        """
        # If a database is specified, use it
        if self.database:
            self.spark.sql(f"USE {self.database}")
        
        # Execute the query
        return self.spark.sql(query)