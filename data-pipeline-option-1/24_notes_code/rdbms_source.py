from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame

from .base_source import BaseSource


class RDBMSSource(BaseSource):
    """Source for reading data from RDBMS databases."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the RDBMS source.
        
        Args:
            spark: SparkSession instance
            config: Source configuration with JDBC connection details
        """
        super().__init__(spark, config)
        self.jdbc_url = config.get("jdbc_url")
        self.driver = config.get("driver")
        self.user = config.get("user")
        self.password = config.get("password")
        self.tables = config.get("tables", [])
        self.queries = config.get("queries", [])
        self.options = config.get("options", {})
    
    def read(self) -> DataFrame:
        """
        Read data from the RDBMS source.
        
        Returns:
            DataFrame: PySpark DataFrame containing the database data
        """
        if not self.tables and not self.queries:
            raise ValueError("No tables or queries provided")
        
        dataframes = []
        
        # Read from tables
        for table in self.tables:
            df = self._read_table(table)
            dataframes.append(df)
        
        # Read from queries
        for i, query in enumerate(self.queries):
            df = self._read_query(query, f"query_{i}")
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
        Validate the connection to the RDBMS.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        try:
            # Try to create a JDBC connection
            conn_properties = {
                "driver": self.driver,
                "user": self.user,
                "password": self.password
            }
            
            # Add any additional options
            conn_properties.update(self.options)
            
            # Try to read a small amount of data to validate the connection
            test_query = "(SELECT 1 AS test_col) AS test_table"
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table=test_query,
                properties=conn_properties
            ).limit(1).collect()
            
            return True
        except Exception as e:
            print(f"Connection validation failed: {str(e)}")
            return False
    
    def _read_table(self, table: str) -> DataFrame:
        """
        Read data from a table.
        
        Args:
            table: Table name
            
        Returns:
            DataFrame: PySpark DataFrame containing the table data
        """
        conn_properties = {
            "driver": self.driver,
            "user": self.user,
            "password": self.password
        }
        
        # Add any additional options
        conn_properties.update(self.options)
        
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=table,
            properties=conn_properties
        )
    
    def _read_query(self, query: str, alias: str) -> DataFrame:
        """
        Read data from a SQL query.
        
        Args:
            query: SQL query
            alias: Alias for the query result
            
        Returns:
            DataFrame: PySpark DataFrame containing the query result
        """
        conn_properties = {
            "driver": self.driver,
            "user": self.user,
            "password": self.password
        }
        
        # Add any additional options
        conn_properties.update(self.options)
        
        # Wrap the query in parentheses and give it an alias
        query_with_alias = f"({query}) AS {alias}"
        
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query_with_alias,
            properties=conn_properties
        )