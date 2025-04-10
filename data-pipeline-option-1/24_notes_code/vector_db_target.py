from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct

from .base_target import BaseTarget


class VectorDBTarget(BaseTarget):
    """Target for writing data to vector databases."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the vector database target.
        
        Args:
            config: Target configuration with vector database connection details
        """
        super().__init__(config)
        self.db_type = config.get("db_type")
        self.connection_params = config.get("connection_params", {})
        self.collection = config.get("collection")
        self.vector_column = config.get("vector_column")
        self.metadata_columns = config.get("metadata_columns", [])
        self.mode = config.get("mode", "overwrite")
        self.options = config.get("options", {})
    
    def write(self, df: DataFrame) -> None:
        """
        Write data to the vector database target.
        
        Args:
            df: Input dataframe to write
        """
        if not self.db_type or not self.collection:
            raise ValueError("Database type and collection must be provided")
        
        if self.db_type == "chroma":
            self._write_chroma(df)
        elif self.db_type == "postgres":
            self._write_postgres(df)
        elif self.db_type == "neo4j":
            self._write_neo4j(df)
        else:
            raise ValueError(f"Unsupported vector database type: {self.db_type}")
    
    def validate_connection(self) -> bool:
        """
        Validate the connection to the vector database.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        try:
            if self.db_type == "chroma":
                # For ChromaDB, we'll check if the path exists
                import os
                path = self.connection_params.get("path")
                return os.path.exists(path) if path else False
            
            elif self.db_type == "postgres":
                # For PostgreSQL, we'll try to establish a JDBC connection
                jdbc_url = f"jdbc:postgresql://{self.connection_params.get('host')}:{self.connection_params.get('port')}/{self.connection_params.get('database')}"
                conn_properties = {
                    "driver": "org.postgresql.Driver",
                    "user": self.connection_params.get("user"),
                    "password": self.connection_params.get("password")
                }
                
                # Create a small test dataframe
                test_df = DataFrame.sparkSession.createDataFrame([("test",)], ["col1"])
                
                # Try to write to a temporary table
                test_table = "connection_test_" + str(int(time.time()))
                test_df.write.jdbc(
                    url=jdbc_url,
                    table=test_table,
                    mode="overwrite",
                    properties=conn_properties
                )
                
                return True
            
            elif self.db_type == "neo4j":
                # For Neo4j, we'll try to establish a connection using the Neo4j connector
                options = {
                    "url": self.connection_params.get("uri"),
                    "authentication.basic.username": self.connection_params.get("user"),
                    "authentication.basic.password": self.connection_params.get("password")
                }
                
                # Create a small test dataframe
                test_df = DataFrame.sparkSession.createDataFrame([("test",)], ["col1"])
                
                # Try to write to a temporary node
                test_df.write.format("org.neo4j.spark.DataSource") \
                    .options(**options) \
                    .option("labels", "ConnectionTest") \
                    .mode("overwrite") \
                    .save()
                
                return True
            
            return False
        except Exception as e:
            print(f"Connection validation failed: {str(e)}")
            return False
    
    def _write_chroma(self, df: DataFrame) -> None:
        """
        Write data to ChromaDB.
        
        Args:
            df: Input dataframe to write
        """
        # ChromaDB doesn't have a direct Spark connector, so we'll use Python API
        import chromadb
        from chromadb.config import Settings
        
        # Create a client
        client = chromadb.Client(Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory=self.connection_params.get("path")
        ))
        
        # Get or create collection
        collection = client.get_or_create_collection(name=self.collection)
        
        # Prepare data for ChromaDB
        # We need to collect the data to Python
        rows = df.collect()
        
        # Extract IDs, embeddings, documents, and metadata
        ids = []
        embeddings = []
        documents = []
        metadatas = []
        
        for row