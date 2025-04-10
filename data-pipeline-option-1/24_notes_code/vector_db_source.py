from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
import json

from .base_source import BaseSource


class VectorDBSource(BaseSource):
    """Source for reading data from vector databases."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the vector database source.
        
        Args:
            spark: SparkSession instance
            config: Source configuration with vector database connection details
        """
        super().__init__(spark, config)
        self.db_type = config.get("db_type")
        self.connection_params = config.get("connection_params", {})
        self.collections = config.get("collections", [])
        self.queries = config.get("queries", [])
        self.options = config.get("options", {})
    
    def read(self) -> DataFrame:
        """
        Read data from the vector database source.
        
        Returns:
            DataFrame: PySpark DataFrame containing the vector database data
        """
        if not self.collections:
            raise ValueError("No collections provided")
        
        if self.db_type == "chroma":
            return self._read_chroma()
        elif self.db_type == "postgres":
            return self._read_postgres()
        elif self.db_type == "neo4j":
            return self._read_neo4j()
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
                
                self.spark.read.jdbc(
                    url=jdbc_url,
                    table="(SELECT 1 AS test_col) AS test_table",
                    properties=conn_properties
                ).limit(1).collect()
                
                return True
            
            elif self.db_type == "neo4j":
                # For Neo4j, we'll try to establish a connection using the Neo4j connector
                options = {
                    "url": self.connection_params.get("uri"),
                    "authentication.basic.username": self.connection_params.get("user"),
                    "authentication.basic.password": self.connection_params.get("password")
                }
                
                self.spark.read.format("org.neo4j.spark.DataSource") \
                    .options(**options) \
                    .option("query", "RETURN 1 as test") \
                    .load() \
                    .limit(1) \
                    .collect()
                
                return True
            
            return False
        except Exception as e:
            print(f"Connection validation failed: {str(e)}")
            return False
    
    def _read_chroma(self) -> DataFrame:
        """
        Read data from ChromaDB.
        
        Returns:
            DataFrame: PySpark DataFrame containing the ChromaDB data
        """
        # ChromaDB doesn't have a direct Spark connector, so we'll use Python API and convert to DataFrame
        import chromadb
        from chromadb.config import Settings
        
        # Create a client
        client = chromadb.Client(Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory=self.connection_params.get("path")
        ))
        
        all_data = []
        
        # Read from each collection
        for i, collection_name in enumerate(self.collections):
            collection = client.get_collection(name=collection_name)
            
            # Get the query for this collection if available
            query = self.queries[i] if i < len(self.queries) else None
            
            if query:
                # Execute query
                query_text = query.get("text")
                query_embeddings = query.get("embeddings")
                where = query.get("where", {})
                limit = query.get("limit")
                
                result = collection.query(
                    query_texts=query_text,
                    query_embeddings=query_embeddings,
                    where=where,
                    limit=limit
                )
            else:
                # Get all items
                result = collection.get()
            
            # Convert to list of dictionaries
            for i in range(len(result.get("ids", []))):
                item = {
                    "id": result["ids"][i] if "ids" in result else None,
                    "embedding": result["embeddings"][i] if "embeddings" in result else None,
                    "document": result["documents"][i] if "documents" in result else None,
                    "metadata": result["metadatas"][i] if "metadatas" in result else {}
                }
                all_data.append(item)
        
        # Convert to DataFrame
        if all_data:
            return self.spark.createDataFrame(all_data)
        else:
            # Return empty DataFrame with expected schema
            schema = "id STRING, embedding ARRAY<FLOAT>, document STRING, metadata MAP<STRING, STRING>"
            return self.spark.createDataFrame([], schema)
    
    def _read_postgres(self) -> DataFrame:
        """
        Read data from PostgreSQL with vector extension.
        
        Returns:
            DataFrame: PySpark DataFrame containing the PostgreSQL vector data
        """
        jdbc_url = f"jdbc:postgresql://{self.connection_params.get('host')}:{self.connection_params.get('port')}/{self.connection_params.get('database')}"
        conn_properties = {
            "driver": "org.postgresql.Driver",
            "user": self.connection_params.get("user"),
            "password": self.connection_params.get("password")
        }
        
        dataframes = []
        
        # Read from collections/tables with optional queries
        for i, collection in enumerate(self.collections):
            # Get the query for this collection if available
            query = self.queries[i] if i < len(self.queries) else None
            
            if query:
                # Use the provided SQL query
                sql = query.get("sql")
                df = self.spark.read.jdbc(
                    url=jdbc_url,
                    table=f"({sql}) AS query_result",
                    properties=conn_properties
                )
            else:
                # Read the entire table
                df = self.spark.read.jdbc(
                    url=jdbc_url,
                    table=collection,
                    properties=conn_properties
                )
            
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
    
    def _read_neo4j(self) -> DataFrame:
        """
        Read data from Neo4j.
        
        Returns:
            DataFrame: PySpark DataFrame containing the Neo4j data
        """
        options = {
            "url": self.connection_params.get("uri"),
            "authentication.basic.username": self.connection_params.get("user"),
            "authentication.basic.password": self.connection_params.get("password"),
            "database": self.connection_params.get("database", "neo4j")
        }
        
        dataframes = []
        
        # Read from collections/labels with optional queries
        for i, collection in enumerate(self.collections):
            # Get the query for this collection if available
            query = self.queries[i] if i < len(self.queries) else None
            
            if query:
                # Use the provided Cypher query
                cypher = query.get("cypher")
                df = self.spark.read.format("org.neo4j.spark.DataSource") \
                    .options(**options) \
                    .option("query", cypher) \
                    .load()
            else:
                # Read nodes with the given label
                cypher = f"MATCH (n:{collection}) RETURN n"
                df = self.spark.read.format("org.neo4j.spark.DataSource") \
                    .options(**options) \
                    .option("query", cypher) \
                    .load()
            
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