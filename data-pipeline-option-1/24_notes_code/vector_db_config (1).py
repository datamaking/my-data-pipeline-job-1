from typing import Dict, Any, List, Optional, Literal
from ..base_config import BaseConfig, ConfigSingleton


class VectorDBTargetConfig(BaseConfig):
    """Configuration for vector database target systems."""
    
    def __init__(
        self,
        db_type: Literal["chroma", "postgres", "neo4j"],
        connection_params: Dict[str, Any],
        collection: str,
        vector_column: Optional[str] = None,
        metadata_columns: Optional[List[str]] = None,
        mode: Literal["overwrite", "append", "upsert"] = "overwrite",
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize vector database target configuration.
        
        Args:
            db_type: Type of vector database ('chroma', 'postgres', 'neo4j')
            connection_params: Connection parameters specific to the database type
            collection: Collection/table name to write to
            vector_column: Column name for vector data (for PostgreSQL)
            metadata_columns: Columns to store as metadata
            mode: Write mode (overwrite, append, upsert)
            options: Additional options for vector database connection
        """
        self.db_type = db_type
        self.connection_params = connection_params
        self.collection = collection
        self.vector_column = vector_column
        self.metadata_columns = metadata_columns or []
        self.mode = mode
        self.options = options or {}
        
        # PostgreSQL vector database requires vector_column
        if self.db_type == "postgres" and not self.vector_column:
            self.vector_column = "embedding"
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        config = {
            "db_type": self.db_type,
            "connection_params": self.connection_params,
            "collection": self.collection,
            "metadata_columns": self.metadata_columns,
            "mode": self.mode,
            "options": self.options
        }
        
        if self.vector_column:
            config["vector_column"] = self.vector_column
            
        return config
    
    def validate(self) -> bool:
        """Validate the configuration."""
        valid_db_types = ["chroma", "postgres", "neo4j"]
        valid_modes = ["overwrite", "append", "upsert"]
        
        if self.db_type not in valid_db_types:
            return False
        if not self.connection_params:
            return False
        if not self.collection:
            return False
        if self.mode not in valid_modes:
            return False
        
        # Validate connection parameters based on db_type
        if self.db_type == "chroma":
            if "path" not in self.connection_params:
                return False
        elif self.db_type == "postgres":
            required_params = ["host", "port", "database", "user", "password"]
            if not all(param in self.connection_params for param in required_params):
                return False
            if not self.vector_column:
                return False
        elif self.db_type == "neo4j":
            required_params = ["uri", "user", "password"]
            if not all(param in self.connection_params for param in required_params):
                return False
        
        return True


# Example usage for ChromaDB:
# chroma_target_config = ConfigSingleton.get_instance(
#     VectorDBTargetConfig,
#     db_type="chroma",
#     connection_params={"path": "/path/to/chromadb"},
#     collection="document_embeddings",
#     metadata_columns=["title", "author", "date"],
#     mode="upsert",
#     options={"embedding_function": "sentence-transformers"}
# )

# Example usage for PostgreSQL with vector extension:
# postgres_vector_target_config = ConfigSingleton.get_instance(
#     VectorDBTargetConfig,
#     db_type="postgres",
#     connection_params={
#         "host": "localhost",
#         "port": 5432,
#         "database": "vectordb",
#         "user": "username",
#         "password": "password"
#     },
#     collection="document_embeddings",
#     vector_column="embedding",
#     metadata_columns=["title", "content", "url"],
#     mode="upsert",
#     options={"vector_dimension": 768, "index_type": "ivfflat"}
# )