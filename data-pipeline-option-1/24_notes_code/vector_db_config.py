from typing import Dict, Any, List, Optional, Literal
from ..base_config import BaseConfig, ConfigSingleton


class VectorDBSourceConfig(BaseConfig):
    """Configuration for vector database source systems."""
    
    def __init__(
        self,
        db_type: Literal["chroma", "postgres", "neo4j"],
        connection_params: Dict[str, Any],
        collections: Optional[List[str]] = None,
        queries: Optional[List[Dict[str, Any]]] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize vector database source configuration.
        
        Args:
            db_type: Type of vector database ('chroma', 'postgres', 'neo4j')
            connection_params: Connection parameters specific to the database type
            collections: List of collections/tables to read from
            queries: List of queries specific to the vector database
            options: Additional options for vector database connection
        """
        self.db_type = db_type
        self.connection_params = connection_params
        self.collections = collections or []
        self.queries = queries or []
        self.options = options or {}
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "db_type": self.db_type,
            "connection_params": self.connection_params,
            "collections": self.collections,
            "queries": self.queries,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        if self.db_type not in ["chroma", "postgres", "neo4j"]:
            return False
        if not self.connection_params:
            return False
        
        # Validate connection parameters based on db_type
        if self.db_type == "chroma":
            if "path" not in self.connection_params:
                return False
        elif self.db_type == "postgres":
            required_params = ["host", "port", "database", "user", "password"]
            if not all(param in self.connection_params for param in required_params):
                return False
        elif self.db_type == "neo4j":
            required_params = ["uri", "user", "password"]
            if not all(param in self.connection_params for param in required_params):
                return False
        
        return True


# Example usage for ChromaDB:
# chroma_config = ConfigSingleton.get_instance(
#     VectorDBSourceConfig,
#     db_type="chroma",
#     connection_params={"path": "/path/to/chromadb"},
#     collections=["documents"],
#     options={"embedding_function": "sentence-transformers"}
# )

# Example usage for PostgreSQL with vector extension:
# postgres_vector_config = ConfigSingleton.get_instance(
#     VectorDBSourceConfig,
#     db_type="postgres",
#     connection_params={
#         "host": "localhost",
#         "port": 5432,
#         "database": "vectordb",
#         "user": "username",
#         "password": "password"
#     },
#     collections=["embeddings"],
#     queries=[{"sql": "SELECT * FROM embeddings WHERE vector_column <-> '[0.1, 0.2, 0.3]' < 0.5"}]
# )