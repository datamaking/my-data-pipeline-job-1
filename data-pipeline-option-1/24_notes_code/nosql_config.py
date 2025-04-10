from typing import Dict, Any, List, Optional
from ..base_config import BaseConfig, ConfigSingleton


class NoSQLSourceConfig(BaseConfig):
    """Configuration for NoSQL source systems (e.g., MongoDB)."""
    
    def __init__(
        self,
        connection_uri: str,
        database: str,
        collections: List[str],
        queries: Optional[List[Dict[str, Any]]] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize NoSQL source configuration.
        
        Args:
            connection_uri: Connection URI for the NoSQL database
            database: Database name
            collections: List of collections to read from
            queries: List of query documents (for MongoDB)
            options: Additional options for NoSQL connection
        """
        self.connection_uri = connection_uri
        self.database = database
        self.collections = collections
        self.queries = queries or []
        self.options = options or {}
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "connection_uri": self.connection_uri,
            "database": self.database,
            "collections": self.collections,
            "queries": self.queries,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        if not self.connection_uri or not self.database:
            return False
        if not self.collections:
            return False
        return True


# Example usage:
# nosql_config = ConfigSingleton.get_instance(
#     NoSQLSourceConfig,
#     connection_uri="mongodb://localhost:27017",
#     database="mydatabase",
#     collections=["collection1", "collection2"],
#     queries=[{"status": "active"}],
#     options={"readPreference": "primaryPreferred"}
# )