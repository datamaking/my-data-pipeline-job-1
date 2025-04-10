from typing import Dict, Any, List, Optional
from ..base_config import BaseConfig, ConfigSingleton


class HiveSourceConfig(BaseConfig):
    """Configuration for Hive source systems."""
    
    def __init__(
        self,
        metastore_uri: str,
        database: str,
        tables: Optional[List[str]] = None,
        queries: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Hive source configuration.
        
        Args:
            metastore_uri: URI for the Hive metastore
            database: Hive database name
            tables: List of tables to read from
            queries: List of HQL queries to execute
            options: Additional options for Hive connection
        """
        self.metastore_uri = metastore_uri
        self.database = database
        self.tables = tables or []
        self.queries = queries or []
        self.options = options or {}
        
        if not self.tables and not self.queries:
            raise ValueError("Either tables or queries must be provided")
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "metastore_uri": self.metastore_uri,
            "database": self.database,
            "tables": self.tables,
            "queries": self.queries,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        if not self.metastore_uri or not self.database:
            return False
        if not self.tables and not self.queries:
            return False
        return True


# Example usage:
# hive_config = ConfigSingleton.get_instance(
#     HiveSourceConfig,
#     metastore_uri="thrift://localhost:9083",
#     database="default",
#     tables=["table1", "table2"],
#     options={"hive.exec.dynamic.partition": "true"}
# )