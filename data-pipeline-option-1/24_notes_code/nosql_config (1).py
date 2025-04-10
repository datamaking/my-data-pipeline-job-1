from typing import Dict, Any, List, Optional, Literal
from ..base_config import BaseConfig, ConfigSingleton


class NoSQLTargetConfig(BaseConfig):
    """Configuration for NoSQL target systems (e.g., MongoDB)."""
    
    def __init__(
        self,
        connection_uri: str,
        database: str,
        collection: str,
        mode: Literal["overwrite", "append", "upsert"] = "overwrite",
        load_type: Literal["full", "scd2", "cdc"] = "full",
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize NoSQL target configuration.
        
        Args:
            connection_uri: Connection URI for the NoSQL database
            database: Database name
            collection: Collection name to write to
            mode: Write mode (overwrite, append, upsert)
            load_type: Type of data load (full, scd2, cdc)
            options: Additional options for NoSQL connection
        """
        self.connection_uri = connection_uri
        self.database = database
        self.collection = collection
        self.mode = mode
        self.load_type = load_type
        self.options = options or {}
        
        # Additional parameters for SCD Type 2
        if self.load_type == "scd2":
            self.options.setdefault("key_fields", [])
            self.options.setdefault("effective_from_field", "effective_from")
            self.options.setdefault("effective_to_field", "effective_to")
            self.options.setdefault("current_flag_field", "is_current")
        
        # Additional parameters for CDC
        if self.load_type == "cdc":
            self.options.setdefault("key_fields", [])
            self.options.setdefault("operation_field", "operation")
            self.options.setdefault("timestamp_field", "timestamp")
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "connection_uri": self.connection_uri,
            "database": self.database,
            "collection": self.collection,
            "mode": self.mode,
            "load_type": self.load_type,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        valid_modes = ["overwrite", "append", "upsert"]
        valid_load_types = ["full", "scd2", "cdc"]
        
        if not self.connection_uri:
            return False
        if not self.database or not self.collection:
            return False
        if self.mode not in valid_modes:
            return False
        if self.load_type not in valid_load_types:
            return False
        
        # Validate SCD Type 2 configuration
        if self.load_type == "scd2":
            if "key_fields" not in self.options or not self.options["key_fields"]:
                return False
        
        # Validate CDC configuration
        if self.load_type == "cdc":
            if "key_fields" not in self.options or not self.options["key_fields"]:
                return False
        
        return True


# Example usage:
# nosql_target_config = ConfigSingleton.get_instance(
#     NoSQLTargetConfig,
#     connection_uri="mongodb://localhost:27017",
#     database="nlp_data",
#     collection="processed_documents",
#     mode="upsert",
#     load_type="scd2",
#     options={
#         "key_fields": ["document_id"],
#         "effective_from_field": "valid_from",
#         "effective_to_field": "valid_to",
#         "current_flag_field": "is_active",
#         "write_concern": {"w": "majority"}
#     }
# )