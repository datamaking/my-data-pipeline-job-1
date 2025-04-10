from typing import Dict, Any, List, Optional, Literal
from ..base_config import BaseConfig, ConfigSingleton


class HiveTargetConfig(BaseConfig):
    """Configuration for Hive target systems."""
    
    def __init__(
        self,
        database: str,
        table: str,
        format: Literal["parquet", "orc", "avro", "text"] = "parquet",
        mode: Literal["overwrite", "append", "error", "ignore"] = "overwrite",
        partition_by: Optional[List[str]] = None,
        load_type: Literal["full", "scd2", "cdc"] = "full",
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Hive target configuration.
        
        Args:
            database: Hive database name
            table: Target table name
            format: Storage format for the Hive table
            mode: Write mode (overwrite, append, error, ignore)
            partition_by: Columns to partition the table by
            load_type: Type of data load (full, scd2, cdc)
            options: Additional options for Hive
        """
        self.database = database
        self.table = table
        self.format = format
        self.mode = mode
        self.partition_by = partition_by or []
        self.load_type = load_type
        self.options = options or {}
        
        # Additional parameters for SCD Type 2
        if self.load_type == "scd2":
            self.options.setdefault("key_columns", [])
            self.options.setdefault("effective_from_column", "effective_from")
            self.options.setdefault("effective_to_column", "effective_to")
            self.options.setdefault("current_flag_column", "is_current")
        
        # Additional parameters for CDC
        if self.load_type == "cdc":
            self.options.setdefault("key_columns", [])
            self.options.setdefault("operation_column", "operation")
            self.options.setdefault("timestamp_column", "timestamp")
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "database": self.database,
            "table": self.table,
            "format": self.format,
            "mode": self.mode,
            "partition_by": self.partition_by,
            "load_type": self.load_type,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        valid_formats = ["parquet", "orc", "avro", "text"]
        valid_modes = ["overwrite", "append", "error", "ignore"]
        valid_load_types = ["full", "scd2", "cdc"]
        
        if not self.database or not self.table:
            return False
        if self.format not in valid_formats:
            return False
        if self.mode not in valid_modes:
            return False
        if self.load_type not in valid_load_types:
            return False
        
        # Validate SCD Type 2 configuration
        if self.load_type == "scd2":
            if "key_columns" not in self.options or not self.options["key_columns"]:
                return False
        
        # Validate CDC configuration
        if self.load_type == "cdc":
            if "key_columns" not in self.options or not self.options["key_columns"]:
                return False
        
        return True


# Example usage:
# hive_target_config = ConfigSingleton.get_instance(
#     HiveTargetConfig,
#     database="analytics",
#     table="processed_documents",
#     format="parquet",
#     mode="overwrite",
#     partition_by=["year", "month", "day"],
#     load_type="full",
#     options={"compression": "snappy"}
# )