from typing import Dict, Any, List, Optional, Literal
from ..base_config import BaseConfig, ConfigSingleton


class RDBMSTargetConfig(BaseConfig):
    """Configuration for RDBMS target systems."""
    
    def __init__(
        self,
        jdbc_url: str,
        driver: str,
        user: str,
        password: str,
        table: str,
        schema: Optional[str] = None,
        mode: Literal["overwrite", "append", "error", "ignore"] = "overwrite",
        load_type: Literal["full", "scd2", "cdc"] = "full",
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize RDBMS target configuration.
        
        Args:
            jdbc_url: JDBC URL for the database
            driver: JDBC driver class
            user: Database username
            password: Database password
            table: Target table name
            schema: Database schema (if applicable)
            mode: Write mode (overwrite, append, error, ignore)
            load_type: Type of data load (full, scd2, cdc)
            batch_size: Number of records to insert in each batch
            options: Additional options for database connection
        """
        self.jdbc_url = jdbc_url
        self.driver = driver
        self.user = user
        self.password = password
        self.table = table
        self.schema = schema
        self.mode = mode
        self.load_type = load_type
        self.batch_size = batch_size
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
        config = {
            "jdbc_url": self.jdbc_url,
            "driver": self.driver,
            "user": self.user,
            "password": self.password,
            "table": self.table,
            "mode": self.mode,
            "load_type": self.load_type,
            "batch_size": self.batch_size,
            "options": self.options
        }
        
        if self.schema:
            config["schema"] = self.schema
            
        return config
    
    def validate(self) -> bool:
        """Validate the configuration."""
        valid_modes = ["overwrite", "append", "error", "ignore"]
        valid_load_types = ["full", "scd2", "cdc"]
        
        if not self.jdbc_url or not self.driver:
            return False
        if not self.user or not self.password:
            return False
        if not self.table:
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
# rdbms_target_config = ConfigSingleton.get_instance(
#     RDBMSTargetConfig,
#     jdbc_url="jdbc:postgresql://localhost:5432/mydatabase",
#     driver="org.postgresql.Driver",
#     user="username",
#     password="password",
#     table="target_table",
#     schema="public",
#     mode="overwrite",
#     load_type="scd2",
#     options={
#         "key_columns": ["id", "product_code"],
#         "effective_from_column": "valid_from",
#         "effective_to_column": "valid_to",
#         "current_flag_column": "is_active"
#     }
# )