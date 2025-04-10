from typing import Dict, Any
from pyspark.sql import DataFrame

from .base_target import BaseTarget
from .file_target import FileTarget
from .rdbms_target import RDBMSTarget
from .hive_target import HiveTarget
from .nosql_target import NoSQLTarget
from .vector_db_target import VectorDBTarget


class TargetFactory:
    """
    Factory for creating target instances.
    Implements the Factory Pattern.
    """
    
    @staticmethod
    def create_target(target_type: str, config: Dict[str, Any]) -> BaseTarget:
        """
        Create a target instance based on the target type.
        
        Args:
            target_type: Type of target to create
            config: Target configuration
            
        Returns:
            BaseTarget: Instance of a target class
            
        Raises:
            ValueError: If the target type is not supported
        """
        if target_type == "file":
            return FileTarget(config)
        elif target_type == "rdbms":
            return RDBMSTarget(config)
        elif target_type == "hive":
            return HiveTarget(config)
        elif target_type == "nosql":
            return NoSQLTarget(config)
        elif target_type == "vector_db":
            return VectorDBTarget(config)
        else:
            raise ValueError(f"Unsupported target type: {target_type}")