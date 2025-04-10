from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame


class BaseTarget(ABC):
    """Base class for all targets."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the base target.
        
        Args:
            config: Target configuration
        """
        self.config = config
    
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """
        Write data to the target.
        
        Args:
            df: Input dataframe to write
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate the connection to the target.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        pass