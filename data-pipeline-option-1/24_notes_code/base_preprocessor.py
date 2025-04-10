from abc import ABC, abstractmethod
from typing import Dict, Any
from pyspark.sql import DataFrame


class BasePreprocessor(ABC):
    """Base class for all preprocessors."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the base preprocessor.
        
        Args:
            config: Preprocessor configuration
        """
        self.config = config
    
    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        """
        Process the input dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Processed dataframe
        """
        pass