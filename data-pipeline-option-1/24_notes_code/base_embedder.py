from abc import ABC, abstractmethod
from typing import Dict, Any, List
from pyspark.sql import DataFrame


class BaseEmbedder(ABC):
    """Base class for all embedders."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the base embedder.
        
        Args:
            config: Embedder configuration
        """
        self.config = config
        self.input_col = config.get("input_col", "text")
        self.output_col = config.get("output_col", "embedding")
    
    @abstractmethod
    def create_embeddings(self, df: DataFrame) -> DataFrame:
        """
        Create embeddings for the input dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Dataframe with embeddings
        """
        pass