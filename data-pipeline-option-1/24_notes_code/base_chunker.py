from abc import ABC, abstractmethod
from typing import Dict, Any, List
from pyspark.sql import DataFrame


class BaseChunker(ABC):
    """Base class for all chunkers."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the base chunker.
        
        Args:
            config: Chunker configuration
        """
        self.config = config
        self.input_col = config.get("input_col", "text")
        self.output_col = config.get("output_col", "chunks")
        self.id_col = config.get("id_col", "id")
    
    @abstractmethod
    def chunk(self, df: DataFrame) -> DataFrame:
        """
        Chunk the input dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Dataframe with chunks
        """
        pass