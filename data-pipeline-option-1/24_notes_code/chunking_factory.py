from typing import Dict, Any
from pyspark.sql import DataFrame

from .base_chunker import BaseChunker
from .fixed_size_chunker import FixedSizeChunker
from .semantic_chunker import SemanticChunker
from .chunk_smoother import ChunkSmoother


class ChunkingFactory:
    """
    Factory for creating chunker instances.
    Implements the Factory Pattern.
    """
    
    @staticmethod
    def create_chunker(strategy: str, config: Dict[str, Any]) -> BaseChunker:
        """
        Create a chunker instance based on the chunking strategy.
        
        Args:
            strategy: Chunking strategy
            config: Chunker configuration
            
        Returns:
            BaseChunker: Instance of a chunker class
            
        Raises:
            ValueError: If the chunking strategy is not supported
        """
        if strategy == "fixed_size":
            return FixedSizeChunker(config)
        elif strategy == "semantic":
            return SemanticChunker(config)
        else:
            raise ValueError(f"Unsupported chunking strategy: {strategy}")
    
    @staticmethod
    def create_smoother(config: Dict[str, Any]) -> ChunkSmoother:
        """
        Create a chunk smoother instance.
        
        Args:
            config: Smoother configuration
            
        Returns:
            ChunkSmoother: Instance of a chunk smoother
        """
        return ChunkSmoother(config)
    
    @staticmethod
    def apply_chunking_with_smoothing(
        df: DataFrame,
        chunker: BaseChunker,
        smoother: ChunkSmoother = None
    ) -> DataFrame:
        """
        Apply chunking and optional smoothing to a dataframe.
        
        Args:
            df: Input dataframe
            chunker: Chunker instance
            smoother: Optional smoother instance
            
        Returns:
            DataFrame: Dataframe with chunks
        """
        # Apply chunking
        chunked_df = chunker.chunk(df)
        
        # Apply smoothing if provided
        if smoother and smoother.is_enabled():
            return smoother.smooth(chunked_df)
        
        return chunked_df