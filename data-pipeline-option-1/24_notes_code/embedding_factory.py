from typing import Dict, Any
from pyspark.sql import DataFrame

from .base_embedder import BaseEmbedder
from .tfidf_embedder import TFIDFEmbedder
from .sentence_embedder import SentenceEmbedder


class EmbeddingFactory:
    """
    Factory for creating embedder instances.
    Implements the Factory Pattern.
    """
    
    @staticmethod
    def create_embedder(method: str, config: Dict[str, Any]) -> BaseEmbedder:
        """
        Create an embedder instance based on the embedding method.
        
        Args:
            method: Embedding method
            config: Embedder configuration
            
        Returns:
            BaseEmbedder: Instance of an embedder class
            
        Raises:
            ValueError: If the embedding method is not supported
        """
        if method == "tfidf":
            return TFIDFEmbedder(config)
        elif method == "sentence_transformer":
            return SentenceEmbedder(config)
        else:
            raise ValueError(f"Unsupported embedding method: {method}")