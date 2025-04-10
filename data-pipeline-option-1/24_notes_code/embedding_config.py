from typing import Dict, Any, List, Optional, Literal
from .base_config import BaseConfig, ConfigSingleton


class EmbeddingConfig(BaseConfig):
    """Configuration for embedding module."""
    
    def __init__(
        self,
        method: Literal["tfidf", "sentence_transformer", "word2vec", "glove", "custom"],
        params: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize embedding configuration.
        
        Args:
            method: Embedding method to use
            params: Parameters specific to the embedding method
            options: Additional options for embedding generation
        """
        self.method = method
        self.params = params
        self.options = options or {}
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "method": self.method,
            "params": self.params,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        valid_methods = ["tfidf", "sentence_transformer", "word2vec", "glove", "custom"]
        if self.method not in valid_methods:
            return False
        
        # Validate method-specific parameters
        if self.method == "tfidf":
            required_params = ["max_features", "min_df"]
            if not all(param in self.params for param in required_params):
                return False
        elif self.method == "sentence_transformer":
            if "model_name" not in self.params:
                return False
        elif self.method == "word2vec" or self.method == "glove":
            if "vector_size" not in self.params:
                return False
        elif self.method == "custom":
            if "embedding_function" not in self.params:
                return False
        
        return True


# Example usage:
# tfidf_config = ConfigSingleton.get_instance(
#     EmbeddingConfig,
#     method="tfidf",
#     params={"max_features": 10000, "min_df": 5, "ngram_range": (1, 2)},
#     options={"binary": False, "use_idf": True}
# )

# sentence_transformer_config = ConfigSingleton.get_instance(
#     EmbeddingConfig,
#     method="sentence_transformer",
#     params={"model_name": "all-MiniLM-L6-v2", "max_seq_length": 256},
#     options={"device": "cuda", "batch_size": 32}
# )