from typing import Dict, Any, List, Optional, Literal
from .base_config import BaseConfig, ConfigSingleton


class ChunkingConfig(BaseConfig):
    """Configuration for chunking module."""
    
    def __init__(
        self,
        strategy: Literal["fixed_size", "semantic", "sentence", "paragraph"],
        params: Dict[str, Any],
        smoothing: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize chunking configuration.
        
        Args:
            strategy: Chunking strategy to use
            params: Parameters specific to the chunking strategy
            smoothing: Configuration for chunk smoothing
            options: Additional options for chunking
        """
        self.strategy = strategy
        self.params = params
        self.smoothing = smoothing or {}
        self.options = options or {}
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "strategy": self.strategy,
            "params": self.params,
            "smoothing": self.smoothing,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        valid_strategies = ["fixed_size", "semantic", "sentence", "paragraph"]
        if self.strategy not in valid_strategies:
            return False
        
        # Validate strategy-specific parameters
        if self.strategy == "fixed_size":
            if "chunk_size" not in self.params:
                return False
            if not isinstance(self.params["chunk_size"], int) or self.params["chunk_size"] <= 0:
                return False
            if "overlap" in self.params and (not isinstance(self.params["overlap"], int) or self.params["overlap"] < 0):
                return False
        elif self.strategy == "semantic":
            if "min_chunk_size" not in self.params or "max_chunk_size" not in self.params:
                return False
        
        # Validate smoothing if provided
        if self.smoothing:
            if "enabled" not in self.smoothing:
                return False
            if self.smoothing["enabled"] and "method" not in self.smoothing:
                return False
        
        return True


# Example usage:
# chunking_config = ConfigSingleton.get_instance(
#     ChunkingConfig,
#     strategy="fixed_size",
#     params={"chunk_size": 1000, "overlap": 200},
#     smoothing={"enabled": True, "method": "sentence_boundary", "max_lookback": 100},
#     options={"preserve_metadata": True}
# )