from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, lower

from ..base_pipeline import BasePipeline
from ...source.source_factory import SourceFactory
from ...preprocessing.preprocessing_factory import PreprocessingFactory
from ...chunking.chunking_factory import ChunkingFactory
from ...embedding.embedding_factory import EmbeddingFactory
from ...target.target_factory import TargetFactory


class HRPipeline(BasePipeline):
    """Pipeline for HR domain data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the HR pipeline.
        
        Args:
            config: Pipeline configuration
        """
        super().__init__(config)
        self.domain_config = config.get("hr", {})
    
    def configure(self) -> None:
        """Configure the pipeline with HR-specific configurations."""
        # Add domain-specific configurations
        self.logger.info("Configuring HR pipeline")
        
        # Configure source
        source_config = self.config.get("source", {})
        source_config.update(self.domain_config.get("source", {}))
        self.config["source"] = source_config
        
        # Configure preprocessing
        preprocessing_config = self.config.get("preprocessing", {})
        preprocessing_config.update(self.domain_config.get("preprocessing", {}))
        self.config["preprocessing"] = preprocessing_config
        
        # Configure chunking
        chunking_config = self.config.get("chunking", {})
        chunking_config.update(self.domain_config.get("chunking", {}))
        self.config["chunking"] = chunking_config
        
        # Configure embedding
        embedding_config = self.config.get("embedding", {})
        embedding_config.update(self.domain_