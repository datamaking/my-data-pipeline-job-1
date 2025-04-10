from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame

from ..base_pipeline import BasePipeline
from ...source.source_factory import SourceFactory
from ...preprocessing.preprocessing_factory import PreprocessingFactory
from ...chunking.chunking_factory import ChunkingFactory
from ...embedding.embedding_factory import EmbeddingFactory
from ...target.target_factory import TargetFactory


class AdminPipeline(BasePipeline):
    """Pipeline for administrative domain data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the admin pipeline.
        
        Args:
            config: Pipeline configuration
        """
        super().__init__(config)
        self.domain_config = config.get("admin", {})
    
    def configure(self) -> None:
        """Configure the pipeline with admin-specific configurations."""
        # Add domain-specific configurations
        self.logger.info("Configuring Admin pipeline")
        
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
        embedding_config.update(self.domain_config.get("embedding", {}))
        self.config["embedding"] = embedding_config
        
        # Configure target
        target_config = self.config.get("target", {})
        target_config.update(self.domain_config.get("target", {}))
        self.config["target"] = target_config
    
    def extract(self) -> DataFrame:
        """
        Extract data from source systems for admin domain.
        
        Returns:
            DataFrame: Extracted data
        """
        self.logger.info("Extracting data for Admin domain")
        
        source_config = self.config.get("source", {})
        source_type = source_config.get("type")
        
        if not source_type:
            raise ValueError("Source type must be specified")
        
        # Create source instance
        source = SourceFactory.create_source(source_type, self.spark, source_config)
        
        # Validate connection
        if not source.validate_connection():
            raise ConnectionError(f"Failed to connect to source: {source_type}")
        
        # Extract data
        return source.read()
    
    def preprocess(self, df: DataFrame) -> DataFrame:
        """
        Preprocess the extracted data for admin domain.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Preprocessed dataframe
        """
        self.logger.info("Preprocessing data for Admin domain")
        
        preprocessing_config = self.config.get("preprocessing", {})
        steps = preprocessing_config.get("steps", [])
        
        if not steps:
            self.logger.warning("No preprocessing steps configured, returning input dataframe")
            return df
        
        # Create preprocessors
        preprocessors = {}
        for step in steps:
            step_type = step.get("type")
            step_config = step.get("config", {})
            preprocessor = PreprocessingFactory.create_preprocessor(step_type, step_config)
            preprocessors[step_type] = preprocessor
        
        # Apply preprocessing steps in sequence
        result_df = df
        for step_type, preprocessor in preprocessors.items():
            self.logger.info(f"Applying preprocessing step: {step_type}")
            result_df = preprocessor.process(result_df)
        
        return result_df
    
    def chunk(self, df: DataFrame) -> DataFrame:
        """
        Chunk the preprocessed data for admin domain.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Chunked dataframe
        """
        self.logger.info("Chunking data for Admin domain")
        
        chunking_config = self.config.get("chunking", {})
        strategy = chunking_config.get("strategy")
        
        if not strategy:
            self.logger.warning("No chunking strategy configured, returning input dataframe")
            return df
        
        # Create chunker
        chunker = ChunkingFactory.create_chunker(strategy, chunking_config)
        
        # Create smoother if configured
        smoother = None
        if chunking_config.get("smoothing", {}).get("enabled", False):
            smoother = ChunkingFactory.create_smoother(chunking_config.get("smoothing", {}))
        
        # Apply chunking and smoothing
        return ChunkingFactory.apply_chunking_with_smoothing(df, chunker, smoother)
    
    def embed(self, df: DataFrame) -> DataFrame:
        """
        Create embeddings for the chunked data for admin domain.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Dataframe with embeddings
        """
        self.logger.info("Creating embeddings for Admin domain")
        
        embedding_config = self.config.get("embedding", {})
        method = embedding_config.get("method")
        
        if not method:
            self.logger.warning("No embedding method configured, returning input dataframe")
            return df
        
        # Create embedder
        embedder = EmbeddingFactory.create_embedder(method, embedding_config)
        
        # Create embeddings
        return embedder.create_embeddings(df)
    
    def load(self, df: DataFrame) -> None:
        """
        Load the data to target systems for admin domain.
        
        Args:
            df: Input dataframe
        """
        self.logger.info("Loading data for Admin domain")
        
        target_config = self.config.get("target", {})
        target_type = target_config.get("type")
        
        if not target_type:
            raise ValueError("Target type must be specified")
        
        # Create target instance
        target = TargetFactory.create_target(target_type, target_config)
        
        # Validate connection
        if not target.validate_connection():
            raise ConnectionError(f"Failed to connect to target: {target_type}")
        
        # Load data
        target.write(df)