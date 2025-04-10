from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame

from ..config.config_manager import ConfigManager, ConfigSingleton
from ..source.source_factory import SourceFactory
from ..preprocessing.preprocessing_factory import PreprocessingFactory
from ..chunking.chunking_factory import ChunkingFactory
from ..embedding.embedding_factory import EmbeddingFactory
from ..target.target_factory import TargetFactory
from ..utils.logger import LoggerSingleton
from ..utils.exception_handler import ExceptionHandler


class BasePipeline(ABC):
    """Base class for all pipelines."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the base pipeline.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.logger = LoggerSingleton.get_instance().get_logger()
        self.exception_handler = ExceptionHandler.get_instance()
        
        # Initialize SparkSession
        self.spark = self._create_spark_session()
        
        # Initialize config manager
        self.config_manager = ConfigSingleton.get_instance(ConfigManager)
        
        # Initialize intermediate data storage
        self.intermediate_data = {}
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create a SparkSession.
        
        Returns:
            SparkSession: Spark session instance
        """
        spark_config = self.config.get("spark", {})
        app_name = spark_config.get("app_name", "NLP ETL Pipeline")
        master = spark_config.get("master", "local[*]")
        
        # Create builder
        builder = SparkSession.builder.appName(app_name).master(master)
        
        # Add Hive support if configured
        if spark_config.get("enable_hive", False):
            builder = builder.enableHiveSupport()
        
        # Add config options
        for key, value in spark_config.get("config_options", {}).items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    @abstractmethod
    def configure(self) -> None:
        """Configure the pipeline with specific configurations."""
        pass
    
    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data from source systems.
        
        Returns:
            DataFrame: Extracted data
        """
        pass
    
    @abstractmethod
    def preprocess(self, df: DataFrame) -> DataFrame:
        """
        Preprocess the extracted data.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Preprocessed dataframe
        """
        pass
    
    @abstractmethod
    def chunk(self, df: DataFrame) -> DataFrame:
        """
        Chunk the preprocessed data.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Chunked dataframe
        """
        pass
    
    @abstractmethod
    def embed(self, df: DataFrame) -> DataFrame:
        """
        Create embeddings for the chunked data.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Dataframe with embeddings
        """
        pass
    
    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Load the data to target systems.
        
        Args:
            df: Input dataframe
        """
        pass
    
    def run(self) -> None:
        """Run the pipeline."""
        try:
            self.logger.info("Starting pipeline execution")
            
            # Configure the pipeline
            self.logger.info("Configuring pipeline")
            self.configure()
            
            # Extract data
            self.logger.info("Extracting data from sources")
            extracted_df = self.extract()
            self.intermediate_data["extracted"] = extracted_df
            
            # Preprocess data
            self.logger.info("Preprocessing data")
            preprocessed_df = self.preprocess(extracted_df)
            self.intermediate_data["preprocessed"] = preprocessed_df
            
            # Chunk data
            self.logger.info("Chunking data")
            chunked_df = self.chunk(preprocessed_df)
            self.intermediate_data["chunked"] = chunked_df
            
            # Create embeddings
            self.logger.info("Creating embeddings")
            embedded_df = self.embed(chunked_df)
            self.intermediate_data["embedded"] = embedded_df
            
            # Load data to targets
            self.logger.info("Loading data to targets")
            self.load(embedded_df)
            
            self.logger.info("Pipeline execution completed successfully")
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            raise
        finally:
            # Clean up intermediate data if configured
            if self.config.get("clean_intermediate_data", True):
                self.intermediate_data.clear()
    
    def get_intermediate_data(self, stage: str) -> Optional[DataFrame]:
        """
        Get intermediate data from a specific pipeline stage.
        
        Args:
            stage: Pipeline stage name
            
        Returns:
            DataFrame: Intermediate dataframe
        """
        return self.intermediate_data.get(stage)
    
    def persist_intermediate_data(self, stage: str, path: str) -> None:
        """
        Persist intermediate data to a file.
        
        Args:
            stage: Pipeline stage name
            path: Output path
        """
        df = self.get_intermediate_data(stage)
        if df is not None:
            df.write.parquet(path, mode="overwrite")
            self.logger.info(f"Persisted intermediate data for stage '{stage}' to {path}")
        else:
            self.logger.warning(f"No intermediate data found for stage '{stage}'")
    
    def load_intermediate_data(self, stage: str, path: str) -> None:
        """
        Load intermediate data from a file.
        
        Args:
            stage: Pipeline stage name
            path: Input path
        """
        try:
            df = self.spark.read.parquet(path)
            self.intermediate_data[stage] = df
            self.logger.info(f"Loaded intermediate data for stage '{stage}' from {path}")
        except Exception as e:
            self.logger.error(f"Failed to load intermediate data for stage '{stage}': {str(e)}")
            raise