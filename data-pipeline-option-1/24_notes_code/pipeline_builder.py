from typing import Dict, Any, List, Optional, Type
from pyspark.sql import SparkSession

from .base_pipeline import BasePipeline
from .domain_pipelines.admin_pipeline import AdminPipeline
from .domain_pipelines.hr_pipeline import HRPipeline
from .domain_pipelines.finance_pipeline import FinancePipeline
from .domain_pipelines.it_helpdesk_pipeline import ITHelpdeskPipeline


class PipelineBuilder:
    """
    Builder for creating pipeline instances.
    Implements the Builder Pattern.
    """
    
    def __init__(self):
        """Initialize the pipeline builder."""
        self.config = {}
        self.pipeline_type = None
    
    def with_spark_config(self, spark_config: Dict[str, Any]) -> 'PipelineBuilder':
        """
        Set Spark configuration.
        
        Args:
            spark_config: Spark configuration
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config["spark"] = spark_config
        return self
    
    def with_source_config(self, source_config: Dict[str, Any]) -> 'PipelineBuilder':
        """
        Set source configuration.
        
        Args:
            source_config: Source configuration
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config["source"] = source_config
        return self
    
    def with_preprocessing_config(self, preprocessing_config: Dict[str, Any]) -> 'PipelineBuilder':
        """
        Set preprocessing configuration.
        
        Args:
            preprocessing_config: Preprocessing configuration
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config["preprocessing"] = preprocessing_config
        return self
    
    def with_chunking_config(self, chunking_config: Dict[str, Any]) -> 'PipelineBuilder':
        """
        Set chunking configuration.
        
        Args:
            chunking_config: Chunking configuration
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config["chunking"] = chunking_config
        return self
    
    def with_embedding_config(self, embedding_config: Dict[str, Any]) -> 'PipelineBuilder':
        """
        Set embedding configuration.
        
        Args:
            embedding_config: Embedding configuration
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config["embedding"] = embedding_config
        return self
    
    def with_target_config(self, target_config: Dict[str, Any]) -> 'PipelineBuilder':
        """
        Set target configuration.
        
        Args:
            target_config: Target configuration
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config["target"] = target_config
        return self
    
    def with_logging_config(self, logging_config: Dict[str, Any]) -> 'PipelineBuilder':
        """
        Set logging configuration.
        
        Args:
            logging_config: Logging configuration
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config["logging"] = logging_config
        return self
    
    def with_exception_handling_config(self, exception_config: Dict[str, Any]) -> 'PipelineBuilder':
        """
        Set exception handling configuration.
        
        Args:
            exception_config: Exception handling configuration
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config["exception_handling"] = exception_config
        return self
    
    def with_pipeline_type(self, pipeline_type: str) -> 'PipelineBuilder':
        """
        Set pipeline type.
        
        Args:
            pipeline_type: Pipeline type
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.pipeline_type = pipeline_type
        return self
    
    def with_custom_config(self, key: str, value: Any) -> 'PipelineBuilder':
        """
        Set custom configuration.
        
        Args:
            key: Configuration key
            value: Configuration value
            
        Returns:
            PipelineBuilder: Builder instance
        """
        self.config[key] = value
        return self
    
    def build(self) -> BasePipeline:
        """
        Build a pipeline instance.
        
        Returns:
            BasePipeline: Pipeline instance
            
        Raises:
            ValueError: If pipeline type is not supported
        """
        if not self.pipeline_type:
            raise ValueError("Pipeline type must be specified")
        
        if self.pipeline_type == "admin":
            return AdminPipeline(self.config)
        elif self.pipeline_type == "hr":
            return HRPipeline(self.config)
        elif self.pipeline_type == "finance":
            return FinancePipeline(self.config)
        elif self.pipeline_type == "it_helpdesk":
            return ITHelpdeskPipeline(self.config)
        else:
            raise ValueError(f"Unsupported pipeline type: {self.pipeline_type}")