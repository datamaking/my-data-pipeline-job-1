from typing import Dict, Any
from pyspark.sql import DataFrame

from .base_preprocessor import BasePreprocessor
from .html_parser import HTMLParser
from .data_cleaner import DataCleaner


class PreprocessingFactory:
    """
    Factory for creating preprocessor instances.
    Implements the Factory Pattern.
    """
    
    @staticmethod
    def create_preprocessor(preprocessor_type: str, config: Dict[str, Any]) -> BasePreprocessor:
        """
        Create a preprocessor instance based on the preprocessor type.
        
        Args:
            preprocessor_type: Type of preprocessor to create
            config: Preprocessor configuration
            
        Returns:
            BasePreprocessor: Instance of a preprocessor class
            
        Raises:
            ValueError: If the preprocessor type is not supported
        """
        if preprocessor_type == "html_parser":
            return HTMLParser(config)
        elif preprocessor_type == "data_cleaner":
            return DataCleaner(config)
        else:
            raise ValueError(f"Unsupported preprocessor type: {preprocessor_type}")
    
    @staticmethod
    def create_pipeline(steps: Dict[str, Dict[str, Any]]) -> Dict[str, BasePreprocessor]:
        """
        Create a pipeline of preprocessors.
        
        Args:
            steps: Dictionary of preprocessor types and their configurations
            
        Returns:
            Dict[str, BasePreprocessor]: Dictionary of preprocessor instances
        """
        preprocessors = {}
        for step_name, step_config in steps.items():
            preprocessor_type = step_config.get("type")
            preprocessor_config = step_config.get("config", {})
            preprocessors[step_name] = PreprocessingFactory.create_preprocessor(
                preprocessor_type, preprocessor_config
            )
        return preprocessors
    
    @staticmethod
    def apply_pipeline(df: DataFrame, preprocessors: Dict[str, BasePreprocessor]) -> DataFrame:
        """
        Apply a pipeline of preprocessors to a dataframe.
        
        Args:
            df: Input dataframe
            preprocessors: Dictionary of preprocessor instances
            
        Returns:
            DataFrame: Processed dataframe
        """
        result_df = df
        for step_name, preprocessor in preprocessors.items():
            result_df = preprocessor.process(result_df)
        return result_df