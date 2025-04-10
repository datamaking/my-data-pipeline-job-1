import argparse
import json
import os
from typing import Dict, Any

from pyspark.sql import SparkSession

from pipeline.pipeline_builder import PipelineBuilder
from utils.logger import LoggerSingleton
from utils.exception_handler import ExceptionHandler


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="NLP ETL Pipeline")
    parser.add_argument(
        "--config", 
        type=str, 
        required=True, 
        help="Path to configuration file"
    )
    parser.add_argument(
        "--domain", 
        type=str, 
        choices=["admin", "hr", "finance", "it_helpdesk"], 
        required=True, 
        help="Domain type for the pipeline"
    )
    parser.add_argument(
        "--log-level", 
        type=str, 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], 
        default="INFO", 
        help="Logging level"
    )
    parser.add_argument(
        "--log-file", 
        type=str, 
        help="Path to log file"
    )
    return parser.parse_args()


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r") as f:
        return json.load(f)


def main():
    """Main entry point for the NLP ETL Pipeline."""
    # Parse command line arguments
    args = parse_args()
    
    # Initialize logger
    logger_config = {
        "log_level": args.log_level,
        "log_file": args.log_file,
        "log_to_console": True,
        "log_to_file": bool(args.log_file)
    }
    logger = LoggerSingleton.get_instance(logger_config).get_logger()
    
    # Initialize exception handler
    exception_handler = ExceptionHandler.get_instance({
        "raise_exceptions": True,
        "log_exceptions": True,
        "exit_on_exception": True,
        "exit_code": 1
    })
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)
        
        # Add logging configuration
        config["logging"] = logger_config
        
        # Build and run pipeline
        logger.info(f"Building {args.domain} pipeline")
        pipeline = PipelineBuilder() \
            .with_pipeline_type(args.domain) \
            .with_spark_config(config.get("spark", {})) \
            .with_source_config(config.get("source", {})) \
            .with_preprocessing_config(config.get("preprocessing", {})) \
            .with_chunking_config(config.get("chunking", {})) \
            .with_embedding_config(config.get("embedding", {})) \
            .with_target_config(config.get("target", {})) \
            .with_logging_config(logger_config) \
            .with_exception_handling_config(config.get("exception_handling", {})) \
            .build()
        
        logger.info("Running pipeline")
        pipeline.run()
        
        logger.info("Pipeline execution completed successfully")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()