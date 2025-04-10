import logging
import os
from typing import Dict, Any, Optional
import json
import time
from datetime import datetime


class LoggerSingleton:
    """
    Logger singleton class.
    Implements the Singleton Pattern.
    """
    
    _instance = None
    
    @classmethod
    def get_instance(cls, config: Optional[Dict[str, Any]] = None):
        """
        Get or create the logger instance.
        
        Args:
            config: Logger configuration
            
        Returns:
            LoggerSingleton: Logger instance
        """
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the logger.
        
        Args:
            config: Logger configuration
        """
        if config is None:
            config = {}
        
        self.log_level = config.get("log_level", "INFO")
        self.log_format = config.get("log_format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.log_file = config.get("log_file")
        self.log_to_console = config.get("log_to_console", True)
        self.log_to_file = config.get("log_to_file", False)
        
        # Create logger
        self.logger = logging.getLogger("nlp_etl_pipeline")
        self.logger.setLevel(getattr(logging, self.log_level))
        
        # Remove existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Create formatters
        formatter = logging.Formatter(self.log_format)
        
        # Add console handler if configured
        if self.log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # Add file handler if configured
        if self.log_to_file and self.log_file:
            # Create log directory if it doesn't exist
            log_dir = os.path.dirname(self.log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def get_logger(self):
        """
        Get the logger instance.
        
        Returns:
            logging.Logger: Logger instance
        """
        return self.logger
    
    def log(self, level: str, message: str, **kwargs):
        """
        Log a message with the specified level.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Log message
            **kwargs: Additional log data
        """
        log_method = getattr(self.logger, level.lower())
        
        # If there are additional kwargs, add them to the log message as JSON
        if kwargs:
            message = f"{message} - {json.dumps(kwargs)}"
        
        log_method(message)
    
    def debug(self, message: str, **kwargs):
        """
        Log a debug message.
        
        Args:
            message: Log message
            **kwargs: Additional log data
        """
        self.log("DEBUG", message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """
        Log an info message.
        
        Args:
            message: Log message
            **kwargs: Additional log data
        """
        self.log("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """
        Log a warning message.
        
        Args:
            message: Log message
            **kwargs: Additional log data
        """
        self.log("WARNING", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """
        Log an error message.
        
        Args:
            message: Log message
            **kwargs: Additional log data
        """
        self.log("ERROR", message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """
        Log a critical message.
        
        Args:
            message: Log message
            **kwargs: Additional log data
        """
        self.log("CRITICAL", message, **kwargs)
    
    def log_execution_time(self, func):
        """
        Decorator to log the execution time of a function.
        
        Args:
            func: Function to decorate
            
        Returns:
            Function: Decorated function
        """
        def wrapper(*args, **kwargs):
            start_time = time.time()
            self.info(f"Starting {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                execution_time = end_time - start_time
                self.info(f"Completed {func.__name__}", execution_time=execution_time)
                return result
            except Exception as e:
                end_time = time.time()
                execution_time = end_time - start_time
                self.error(f"Error in {func.__name__}: {str(e)}", execution_time=execution_time)
                raise
        
        return wrapper