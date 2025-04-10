import sys
import traceback
from typing import Dict, Any, Optional, Callable, Type, List, Union
from functools import wraps

from .logger import LoggerSingleton


class ExceptionHandler:
    """
    Exception handler class.
    Implements the Singleton Pattern.
    """
    
    _instance = None
    
    @classmethod
    def get_instance(cls, config: Optional[Dict[str, Any]] = None):
        """
        Get or create the exception handler instance.
        
        Args:
            config: Exception handler configuration
            
        Returns:
            ExceptionHandler: Exception handler instance
        """
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the exception handler.
        
        Args:
            config: Exception handler configuration
        """
        if config is None:
            config = {}
        
        self.logger = LoggerSingleton.get_instance().get_logger()
        self.raise_exceptions = config.get("raise_exceptions", True)
        self.log_exceptions = config.get("log_exceptions", True)
        self.exit_on_exception = config.get("exit_on_exception", False)
        self.exit_code = config.get("exit_code", 1)
        self.custom_handlers = {}
    
    def register_handler(self, exception_type: Type[Exception], handler: Callable[[Exception], Any]):
        """
        Register a custom exception handler.
        
        Args:
            exception_type: Type of exception to handle
            handler: Handler function
        """
        self.custom_handlers[exception_type] = handler
    
    def handle_exception(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Any:
        """
        Handle an exception.
        
        Args:
            exception: Exception to handle
            context: Additional context information
            
        Returns:
            Any: Result from custom handler if available
        """
        if context is None:
            context = {}
        
        # Get exception details
        exc_type = type(exception)
        exc_message = str(exception)
        exc_traceback = traceback.format_exc()
        
        # Log the exception if configured
        if self.log_exceptions:
            self.logger.error(
                f"Exception occurred: {exc_type.__name__}: {exc_message}",
                exception_type=exc_type.__name__,
                exception_message=exc_message,
                traceback=exc_traceback,
                context=context
            )
        
        # Check if there's a custom handler for this exception type
        for exception_class, handler in self.custom_handlers.items():
            if isinstance(exception, exception_class):
                return handler(exception)
        
        # Exit if configured
        if self.exit_on_exception:
            sys.exit(self.exit_code)
        
        # Re-raise if configured
        if self.raise_exceptions:
            raise exception
    
    def handle_exceptions(self, exception_types: Optional[List[Type[Exception]]] = None):
        """
        Decorator to handle exceptions in a function.
        
        Args:
            exception_types: List of exception types to handle
            
        Returns:
            Callable: Decorator function
        """
        if exception_types is None:
            exception_types = [Exception]
        
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Only handle specified exception types
                    if any(isinstance(e, exc_type) for exc_type in exception_types):
                        context = {
                            "function": func.__name__,
                            "args": str(args),
                            "kwargs": str(kwargs)
                        }
                        return self.handle_exception(e, context)
                    else:
                        raise
            return wrapper
        return decorator
    
    def retry_on_exception(
        self,
        exception_types: Optional[List[Type[Exception]]] = None,
        max_retries: int = 3,
        delay: float = 1.0,
        backoff_factor: float = 2.0,
        jitter: float = 0.1
    ):
        """
        Decorator to retry a function on exception.
        
        Args:
            exception_types: List of exception types to retry on
            max_retries: Maximum number of retries
            delay: Initial delay between retries in seconds
            backoff_factor: Factor to increase delay between retries
            jitter: Random jitter factor to add to delay
            
        Returns:
            Callable: Decorator function
        """
        if exception_types is None:
            exception_types = [Exception]
        
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                import random
                import time
                
                retries = 0
                current_delay = delay
                
                while True:
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        # Only retry on specified exception types
                        if not any(isinstance(e, exc_type) for exc_type in exception_types):
                            raise
                        
                        retries += 1
                        if retries > max_retries:
                            context = {
                                "function": func.__name__,
                                "args": str(args),
                                "kwargs": str(kwargs),
                                "retries": retries
                            }
                            return self.handle_exception(e, context)
                        
                        # Log retry attempt
                        self.logger.warning(
                            f"Retry {retries}/{max_retries} for {func.__name__} after error: {str(e)}",
                            function=func.__name__,
                            retries=retries,
                            max_retries=max_retries,
                            exception=str(e)
                        )
                        
                        # Calculate delay with jitter
                        jitter_amount = random.uniform(-jitter, jitter) * current_delay
                        sleep_time = current_delay + jitter_amount
                        
                        # Sleep before retry
                        time.sleep(max(0, sleep_time))
                        
                        # Increase delay for next retry
                        current_delay *= backoff_factor
            
            return wrapper
        return decorator