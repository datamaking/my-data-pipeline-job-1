from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class BaseConfig(ABC):
    """Base configuration class that all configurations should inherit from."""
    
    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate the configuration."""
        pass


class ConfigSingleton:
    """Singleton pattern implementation for configuration classes."""
    
    _instances = {}
    
    @classmethod
    def get_instance(cls, config_class, *args, **kwargs):
        """Get or create an instance of the configuration class."""
        if config_class not in cls._instances:
            cls._instances[config_class] = config_class(*args, **kwargs)
        return cls._instances[config_class]