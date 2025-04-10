from typing import Dict, Any, Type, Optional
from .base_config import BaseConfig, ConfigSingleton


class ConfigManager:
    """
    Configuration manager that combines various configurations.
    Implements the Singleton pattern.
    """
    
    def __init__(self):
        self._configs: Dict[str, BaseConfig] = {}
    
    def add_config(self, name: str, config: BaseConfig) -> None:
        """Add a configuration to the manager."""
        if not config.validate():
            raise ValueError(f"Invalid configuration for {name}")
        self._configs[name] = config
    
    def get_config(self, name: str) -> Optional[BaseConfig]:
        """Get a configuration by name."""
        return self._configs.get(name)
    
    def get_all_configs(self) -> Dict[str, BaseConfig]:
        """Get all configurations."""
        return self._configs
    
    def get_combined_config(self) -> Dict[str, Any]:
        """Get a combined configuration from all registered configurations."""
        combined_config = {}
        for name, config in self._configs.items():
            combined_config[name] = config.get_config()
        return combined_config


# Singleton instance of ConfigManager
config_manager_instance = ConfigSingleton.get_instance(ConfigManager)