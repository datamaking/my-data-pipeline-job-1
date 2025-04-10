import unittest
from unittest.mock import patch, MagicMock

from config.base_config import BaseConfig, ConfigSingleton
from config.config_manager import ConfigManager


class TestConfigBase(unittest.TestCase):
    """Test cases for base configuration classes."""
    
    def test_config_singleton(self):
        """Test that ConfigSingleton returns the same instance."""
        # Create a mock config class
        mock_config_class = MagicMock()
        
        # Get instances
        instance1 = ConfigSingleton.get_instance(mock_config_class)
        instance2 = ConfigSingleton.get_instance(mock_config_class)
        
        # Assert that they are the same instance
        self.assertIs(instance1, instance2)
        
        # Assert that the mock class was called only once
        mock_config_class.assert_called_once()


class TestConfigManager(unittest.TestCase):
    """Test cases for ConfigManager."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a mock config
        self.mock_config = MagicMock(spec=BaseConfig)
        self.mock_config.validate.return_value = True
        self.mock_config.get_config.return_value = {"key": "value"}
        
        # Create a config manager
        self.config_manager = ConfigManager()
    
    def test_add_config(self):
        """Test adding a configuration."""
        # Add the mock config
        self.config_manager.add_config("test", self.mock_config)
        
        # Assert that the config was added
        self.assertIn("test", self.config_manager.get_all_configs())
        self.assertEqual(self.config_manager.get_config("test"), self.mock_config)
    
    def test_add_invalid_config(self):
        """Test adding an invalid configuration."""
        # Make the mock config invalid
        self.mock_config.validate.return_value = False
        
        # Assert that adding an invalid config raises an exception
        with self.assertRaises(ValueError):
            self.config_manager.add_config("test", self.mock_config)
    
    def test_get_combined_config(self):
        """Test getting a combined configuration."""
        # Add multiple configs
        self.config_manager.add_config("test1", self.mock_config)
        
        mock_config2 = MagicMock(spec=BaseConfig)
        mock_config2.validate.return_value = True
        mock_config2.get_config.return_value = {"key2": "value2"}
        self.config_manager.add_config("test2", mock_config2)
        
        # Get combined config
        combined_config = self.config_manager.get_combined_config()
        
        # Assert that the combined config contains all configs
        self.assertEqual(combined_config, {
            "test1": {"key": "value"},
            "test2": {"key2": "value2"}
        })


if __name__ == "__main__":
    unittest.main()