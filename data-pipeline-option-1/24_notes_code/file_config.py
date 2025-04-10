from typing import Dict, Any, List, Optional
from ..base_config import BaseConfig, ConfigSingleton


class FileSourceConfig(BaseConfig):
    """Configuration for file source systems."""
    
    def __init__(
        self,
        file_paths: List[str],
        file_format: str,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize file source configuration.
        
        Args:
            file_paths: List of file paths to read from
            file_format: Format of the files (e.g., 'csv', 'json', 'text', 'html')
            options: Additional options for reading files
        """
        self.file_paths = file_paths
        self.file_format = file_format
        self.options = options or {}
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "file_paths": self.file_paths,
            "file_format": self.file_format,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        if not self.file_paths:
            return False
        if not self.file_format:
            return False
        return True


# Example usage:
# file_config = ConfigSingleton.get_instance(
#     FileSourceConfig,
#     file_paths=["/path/to/file1.csv", "/path/to/file2.csv"],
#     file_format="csv",
#     options={"header": True, "delimiter": ","}
# )