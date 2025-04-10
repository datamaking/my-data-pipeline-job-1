from typing import Dict, Any, List, Optional, Literal
from ..base_config import BaseConfig, ConfigSingleton


class FileTargetConfig(BaseConfig):
    """Configuration for file target systems."""
    
    def __init__(
        self,
        output_dir: str,
        file_format: Literal["csv", "json", "parquet", "text", "html"],
        mode: Literal["overwrite", "append", "error", "ignore"] = "overwrite",
        partition_by: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize file target configuration.
        
        Args:
            output_dir: Directory to write files to
            file_format: Format of the output files
            mode: Write mode (overwrite, append, error, ignore)
            partition_by: Columns to partition the output by
            options: Additional options for writing files
        """
        self.output_dir = output_dir
        self.file_format = file_format
        self.mode = mode
        self.partition_by = partition_by or []
        self.options = options or {}
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "output_dir": self.output_dir,
            "file_format": self.file_format,
            "mode": self.mode,
            "partition_by": self.partition_by,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        valid_formats = ["csv", "json", "parquet", "text", "html"]
        valid_modes = ["overwrite", "append", "error", "ignore"]
        
        if not self.output_dir:
            return False
        if self.file_format not in valid_formats:
            return False
        if self.mode not in valid_modes:
            return False
        
        return True


# Example usage:
# file_target_config = ConfigSingleton.get_instance(
#     FileTargetConfig,
#     output_dir="/path/to/output",
#     file_format="parquet",
#     mode="overwrite",
#     partition_by=["date", "category"],
#     options={"compression": "snappy"}
# )