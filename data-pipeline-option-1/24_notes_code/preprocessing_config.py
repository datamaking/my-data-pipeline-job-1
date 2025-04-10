from typing import Dict, Any, List, Optional
from .base_config import BaseConfig, ConfigSingleton


class PreprocessingConfig(BaseConfig):
    """Configuration for preprocessing module."""
    
    def __init__(
        self,
        steps: List[Dict[str, Any]],
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize preprocessing configuration.
        
        Args:
            steps: List of preprocessing steps with their parameters
            options: Additional options for preprocessing
        """
        self.steps = steps
        self.options = options or {}
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "steps": self.steps,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        if not self.steps:
            return False
        
        # Validate each step has a type
        for step in self.steps:
            if "type" not in step:
                return False
            
            # Validate specific step types
            if step["type"] == "html_parser":
                if "extract_tags" not in step:
                    return False
            elif step["type"] == "data_cleaner":
                if "operations" not in step:
                    return False
        
        return True


# Example usage:
# preprocessing_config = ConfigSingleton.get_instance(
#     PreprocessingConfig,
#     steps=[
#         {
#             "type": "html_parser",
#             "extract_tags": ["p", "h1", "h2", "h3"],
#             "remove_scripts": True
#         },
#         {
#             "type": "data_cleaner",
#             "operations": ["remove_stopwords", "lowercase", "lemmatize"],
#             "custom_stopwords": ["example", "test"]
#         }
#     ],
#     options={"parallel": True, "num_partitions": 100}
# )