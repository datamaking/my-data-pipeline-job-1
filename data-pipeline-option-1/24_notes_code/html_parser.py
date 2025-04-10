from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from .base_preprocessor import BasePreprocessor


class HTMLParser(BasePreprocessor):
    """Preprocessor for parsing HTML content."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the HTML parser.
        
        Args:
            config: HTML parser configuration
        """
        super().__init__(config)
        self.extract_tags = config.get("extract_tags", [])
        self.remove_scripts = config.get("remove_scripts", True)
        self.remove_styles = config.get("remove_styles", True)
        self.input_col = config.get("input_col", "html")
        self.output_col = config.get("output_col", "text")
    
    def process(self, df: DataFrame) -> DataFrame:
        """
        Process the input dataframe by parsing HTML content.
        
        Args:
            df: Input dataframe with HTML content
            
        Returns:
            DataFrame: Dataframe with parsed text
        """
        # Define UDF for HTML parsing
        @udf(StringType())
        def parse_html(html_content):
            if not html_content:
                return None
            
            try:
                from bs4 import BeautifulSoup
                
                # Parse HTML
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Remove scripts if configured
                if self.remove_scripts:
                    for script in soup.find_all('script'):
                        script.decompose()
                
                # Remove styles if configured
                if self.remove_styles:
                    for style in soup.find_all('style'):
                        style.decompose()
                
                # Extract specific tags if configured
                if self.extract_tags:
                    extracted_text = []
                    for tag in self.extract_tags:
                        elements = soup.find_all(tag)
                        for element in elements:
                            extracted_text.append(element.get_text(strip=True))
                    return ' '.join(extracted_text)
                
                # Otherwise, extract all text
                return soup.get_text(separator=' ', strip=True)
            except Exception as e:
                print(f"Error parsing HTML: {str(e)}")
                return None
        
        # Apply the UDF to the dataframe
        if self.input_col in df.columns:
            return df.withColumn(self.output_col, parse_html(df[self.input_col]))
        else:
            print(f"Warning: Input column '{self.input_col}' not found in dataframe")
            return df