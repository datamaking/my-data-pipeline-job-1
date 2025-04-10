from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, lower, regexp_replace
from pyspark.sql.types import StringType, ArrayType
import re

from .base_preprocessor import BasePreprocessor


class DataCleaner(BasePreprocessor):
    """Preprocessor for cleaning text data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data cleaner.
        
        Args:
            config: Data cleaner configuration
        """
        super().__init__(config)
        self.operations = config.get("operations", [])
        self.input_col = config.get("input_col", "text")
        self.output_col = config.get("output_col", "cleaned_text")
        self.custom_stopwords = config.get("custom_stopwords", [])
        self.language = config.get("language", "english")
    
    def process(self, df: DataFrame) -> DataFrame:
        """
        Process the input dataframe by cleaning text data.
        
        Args:
            df: Input dataframe with text data
            
        Returns:
            DataFrame: Dataframe with cleaned text
        """
        if self.input_col not in df.columns:
            print(f"Warning: Input column '{self.input_col}' not found in dataframe")
            return df
        
        result_df = df
        
        # Apply operations in sequence
        for operation in self.operations:
            result_df = self._apply_operation(result_df, operation)
        
        return result_df
    
    def _apply_operation(self, df: DataFrame, operation: str) -> DataFrame:
        """
        Apply a cleaning operation to the dataframe.
        
        Args:
            df: Input dataframe
            operation: Cleaning operation to apply
            
        Returns:
            DataFrame: Dataframe with the operation applied
        """
        if operation == "lowercase":
            return df.withColumn(self.output_col, lower(col(self.input_col)))
        
        elif operation == "remove_punctuation":
            return df.withColumn(
                self.output_col,
                regexp_replace(col(self.input_col), "[^\\w\\s]", "")
            )
        
        elif operation == "remove_numbers":
            return df.withColumn(
                self.output_col,
                regexp_replace(col(self.input_col), "\\d+", "")
            )
        
        elif operation == "remove_whitespace":
            return df.withColumn(
                self.output_col,
                regexp_replace(col(self.input_col), "\\s+", " ")
            )
        
        elif operation == "remove_stopwords":
            # Define UDF for stopword removal
            @udf(StringType())
            def remove_stopwords(text):
                if not text:
                    return None
                
                try:
                    import nltk
                    from nltk.corpus import stopwords
                    
                    # Download stopwords if not already downloaded
                    try:
                        nltk.data.find(f'corpora/stopwords')
                    except LookupError:
                        nltk.download('stopwords')
                    
                    # Get stopwords
                    stop_words = set(stopwords.words(self.language))
                    
                    # Add custom stopwords
                    stop_words.update(self.custom_stopwords)
                    
                    # Remove stopwords
                    words = text.split()
                    filtered_words = [word for word in words if word.lower() not in stop_words]
                    
                    return ' '.join(filtered_words)
                except Exception as e:
                    print(f"Error removing stopwords: {str(e)}")
                    return text
            
            return df.withColumn(self.output_col, remove_stopwords(col(self.input_col)))
        
        elif operation == "lemmatize":
            # Define UDF for lemmatization
            @udf(StringType())
            def lemmatize_text(text):
                if not text:
                    return None
                
                try:
                    import nltk
                    from nltk.stem import WordNetLemmatizer
                    
                    # Download WordNet if not already downloaded
                    try:
                        nltk.data.find('corpora/wordnet')
                    except LookupError:
                        nltk.download('wordnet')
                    
                    # Initialize lemmatizer
                    lemmatizer = WordNetLemmatizer()
                    
                    # Lemmatize words
                    words = text.split()
                    lemmatized_words = [lemmatizer.lemmatize(word) for word in words]
                    
                    return ' '.join(lemmatized_words)
                except Exception as e:
                    print(f"Error lemmatizing text: {str(e)}")
                    return text
            
            return df.withColumn(self.output_col, lemmatize_text(col(self.input_col)))
        
        elif operation == "stem":
            # Define UDF for stemming
            @udf(StringType())
            def stem_text(text):
                if not text:
                    return None
                
                try:
                    from nltk.stem.porter import PorterStemmer
                    
                    # Initialize stemmer
                    stemmer = PorterStemmer()
                    
                    # Stem words
                    words = text.split()
                    stemmed_words = [stemmer.stem(word) for word in words]
                    
                    return ' '.join(stemmed_words)
                except Exception as e:
                    print(f"Error stemming text: {str(e)}")
                    return text
            
            return df.withColumn(self.output_col, stem_text(col(self.input_col)))
        
        elif operation == "tokenize":
            # Define UDF for tokenization
            @udf(ArrayType(StringType()))
            def tokenize_text(text):
                if not text:
                    return None
                
                try:
                    import nltk
                    
                    # Download punkt if not already downloaded
                    try:
                        nltk.data.find('tokenizers/punkt')
                    except LookupError:
                        nltk.download('punkt')
                    
                    # Tokenize text
                    return nltk.word_tokenize(text)
                except Exception as e:
                    print(f"Error tokenizing text: {str(e)}")
                    return text.split()
            
            return df.withColumn(self.output_col, tokenize_text(col(self.input_col)))
        
        else:
            print(f"Warning: Unknown operation '{operation}'")
            return df