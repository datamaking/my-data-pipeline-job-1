from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, array
from pyspark.sql.types import ArrayType, FloatType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer, StopWordsRemover

from .base_embedder import BaseEmbedder


class TFIDFEmbedder(BaseEmbedder):
    """Embedder that creates TF-IDF embeddings."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the TF-IDF embedder.
        
        Args:
            config: Embedder configuration
        """
        super().__init__(config)
        self.max_features = config.get("max_features", 10000)
        self.min_df = config.get("min_df", 5)
        self.max_df = config.get("max_df", 0.95)
        self.binary = config.get("binary", False)
        self.use_idf = config.get("use_idf", True)
        self.ngram_range = config.get("ngram_range", (1, 1))
        self.tokenized_col = f"{self.input_col}_tokenized"
        self.filtered_col = f"{self.input_col}_filtered"
        self.tf_col = f"{self.input_col}_tf"
    
    def create_embeddings(self, df: DataFrame) -> DataFrame:
        """
        Create TF-IDF embeddings for the input dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Dataframe with TF-IDF embeddings
        """
        if self.input_col not in df.columns:
            print(f"Warning: Input column '{self.input_col}' not found in dataframe")
            return df
        
        # Tokenize text
        tokenizer = Tokenizer(inputCol=self.input_col, outputCol=self.tokenized_col)
        tokenized_df = tokenizer.transform(df)
        
        # Remove stopwords
        remover = StopWordsRemover(inputCol=self.tokenized_col, outputCol=self.filtered_col)
        filtered_df = remover.transform(tokenized_df)
        
        # Create term frequency vectors
        if self.ngram_range[1] > 1:
            # Use CountVectorizer for n-grams
            cv = CountVectorizer(
                inputCol=self.filtered_col,
                outputCol=self.tf_col,
                vocabSize=self.max_features,
                minDF=self.min_df,
                maxDF=self.max_df,
                binary=self.binary
            )
            cv_model = cv.fit(filtered_df)
            tf_df = cv_model.transform(filtered_df)
        else:
            # Use HashingTF for unigrams
            hashingTF = HashingTF(
                inputCol=self.filtered_col,
                outputCol=self.tf_col,
                numFeatures=self.max_features,
                binary=self.binary
            )
            tf_df = hashingTF.transform(filtered_df)
        
        # Create IDF vectors if configured
        if self.use_idf:
            idf = IDF(inputCol=self.tf_col, outputCol=self.output_col, minDocFreq=self.min_df)
            idf_model = idf.fit(tf_df)
            result_df = idf_model.transform(tf_df)
        else:
            # Use TF vectors directly
            result_df = tf_df.withColumnRenamed(self.tf_col, self.output_col)
        
        # Drop intermediate columns
        result_df = result_df.drop(self.tokenized_col, self.filtered_col, self.tf_col)
        
        return result_df