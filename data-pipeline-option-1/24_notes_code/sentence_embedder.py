from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, array, lit
from pyspark.sql.types import ArrayType, FloatType

from .base_embedder import BaseEmbedder


class SentenceEmbedder(BaseEmbedder):
    """Embedder that creates sentence embeddings using transformer models."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the sentence embedder.
        
        Args:
            config: Embedder configuration
        """
        super().__init__(config)
        self.model_name = config.get("model_name", "all-MiniLM-L6-v2")
        self.max_seq_length = config.get("max_seq_length", 256)
        self.batch_size = config.get("batch_size", 32)
        self.device = config.get("device", "cpu")
    
    def create_embeddings(self, df: DataFrame) -> DataFrame:
        """
        Create sentence embeddings for the input dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Dataframe with sentence embeddings
        """
        if self.input_col not in df.columns:
            print(f"Warning: Input column '{self.input_col}' not found in dataframe")
            return df
        
        # Define UDF for sentence embedding
        @udf(ArrayType(FloatType()))
        def create_sentence_embedding(text):
            if not text:
                return None
            
            try:
                from sentence_transformers import SentenceTransformer
                
                # Initialize the model
                model = SentenceTransformer(self.model_name, device=self.device)
                
                # Set max sequence length
                model.max_seq_length = self.max_seq_length
                
                # Create embedding
                embedding = model.encode(text)
                
                # Convert to list for Spark
                return embedding.tolist()
            except Exception as e:
                print(f"Error creating sentence embedding: {str(e)}")
                return None
        
        # Apply the UDF to create embeddings
        result_df = df.withColumn(self.output_col, create_sentence_embedding(col(self.input_col)))
        
        return result_df
    
    def create_embeddings_batched(self, df: DataFrame) -> DataFrame:
        """
        Create sentence embeddings for the input dataframe in batches.
        This is more efficient for large datasets.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Dataframe with sentence embeddings
        """
        if self.input_col not in df.columns:
            print(f"Warning: Input column '{self.input_col}' not found in dataframe")
            return df
        
        # Collect texts to process in Python
        texts = [row[self.input_col] for row in df.select(self.input_col).collect()]
        
        # Create embeddings in batches
        try:
            from sentence_transformers import SentenceTransformer
            import numpy as np
            
            # Initialize the model
            model = SentenceTransformer(self.model_name, device=self.device)
            
            # Set max sequence length
            model.max_seq_length = self.max_seq_length
            
            # Create embeddings in batches
            embeddings = model.encode(texts, batch_size=self.batch_size)
            
            # Convert embeddings to list of lists
            embedding_lists = [embedding.tolist() for embedding in embeddings]
            
            # Create a list of (text, embedding) tuples
            text_embedding_pairs = list(zip(texts, embedding_lists))
            
            # Create a new dataframe with embeddings
            from pyspark.sql import Row
            embedding_rows = [Row(text=text, embedding=embedding) for text, embedding in text_embedding_pairs]
            embedding_df = df.sparkSession.createDataFrame(embedding_rows)
            
            # Join with original dataframe
            result_df = df.join(
                embedding_df,
                df[self.input_col] == embedding_df.text
            ).drop(embedding_df.text)
            
            return result_df
        except Exception as e:
            print(f"Error creating sentence embeddings in batches: {str(e)}")
            
            # Fall back to non-batched approach
            return self.create_embeddings(df)