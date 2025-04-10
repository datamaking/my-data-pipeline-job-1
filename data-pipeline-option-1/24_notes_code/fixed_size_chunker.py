from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, explode, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType

from .base_chunker import BaseChunker


class FixedSizeChunker(BaseChunker):
    """Chunker that splits text into fixed-size chunks."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the fixed-size chunker.
        
        Args:
            config: Chunker configuration
        """
        super().__init__(config)
        self.chunk_size = config.get("chunk_size", 1000)
        self.overlap = config.get("overlap", 0)
        self.preserve_words = config.get("preserve_words", True)
    
    def chunk(self, df: DataFrame) -> DataFrame:
        """
        Chunk the input dataframe into fixed-size chunks.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame: Dataframe with chunks
        """
        if self.input_col not in df.columns:
            print(f"Warning: Input column '{self.input_col}' not found in dataframe")
            return df
        
        # Define schema for the chunks
        chunk_schema = ArrayType(
            StructType([
                StructField("chunk_id", IntegerType(), False),
                StructField("text", StringType(), True),
                StructField("start_idx", IntegerType(), True),
                StructField("end_idx", IntegerType(), True)
            ])
        )
        
        # Define UDF for chunking
        @udf(chunk_schema)
        def create_chunks(text):
            if not text:
                return None
            
            chunks = []
            text_length = len(text)
            
            # If text is shorter than chunk size, return it as a single chunk
            if text_length <= self.chunk_size:
                chunks.append((0, text, 0, text_length))
                return chunks
            
            # Calculate effective chunk size (considering overlap)
            effective_chunk_size = self.chunk_size - self.overlap
            
            # Create chunks
            for i in range(0, text_length, effective_chunk_size):
                # Calculate start and end indices
                start_idx = i
                end_idx = min(i + self.chunk_size, text_length)
                
                # Get chunk text
                chunk_text = text[start_idx:end_idx]
                
                # Adjust chunk boundaries to preserve words if configured
                if self.preserve_words and i > 0:
                    # Find the first space after the start index
                    first_space = chunk_text.find(' ')
                    if first_space > 0:
                        start_idx += first_space + 1
                        chunk_text = text[start_idx:end_idx]
                
                if self.preserve_words and end_idx < text_length:
                    # Find the last space before the end index
                    last_space = chunk_text.rfind(' ')
                    if last_space > 0:
                        end_idx = start_idx + last_space
                        chunk_text = text[start_idx:end_idx]
                
                # Add chunk to the list
                chunk_id = len(chunks)
                chunks.append((chunk_id, chunk_text, start_idx, end_idx))
                
                # Break if we've reached the end of the text
                if end_idx >= text_length:
                    break
            
            return chunks
        
        # Apply the UDF to create chunks
        chunked_df = df.withColumn("chunks", create_chunks(col(self.input_col)))
        
        # Explode the chunks array to create one row per chunk
        exploded_df = chunked_df.select(
            "*",
            explode("chunks").alias("chunk_struct")
        ).select(
            "*",
            col("chunk_struct.chunk_id").alias("chunk_id"),
            col("chunk_struct.text").alias(self.output_col),
            col("chunk_struct.start_idx").alias("start_idx"),
            col("chunk_struct.end_idx").alias("end_idx")
        ).drop("chunks", "chunk_struct")
        
        return exploded_df