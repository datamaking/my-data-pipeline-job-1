from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, explode, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType

from .base_chunker import BaseChunker


class SemanticChunker(BaseChunker):
    """Chunker that splits text into semantic chunks based on content."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the semantic chunker.
        
        Args:
            config: Chunker configuration
        """
        super().__init__(config)
        self.min_chunk_size = config.get("min_chunk_size", 100)
        self.max_chunk_size = config.get("max_chunk_size", 1000)
        self.separator = config.get("separator", "\n\n")
        self.fallback_separators = config.get("fallback_separators", ["\n", ". ", "! ", "? "])
    
    def chunk(self, df: DataFrame) -> DataFrame:
        """
        Chunk the input dataframe into semantic chunks.
        
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
        def create_semantic_chunks(text):
            if not text:
                return None
            
            chunks = []
            
            # First try to split by the primary separator
            sections = text.split(self.separator)
            
            # If we only have one section, try fallback separators
            if len(sections) == 1:
                for separator in self.fallback_separators:
                    sections = text.split(separator)
                    if len(sections) > 1:
                        break
            
            # Process sections into chunks
            current_chunk = ""
            current_start = 0
            chunk_id = 0
            
            for section in sections:
                # If adding this section would exceed max_chunk_size, finalize the current chunk
                if len(current_chunk) + len(section) > self.max_chunk_size and len(current_chunk) >= self.min_chunk_size:
                    chunks.append((chunk_id, current_chunk, current_start, current_start + len(current_chunk)))
                    chunk_id += 1
                    current_chunk = section
                    current_start = text.find(section, current_start)
                else:
                    # Add separator back if this isn't the first section in the chunk
                    if current_chunk:
                        # Find which separator was used
                        separator_used = self.separator
                        if text.find(current_chunk + separator_used + section) == -1:
                            for separator in self.fallback_separators:
                                if text.find(current_chunk + separator + section) != -1:
                                    separator_used = separator
                                    break
                        current_chunk += separator_used
                    
                    current_chunk += section
            
            # Add the last chunk if it's not empty
            if current_chunk:
                chunks.append((chunk_id, current_chunk, current_start, current_start + len(current_chunk)))
            
            return chunks
        
        # Apply the UDF to create chunks
        chunked_df = df.withColumn("chunks", create_semantic_chunks(col(self.input_col)))
        
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