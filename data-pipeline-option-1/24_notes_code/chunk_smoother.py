from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, lag, lead, when
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

from .base_chunker import BaseChunker


class ChunkSmoother(BaseChunker):
    """Smoother that adjusts chunk boundaries for better coherence."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the chunk smoother.
        
        Args:
            config: Smoother configuration
        """
        super().__init__(config)
        self.enabled = config.get("enabled", True)
        self.method = config.get("method", "sentence_boundary")
        self.max_lookback = config.get("max_lookback", 100)
        self.max_lookahead = config.get("max_lookahead", 100)
    
    def is_enabled(self) -> bool:
        """
        Check if smoothing is enabled.
        
        Returns:
            bool: True if smoothing is enabled, False otherwise
        """
        return self.enabled
    
    def smooth(self, df: DataFrame) -> DataFrame:
        """
        Smooth the chunks in the input dataframe.
        
        Args:
            df: Input dataframe with chunks
            
        Returns:
            DataFrame: Dataframe with smoothed chunks
        """
        if not self.enabled:
            return df
        
        if self.output_col not in df.columns:
            print(f"Warning: Output column '{self.output_col}' not found in dataframe")
            return df
        
        if self.method == "sentence_boundary":
            return self._smooth_sentence_boundary(df)
        elif self.method == "overlap":
            return self._smooth_overlap(df)
        else:
            print(f"Warning: Unknown smoothing method '{self.method}'")
            return df
    
    def _smooth_sentence_boundary(self, df: DataFrame) -> DataFrame:
        """
        Smooth chunks by adjusting boundaries to sentence boundaries.
        
        Args:
            df: Input dataframe with chunks
            
        Returns:
            DataFrame: Dataframe with smoothed chunks
        """
        # Define UDF for sentence boundary smoothing
        @udf(StringType())
        def adjust_to_sentence_boundary(text, is_first_chunk, is_last_chunk):
            if not text:
                return text
            
            import re
            
            # Don't adjust first chunk at the beginning
            if not is_first_chunk:
                # Find the first sentence boundary
                sentence_end_pattern = r'[.!?]\s+'
                match = re.search(sentence_end_pattern, text[:self.max_lookback])
                
                if match:
                    # Adjust the start of the chunk to the sentence boundary
                    start_idx = match.end()
                    text = text[start_idx:]
            
            # Don't adjust last chunk at the end
            if not is_last_chunk:
                # Find the last sentence boundary
                reversed_text = text[::-1]
                sentence_start_pattern = r'\s+[.!?]'
                match = re.search(sentence_start_pattern, reversed_text[:self.max_lookahead])
                
                if match:
                    # Adjust the end of the chunk to the sentence boundary
                    end_idx = len(text) - match.end()
                    text = text[:end_idx]
            
            return text
        
        # Create a window specification for determining first and last chunks
        window_spec = Window.partitionBy(self.id_col).orderBy("chunk_id")
        
        # Add flags for first and last chunks
        df_with_flags = df.withColumn(
            "is_first_chunk",
            lag("chunk_id", 1, -1).over(window_spec) == -1
        ).withColumn(
            "is_last_chunk",
            lead("chunk_id", 1, -1).over(window_spec) == -1
        )
        
        # Apply the UDF to adjust chunk boundaries
        smoothed_df = df_with_flags.withColumn(
            self.output_col,
            adjust_to_sentence_boundary(
                col(self.output_col),
                col("is_first_chunk"),
                col("is_last_chunk")
            )
        ).drop("is_first_chunk", "is_last_chunk")
        
        return smoothed_df
    
    def _smooth_overlap(self, df: DataFrame) -> DataFrame:
        """
        Smooth chunks by adding overlap between adjacent chunks.
        
        Args:
            df: Input dataframe with chunks
            
        Returns:
            DataFrame: Dataframe with smoothed chunks
        """
        # Create a window specification for accessing adjacent chunks
        window_spec = Window.partitionBy(self.id_col).orderBy("chunk_id")
        
        # Add previous and next chunk text
        df_with_adjacent = df.withColumn(
            "prev_chunk_text",
            lag(self.output_col, 1).over(window_spec)
        ).withColumn(
            "next_chunk_text",
            lead(self.output_col, 1).over(window_spec)
        )
        
        # Define UDF for overlap smoothing
        @udf(StringType())
        def add_overlap(text, prev_text, next_text):
            if not text:
                return text
            
            result = text
            
            # Add overlap from previous chunk
            if prev_text:
                # Get the last part of the previous chunk
                overlap_size = min(self.max_lookback, len(prev_text))
                prev_overlap = prev_text[-overlap_size:]
                
                # Find a good boundary (e.g., space)
                space_idx = prev_overlap.find(' ')
                if space_idx > 0:
                    prev_overlap = prev_overlap[space_idx:]
                
                result = prev_overlap + result
            
            # Add overlap to next chunk
            if next_text:
                # Get the first part of the next chunk
                overlap_size = min(self.max_lookahead, len(next_text))
                next_overlap = next_text[:overlap_size]
                
                # Find a good boundary (e.g., space)
                space_idx = next_overlap.rfind(' ')
                if space_idx > 0:
                    next_overlap = next_overlap[:space_idx]
                
                result = result + next_overlap
            
            return result
        
        # Apply the UDF to add overlap
        smoothed_df = df_with_adjacent.withColumn(
            self.output_col,
            add_overlap(
                col(self.output_col),
                col("prev_chunk_text"),
                col("next_chunk_text")
            )
        ).drop("prev_chunk_text", "next_chunk_text")
        
        return smoothed_df