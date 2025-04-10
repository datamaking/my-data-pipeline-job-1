from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

from .base_target import BaseTarget


class HiveTarget(BaseTarget):
    """Target for writing data to Hive tables."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Hive target.
        
        Args:
            config: Target configuration with Hive connection details
        """
        super().__init__(config)
        self.database = config.get("database")
        self.table = config.get("table")
        self.format = config.get("format", "parquet")
        self.mode = config.get("mode", "overwrite")
        self.partition_by = config.get("partition_by", [])
        self.load_type = config.get("load_type", "full")
        self.options = config.get("options", {})
        
        # Configure Spark to use Hive metastore
        if "metastore_uri" in config:
            self.metastore_uri = config.get("metastore_uri")
            self.spark.conf.set("hive.metastore.uris", self.metastore_uri)
    
    def write(self, df: DataFrame) -> None:
        """
        Write data to the Hive target.
        
        Args:
            df: Input dataframe to write
        """
        if not self.database or not self.table:
            raise ValueError("Database and table must be provided")
        
        # Construct the fully qualified table name
        full_table_name = f"{self.database}.{self.table}"
        
        # Handle different load types
        if self.load_type == "full":
            # Full load - simply write the data
            writer = df.write.format(self.format).mode(self.mode)
            
            # Add options
            for key, value in self.options.items():
                writer = writer.option(key, value)
            
            # Add partitioning if specified
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            # Write the data
            writer.saveAsTable(full_table_name)
        
        elif self.load_type == "scd2":
            # SCD Type 2 load - handle historical data
            self._write_scd2(df, full_table_name)
        
        elif self.load_type == "cdc":
            # CDC load - handle change data capture
            self._write_cdc(df, full_table_name)
        
        else:
            raise ValueError(f"Unsupported load type: {self.load_type}")
    
    def validate_connection(self) -> bool:
        """
        Validate the connection to Hive.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        try:
            # Try to execute a simple query to validate the connection
            df.sparkSession.sql("SHOW DATABASES").limit(1).collect()
            
            # Check if the database exists
            databases = df.sparkSession.sql("SHOW DATABASES").collect()
            database_names = [row.databaseName for row in databases]
            
            if self.database not in database_names:
                print(f"Database '{self.database}' does not exist")
                return False
            
            return True
        except Exception as e:
            print(f"Connection validation failed: {str(e)}")
            return False
    
    def _write_scd2(self, df: DataFrame, full_table_name: str) -> None:
        """
        Write data using SCD Type 2 approach.
        
        Args:
            df: Input dataframe to write
            full_table_name: Fully qualified target table name
        """
        # Get SCD Type 2 configuration
        key_columns = self.options.get("key_columns", [])
        effective_from_column = self.options.get("effective_from_column", "effective_from")
        effective_to_column = self.options.get("effective_to_column", "effective_to")
        current_flag_column = self.options.get("current_flag_column", "is_current")
        
        if not key_columns:
            raise ValueError("Key columns must be provided for SCD Type 2 load")
        
        # Add SCD Type 2 columns to the dataframe if they don't exist
        df_with_scd = df
        
        if effective_from_column not in df.columns:
            df_with_scd = df_with_scd.withColumn(effective_from_column, current_timestamp())
        
        if effective_to_column not in df.columns:
            df_with_scd = df_with_scd.withColumn(
                effective_to_column,
                lit("9999-12-31 23:59:59").cast("timestamp")
            )
        
        if current_flag_column not in df.columns:
            df_with_scd = df_with_scd.withColumn(current_flag_column, lit(True))
        
        # Check if the target table exists
        table_exists = False
        try:
            df.sparkSession.sql(f"DESCRIBE {full_table_name}")
            table_exists = True
        except:
            pass
        
        if table_exists:
            # Read existing data from the target table
            existing_df = df.sparkSession.sql(f"SELECT * FROM {full_table_name}")
            
            # Identify records that need to be updated
            join_condition = " AND ".join([f"new.{col} = old.{col}" for col in key_columns])
            
            # Update existing records (set effective_to and current_flag)
            existing_df.createOrReplaceTempView("existing_data")
            df_with_scd.createOrReplaceTempView("new_data")
            
            # SQL query to update existing records
            update_sql = f"""
            SELECT 
                old.*,
                new.{effective_from_column} AS {effective_to_column},
                FALSE AS {current_flag_column}
            FROM existing_data old
            JOIN new_data new
            ON {join_condition}
            WHERE old.{current_flag_column} = TRUE
            """
            
            updated_records = df.sparkSession.sql(update_sql)
            
            # Union the updated records with the new data
            result_df = updated_records.union(df_with_scd)
            
            # Write the result to the target table
            writer = result_df.write.format(self.format).mode("overwrite")
            
            # Add options
            for key, value in self.options.items():
                if key not in ["key_columns", "effective_from_column", "effective_to_column", "current_flag_column"]:
                    writer = writer.option(key, value)
            
            # Add partitioning if specified
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            # Write the data
            writer.saveAsTable(full_table_name)
        else:
            # If the table doesn't exist, just write the new data
            writer = df_with_scd.write.format(self.format).mode("overwrite")
            
            # Add options
            for key, value in self.options.items():
                if key not in ["key_columns", "effective_from_column", "effective_to_column", "current_flag_column"]:
                    writer = writer.option(key, value)
            
            # Add partitioning if specified
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            # Write the data
            writer.saveAsTable(full_table_name)
    
    def _write_cdc(self, df: DataFrame, full_table_name: str) -> None:
        """
        Write data using CDC approach.
        
        Args:
            df: Input dataframe to write
            full_table_name: Fully qualified target table name
        """
        # Get CDC configuration
        key_columns = self.options.get("key_columns", [])
        operation_column = self.options.get("operation_column", "operation")
        timestamp_column = self.options.get("timestamp_column", "timestamp")
        
        if not key_columns:
            raise ValueError("Key columns must be provided for CDC load")
        
        if operation_column not in df.columns:
            raise ValueError(f"Operation column '{operation_column}' not found in dataframe")
        
        # Add timestamp column if it doesn't exist
        df_with_ts = df
        if timestamp_column not in df.columns:
            df_with_ts = df.withColumn(timestamp_column, current_timestamp())
        
        # Check if the target table exists
        table_exists = False
        try:
            df.sparkSession.sql(f"DESCRIBE {full_table_name}")
            table_exists = True
        except:
            pass
        
        # Process each operation type
        df_with_ts.createOrReplaceTempView("cdc_data")
        
        if table_exists:
            # Read existing data from the target table
            existing_df = df.sparkSession.sql(f"SELECT * FROM {full_table_name}")
            existing_df.createOrReplaceTempView("existing_data")
            
            # Insert operations
            insert_df = df.sparkSession.sql(f"SELECT * FROM cdc_data WHERE {operation_column} = 'INSERT'")
            insert_df = insert_df.drop(operation_column)
            
            # Update operations
            update_condition = " AND ".join([f"cdc.{col} = existing.{col}" for col in key_columns])
            update_sql = f"""
            SELECT 
                cdc.*
            FROM cdc_data cdc
            JOIN existing_data existing
            ON {update_condition}
            WHERE cdc.{operation_column} = 'UPDATE'
            """
            update_df = df.sparkSession.sql(update_sql)
            update_df = update_df.drop(operation_column)
            
            # Delete operations
            delete_condition = " AND ".join([f"cdc.{col} = existing.{col}" for col in key_columns])
            delete_sql = f"""
            SELECT 
                existing.*
            FROM existing_data existing
            JOIN cdc_data cdc
            ON {delete_condition}
            WHERE cdc.{operation_column} = 'DELETE'
            """
            delete_df = df.sparkSession.sql(delete_sql)
            
            # Combine operations
            # Keep all existing records except those being deleted
            keep_condition = " AND ".join([f"NOT (existing.{col} = cdc.{col})" for col in key_columns])
            keep_sql = f"""
            SELECT 
                existing.*
            FROM existing_data existing
            LEFT JOIN (SELECT * FROM cdc_data WHERE {operation_column} = 'DELETE') cdc
            ON {delete_condition}
            WHERE cdc.{operation_column} IS NULL
            """
            keep_df = df.sparkSession.sql(keep_sql)
            
            # Union all records to keep, insert, and update
            result_df = keep_df.union(insert_df).union(update_df)
            
            # Write the result to the target table
            writer = result_df.write.format(self.format).mode("overwrite")
            
            # Add options
            for key, value in self.options.items():
                if key not in ["key_columns", "operation_column", "timestamp_column"]:
                    writer = writer.option(key, value)
            
            # Add partitioning if specified
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            # Write the data
            writer.saveAsTable(full_table_name)
        else:
            # If the table doesn't exist, just write the new data (INSERT operations only)
            insert_df = df.sparkSession.sql(f"SELECT * FROM cdc_data WHERE {operation_column} = 'INSERT'")
            insert_df = insert_df.drop(operation_column)
            
            # Write the result to the target table
            writer = insert_df.write.format(self.format).mode("overwrite")
            
            # Add options
            for key, value in self.options.items():
                if key not in ["key_columns", "operation_column", "timestamp_column"]:
                    writer = writer.option(key, value)
            
            # Add partitioning if specified
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            # Write the data
            writer.saveAsTable(full_table_name)