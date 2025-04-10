from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, to_json, struct

from .base_target import BaseTarget


class NoSQLTarget(BaseTarget):
    """Target for writing data to NoSQL databases (e.g., MongoDB)."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the NoSQL target.
        
        Args:
            config: Target configuration with NoSQL connection details
        """
        super().__init__(config)
        self.connection_uri = config.get("connection_uri")
        self.database = config.get("database")
        self.collection = config.get("collection")
        self.mode = config.get("mode", "overwrite")
        self.load_type = config.get("load_type", "full")
        self.options = config.get("options", {})
    
    def write(self, df: DataFrame) -> None:
        """
        Write data to the NoSQL target.
        
        Args:
            df: Input dataframe to write
        """
        if not self.connection_uri or not self.database or not self.collection:
            raise ValueError("Connection URI, database, and collection must be provided")
        
        # Prepare options
        write_options = {
            "uri": self.connection_uri,
            "database": self.database,
            "collection": self.collection
        }
        
        # Add any additional options
        write_options.update(self.options)
        
        # Handle different load types
        if self.load_type == "full":
            # Full load - simply write the data
            df.write.format("mongo").mode(self.mode).options(**write_options).save()
        
        elif self.load_type == "scd2":
            # SCD Type 2 load - handle historical data
            self._write_scd2(df, write_options)
        
        elif self.load_type == "cdc":
            # CDC load - handle change data capture
            self._write_cdc(df, write_options)
        
        else:
            raise ValueError(f"Unsupported load type: {self.load_type}")
    
    def validate_connection(self) -> bool:
        """
        Validate the connection to the NoSQL database.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        try:
            # Try to read a small amount of data to validate the connection
            options = {
                "uri": self.connection_uri,
                "database": self.database,
                "collection": self.collection
            }
            
            # Create a small test dataframe
            test_df = DataFrame.sparkSession.createDataFrame([("test",)], ["col1"])
            
            # Try to write to a temporary collection
            test_collection = "connection_test_" + str(int(time.time()))
            options["collection"] = test_collection
            
            test_df.write.format("mongo").mode("overwrite").options(**options).save()
            
            # Try to read from the temporary collection
            df.sparkSession.read.format("mongo").options(**options).load().limit(1).collect()
            
            return True
        except Exception as e:
            print(f"Connection validation failed: {str(e)}")
            return False
    
    def _write_scd2(self, df: DataFrame, write_options: Dict[str, str]) -> None:
        """
        Write data using SCD Type 2 approach.
        
        Args:
            df: Input dataframe to write
            write_options: MongoDB write options
        """
        # Get SCD Type 2 configuration
        key_fields = self.options.get("key_fields", [])
        effective_from_field = self.options.get("effective_from_field", "effective_from")
        effective_to_field = self.options.get("effective_to_field", "effective_to")
        current_flag_field = self.options.get("current_flag_field", "is_current")
        
        if not key_fields:
            raise ValueError("Key fields must be provided for SCD Type 2 load")
        
        # Add SCD Type 2 fields to the dataframe if they don't exist
        df_with_scd = df
        
        if effective_from_field not in df.columns:
            df_with_scd = df_with_scd.withColumn(effective_from_field, current_timestamp())
        
        if effective_to_field not in df.columns:
            df_with_scd = df_with_scd.withColumn(
                effective_to_field,
                lit("9999-12-31T23:59:59.999Z").cast("timestamp")
            )
        
        if current_flag_field not in df.columns:
            df_with_scd = df_with_scd.withColumn(current_flag_field, lit(True))
        
        # Try to read existing data from the target collection
        try:
            existing_df = df.sparkSession.read.format("mongo").options(**write_options).load()
            
            # Identify records that need to be updated
            join_condition = " AND ".join([f"new.{field} = old.{field}" for field in key_fields])
            
            # Update existing records (set effective_to and current_flag)
            existing_df.createOrReplaceTempView("existing_data")
            df_with_scd.createOrReplaceTempView("new_data")
            
            # SQL query to update existing records
            update_sql = f"""
            SELECT 
                old.*,
                new.{effective_from_field} AS {effective_to_field},
                FALSE AS {current_flag_field}
            FROM existing_data old
            JOIN new_data new
            ON {join_condition}
            WHERE old.{current_flag_field} = TRUE
            """
            
            updated_records = df.sparkSession.sql(update_sql)
            
            # Union the updated records with the new data
            result_df = updated_records.union(df_with_scd)
            
            # Write the result to the target collection
            result_df.write.format("mongo").mode("overwrite").options(**write_options).save()
        except Exception as e:
            print(f"Error reading existing data, assuming first load: {str(e)}")
            # If the collection doesn't exist, just write the new data
            df_with_scd.write.format("mongo").mode("overwrite").options(**write_options).save()
    
    def _write_cdc(self, df: DataFrame, write_options: Dict[str, str]) -> None:
        """
        Write data using CDC approach.
        
        Args:
            df: Input dataframe to write
            write_options: MongoDB write options
        """
        # Get CDC configuration
        key_fields = self.options.get("key_fields", [])
        operation_field = self.options.get("operation_field", "operation")
        timestamp_field = self.options.get("timestamp_field", "timestamp")
        
        if not key_fields:
            raise ValueError("Key fields must be provided for CDC load")
        
        if operation_field not in df.columns:
            raise ValueError(f"Operation field '{operation_field}' not found in dataframe")
        
        # Add timestamp field if it doesn't exist
        df_with_ts = df
        if timestamp_field not in df.columns:
            df_with_ts = df.withColumn(timestamp_field, current_timestamp())
        
        # Process each operation type
        df_with_ts.createOrReplaceTempView("cdc_data")
        
        # For MongoDB, we'll use the MongoDB connector's built-in upsert functionality
        
        # Insert and update operations can be handled with upsert
        upsert_df = df.sparkSession.sql(
            f"SELECT * FROM cdc_data WHERE {operation_field} IN ('INSERT', 'UPDATE')"
        )
        
        if upsert_df.count() > 0:
            # Remove the operation field
            upsert_df = upsert_df.drop(operation_field)
            
            # Write with upsert mode
            upsert_options = write_options.copy()
            upsert_options["replaceDocument"] = "false"  # Update only specified fields
            upsert_options["upsert"] = "true"
            
            # Specify key fields for upsert
            for i, field in enumerate(key_fields):
                upsert_options[f"writeConfig.upsertFields.{i}"] = field
            
            upsert_df.write.format("mongo").mode("append").options(**upsert_options).save()
        
        # Delete operations
        delete_df = df.sparkSession.sql(
            f"SELECT * FROM cdc_data WHERE {operation_field} = 'DELETE'"
        )
        
        if delete_df.count() > 0:
            # For each record to delete, construct a delete query
            delete_records = delete_df.collect()
            
            # Use PyMongo directly for delete operations
            from pymongo import MongoClient
            
            client = MongoClient(self.connection_uri)
            db = client[self.database]
            collection = db[self.collection]
            
            for record in delete_records:
                # Construct filter for key fields
                delete_filter = {field: record[field] for field in key_fields}
                
                # Execute delete operation
                collection.delete_many(delete_filter)
            
            client.close()