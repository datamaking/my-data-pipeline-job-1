from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

from .base_target import BaseTarget


class RDBMSTarget(BaseTarget):
    """Target for writing data to RDBMS databases."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the RDBMS target.
        
        Args:
            config: Target configuration with JDBC connection details
        """
        super().__init__(config)
        self.jdbc_url = config.get("jdbc_url")
        self.driver = config.get("driver")
        self.user = config.get("user")
        self.password = config.get("password")
        self.table = config.get("table")
        self.schema = config.get("schema")
        self.mode = config.get("mode", "overwrite")
        self.load_type = config.get("load_type", "full")
        self.batch_size = config.get("batch_size", 10000)
        self.options = config.get("options", {})
    
    def write(self, df: DataFrame) -> None:
        """
        Write data to the RDBMS target.
        
        Args:
            df: Input dataframe to write
        """
        if not self.jdbc_url or not self.table:
            raise ValueError("JDBC URL and table must be provided")
        
        # Prepare connection properties
        conn_properties = {
            "driver": self.driver,
            "user": self.user,
            "password": self.password,
            "batchsize": str(self.batch_size)
        }
        
        # Add any additional options
        conn_properties.update(self.options)
        
        # Prepare table name with schema if provided
        table_name = f"{self.schema}.{self.table}" if self.schema else self.table
        
        # Handle different load types
        if self.load_type == "full":
            # Full load - simply write the data
            df.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode=self.mode,
                properties=conn_properties
            )
        
        elif self.load_type == "scd2":
            # SCD Type 2 load - handle historical data
            self._write_scd2(df, table_name, conn_properties)
        
        elif self.load_type == "cdc":
            # CDC load - handle change data capture
            self._write_cdc(df, table_name, conn_properties)
        
        else:
            raise ValueError(f"Unsupported load type: {self.load_type}")
    
    def validate_connection(self) -> bool:
        """
        Validate the connection to the RDBMS.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        try:
            # Try to create a JDBC connection
            conn_properties = {
                "driver": self.driver,
                "user": self.user,
                "password": self.password
            }
            
            # Create a small test dataframe
            test_df = DataFrame.sparkSession.createDataFrame([("test",)], ["col1"])
            
            # Try to write to a temporary table
            test_table = "connection_test_" + str(int(time.time()))
            test_df.write.jdbc(
                url=self.jdbc_url,
                table=test_table,
                mode="overwrite",
                properties=conn_properties
            )
            
            # Try to drop the temporary table
            test_df.sparkSession.read.jdbc(
                url=self.jdbc_url,
                table=f"(DROP TABLE {test_table}) AS drop_query",
                properties=conn_properties
            )
            
            return True
        except Exception as e:
            print(f"Connection validation failed: {str(e)}")
            return False
    
    def _write_scd2(self, df: DataFrame, table_name: str, conn_properties: Dict[str, str]) -> None:
        """
        Write data using SCD Type 2 approach.
        
        Args:
            df: Input dataframe to write
            table_name: Target table name
            conn_properties: JDBC connection properties
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
        
        # Read existing data from the target table
        try:
            existing_df = df.sparkSession.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=conn_properties
            )
            
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
            result_df.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=conn_properties
            )
        except Exception as e:
            print(f"Error reading existing data, assuming first load: {str(e)}")
            # If the table doesn't exist, just write the new data
            df_with_scd.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=conn_properties
            )
    
    def _write_cdc(self, df: DataFrame, table_name: str, conn_properties: Dict[str, str]) -> None:
        """
        Write data using CDC approach.
        
        Args:
            df: Input dataframe to write
            table_name: Target table name
            conn_properties: JDBC connection properties
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
        
        # Process each operation type
        df_with_ts.createOrReplaceTempView("cdc_data")
        
        # Insert operations
        insert_df = df.sparkSession.sql(f"SELECT * FROM cdc_data WHERE {operation_column} = 'INSERT'")
        if insert_df.count() > 0:
            insert_df.drop(operation_column).write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode="append",
                properties=conn_properties
            )
        
        # Update operations
        update_df = df.sparkSession.sql(f"SELECT * FROM cdc_data WHERE {operation_column} = 'UPDATE'")
        if update_df.count() > 0:
            # For each record to update, construct an UPDATE statement
            update_records = update_df.collect()
            
            for record in update_records:
                # Construct WHERE clause for key columns
                where_clause = " AND ".join([f"{col} = '{record[col]}'" for col in key_columns])
                
                # Construct SET clause for non-key columns
                set_clauses = []
                for column in update_df.columns:
                    if column not in key_columns and column != operation_column:
                        value = record[column]
                        if value is not None:
                            if isinstance(value, str):
                                set_clauses.append(f"{column} = '{value}'")
                            else:
                                set_clauses.append(f"{column} = {value}")
                
                set_clause = ", ".join(set_clauses)
                
                # Execute UPDATE statement
                update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                df.sparkSession.read.jdbc(
                    url=self.jdbc_url,
                    table=f"({update_sql}) AS update_query",
                    properties=conn_properties
                )
        
        # Delete operations
        delete_df = df.sparkSession.sql(f"SELECT * FROM cdc_data WHERE {operation_column} = 'DELETE'")
        if delete_df.count() > 0:
            # For each record to delete, construct a DELETE statement
            delete_records = delete_df.collect()
            
            for record in delete_records:
                # Construct WHERE clause for key columns
                where_clause = " AND ".join([f"{col} = '{record[col]}'" for col in key_columns])
                
                # Execute DELETE statement
                delete_sql = f"DELETE FROM {table_name} WHERE {where_clause}"
                df.sparkSession.read.jdbc(
                    url=self.jdbc_url,
                    table=f"({delete_sql}) AS delete_query",
                    properties=conn_properties
                )