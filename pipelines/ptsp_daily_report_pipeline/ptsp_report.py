from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, first, lit, round, substring, when, rpad, regexp_replace, split
from pyspark.sql.types import DecimalType, StringType
import os
from datetime import datetime, timedelta
from prefect.blocks.system import Secret
import psycopg2

# PostgreSQL credentials as individual variables
POSTGRES_HOST = "192.168.0.244"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE = "data_warehouse"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "ITEX2024"
PTSP_REPORTS_SCHEMA = "ptsp_reports"
PTSP_SCHEMA = "ptsp_schema"

class PostgresTransactionProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.postgres_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
    
    def _create_spark_session(self):
        """Create and configure Spark session"""
        try:
            spark = SparkSession.builder \
                .appName("PostgresTransactionProcessor") \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.memory", "4g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("INFO")
            return spark
        except Exception as e:
            print(f"Failed to create Spark session: {e}")
            raise
    
    def get_postgres_connection(self):
        """Create PostgreSQL connection"""
        return psycopg2.connect(
            dbname=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
    
    def generate_source_table_name(self, date: datetime):
        """Generate dynamic source table name in format daily_transactions_YYYY_MM"""
        return f"daily_transactions_{date.year}_{date.month:02d}"
    
    def generate_target_table_name(self, date: datetime):
        """Generate dynamic target table name in format summarized_transactions_YYYY_MM"""
        return f"summarized_transactions_{date.year}_{date.month:02d}"
    
    def read_ims_bank_codes(self):
        """Read IMS bank codes table from PostgreSQL"""
        try:
            bank_codes_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("dbtable", f"{PTSP_REPORTS_SCHEMA}.ims_bank_codes") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print("Successfully loaded IMS bank codes table")
            return bank_codes_df
        except Exception as e:
            print(f"Error reading IMS bank codes: {e}")
            raise
    
    def clean_terminal_id(self, terminal_id_col):
        """Clean terminal ID - remove everything after E, keep decimal, pad to 8 characters"""
        return when(terminal_id_col.contains("."), 
                   rpad(
                       split(terminal_id_col, "E").getItem(0),  # Take everything before E
                       8, "0"  # Pad to 8 characters with zeros
                   )
               ).otherwise(terminal_id_col)  # Leave others unchanged
    
    def read_merchant_terminal_rca(self):
        """Read only terminal_id and state from merchant_terminal_rca table, clean terminal IDs, and remove duplicates"""
        try:
            # Read only the required columns from merchant_terminal_rca table
            query = f"""
                SELECT terminal_id, state 
                FROM {PTSP_SCHEMA}.merchant_terminal_rca
            """
            
            rca_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("query", query) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            # Show original terminal IDs from RCA table
            print("Original terminal IDs from merchant_terminal_rca:")
            rca_df.select("terminal_id").distinct().show(10)
            print(f"Original count: {rca_df.count()}")
            
            # Clean the terminal IDs using the same logic as transaction data
            rca_df = rca_df.withColumn("terminal_id", col("terminal_id").cast(StringType()))
            rca_df = rca_df.withColumn("terminal_id", self.clean_terminal_id(col("terminal_id")))
            
            # Remove duplicates based on terminal_id - keep first occurrence for each terminal_id
            rca_df = rca_df.dropDuplicates(["terminal_id"])
            
            # Rename state to state_initials
            rca_df = rca_df.withColumnRenamed("state", "state_initials")
            
            print("Cleaned and deduplicated terminal IDs from merchant_terminal_rca:")
            rca_df.select("terminal_id").distinct().show(10)
            print(f"Final count after deduplication: {rca_df.count()}")
            
            print("Successfully loaded, cleaned, and deduplicated merchant_terminal_rca data")
            return rca_df
        except Exception as e:
            print(f"Error reading merchant_terminal_rca: {e}")
            raise
    
    def read_terminals_table(self):
        """Read terminals table from ptsp_reports schema for state information"""
        try:
            terminals_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("dbtable", f"{PTSP_REPORTS_SCHEMA}.terminals") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print("Successfully loaded terminals table")
            terminals_df.show(5)
            return terminals_df
        except Exception as e:
            print(f"Error reading terminals table: {e}")
            raise
    
    def read_states_regions(self):
        """Read states_regions table from data warehouse"""
        try:
            states_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("dbtable", f"{PTSP_REPORTS_SCHEMA}.states_regions") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print("Successfully loaded states_regions table")
            states_df.show(5)
            return states_df
        except Exception as e:
            print(f"Error reading states_regions: {e}")
            raise
    
    def read_transaction_data(self, table_name: str, processing_date: datetime):
        """Read transaction data from PostgreSQL for the specific day only"""
        try:
            # Format the date for SQL query
            date_str = processing_date.strftime("%Y-%m-%d")
            
            # Use query to filter only today's data and select only required columns
            query = f"""
                SELECT terminal_id, merchant, merchant_id, amount, date 
                FROM {PTSP_REPORTS_SCHEMA}.{table_name} 
                WHERE date::date = '{date_str}'
            """
            
            transactions_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("query", query) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print(f"Successfully loaded transaction data from {table_name} for date: {date_str}")
            print(f"Number of records for {date_str}: {transactions_df.count()}")
            return transactions_df
        except Exception as e:
            print(f"Error reading transaction data: {e}")
            raise
    
    def process_terminal_ids(self, df):
        """Process terminal IDs using the clean_terminal_id logic"""
        try:
            # First, ensure terminal_id is string type
            df = df.withColumn("terminal_id", col("terminal_id").cast(StringType()))
            
            # Show original terminal IDs
            print("Original terminal IDs from transactions:")
            df.select("terminal_id").distinct().show(10)
            
            # Clean the terminal IDs
            df = df.withColumn("terminal_id", self.clean_terminal_id(col("terminal_id")))
            
            print("Cleaned terminal IDs from transactions:")
            df.select("terminal_id").distinct().show(10)
            
            return df
        except Exception as e:
            print(f"Error processing terminal IDs: {e}")
            return df
    
    def calculate_msc(self, df):
        """Calculate MSC column as amount * 0.00125"""
        try:
            # Ensure amount is numeric type
            df = df.withColumn("amount", col("amount").cast(DecimalType(18, 2)))
            
            # Calculate MSC
            df = df.withColumn("msc", col("amount") * lit(0.00125).cast(DecimalType(18, 4)))
            
            return df
        except Exception as e:
            print(f"Error calculating MSC: {e}")
            raise
    
    def aggregate_transactions(self, df):
        """Aggregate transactions by terminal_id"""
        try:
            aggregated_df = df.groupBy("terminal_id") \
                .agg(
                    count("terminal_id").alias("volume"),
                    sum("amount").alias("value"),
                    sum("msc").alias("msc"),
                    first("merchant_id").alias("merchant_id"),
                    first("merchant").alias("merchant"),
                    first("date").alias("date")
                )
            
            print("Successfully aggregated transactions by terminal_id")
            return aggregated_df
        except Exception as e:
            print(f"Error aggregating transactions: {e}")
            raise
    
    def add_bank_column(self, aggregated_df, bank_codes_df):
        """Add bank column based on first 5 characters of terminal_id matching bank_code"""
        try:
            # Extract first 4 characters of terminal_id
            aggregated_df = aggregated_df.withColumn(
                "bank_code_prefix", 
                when(col("terminal_id").contains("."), substring(col("terminal_id"), 1, 5))
                .otherwise(substring(col("terminal_id"), 1, 4))
            )            
            
            # Join with bank codes table
            result_df = aggregated_df.alias("t") \
                .join(
                    bank_codes_df.alias("b"),
                    col("t.bank_code_prefix") == col("b.bank_code"),
                    "left"
                ) \
                .select(
                    col("t.terminal_id"),
                    col("t.volume"),
                    col("t.value"),
                    col("t.msc"),
                    col("t.merchant_id"),
                    col("t.merchant"),
                    col("t.date"),
                    col("b.bank").alias("bank")
                )
            
            # Drop the temporary bank_code_prefix column
            result_df = result_df.drop("bank_code_prefix")
            
            print("Successfully added bank column")
            return result_df
        except Exception as e:
            print(f"Error adding bank column: {e}")
            raise
    
    def update_nova_bank(self, df_with_bank):
        """Update bank to NOVA BANK for all merchants that start with NOVA"""
        try:
            # Count how many merchants start with NOVA before update
            nova_count_before = df_with_bank.filter(col("merchant").startswith("NOVA")).count()
            print(f"Number of merchants starting with NOVA: {nova_count_before}")
            
            # Update bank to NOVA BANK for merchants starting with NOVA
            df_updated = df_with_bank.withColumn(
                "bank",
                when(col("merchant").startswith("NOVA"), lit("NOVA BANK"))
                .otherwise(col("bank"))
            )
            
            # Count how many were updated
            nova_count_after = df_updated.filter(col("bank") == "NOVA BANK").count()
            print(f"Number of records updated to NOVA BANK: {nova_count_after}")
            
            # Show sample of updated records
            print("Sample of NOVA BANK records:")
            df_updated.filter(col("bank") == "NOVA BANK").show(5)
            
            return df_updated
        except Exception as e:
            print(f"Error updating NOVA BANK: {e}")
            raise
    
    def upsert_terminals_to_postgres(self, rca_df):
        """UPSERT terminals data to PostgreSQL using MERGE statements"""
        try:
            # First write to staging table
            staging_table = f"{PTSP_REPORTS_SCHEMA}.terminals_staging"
            
            rca_df.write \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("dbtable", staging_table) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            
            print("Successfully wrote terminals data to staging table")
            
            # Prepare MERGE SQL
            cols = [f'"{c}"' for c in rca_df.columns]
            col_list = ", ".join(cols)
            values_list = ", ".join([f"source.{c}" for c in cols])
            
            merge_sql = f"""
                MERGE INTO {PTSP_REPORTS_SCHEMA}.terminals AS target
                USING {staging_table} AS source
                ON target."terminal_id" = source."terminal_id"
                
                WHEN MATCHED THEN
                    UPDATE SET 
                        "state_initials" = source."state_initials"
                
                WHEN NOT MATCHED THEN
                    INSERT ({col_list})
                    VALUES ({values_list})
            """
            
            conn = self.get_postgres_connection()
            
            try:
                with conn.cursor() as cursor:
                    # Execute the MERGE statement
                    print("Executing MERGE statement on terminals table...")
                    cursor.execute(merge_sql)
                    
                    # Commit the transaction
                    conn.commit()
                    print("Transaction committed successfully.")
                
            except Exception as e:
                conn.rollback()
                print(f"Transaction failed. Rolling back changes. Error: {e}")
                raise
            finally:
                if conn:
                    conn.close()
                    print("Database connection closed.")
                    
        except Exception as e:
            print(f"Error writing terminals to PostgreSQL: {e}")
            raise
    
    def add_state_region_columns(self, df_with_bank, terminals_df, states_regions_df):
        """Add state and region columns by joining with terminals table and states_regions"""
        try:
            # First join with terminals table to get state_initials
            df_with_state = df_with_bank.alias("main") \
                .join(
                    terminals_df.alias("terms"),
                    col("main.terminal_id") == col("terms.terminal_id"),
                    "left"
                ) \
                .select(
                    col("main.*"),
                    col("terms.state_initials")
                )
            
            # Show join results for debugging
            matched_count = df_with_state.filter(col("state_initials").isNotNull()).count()
            unmatched_count = df_with_state.filter(col("state_initials").isNull()).count()
            print(f"Terminal-state matching results: {matched_count} matched, {unmatched_count} unmatched")
            
            # Then join with states_regions to get state and region names
            df_with_region = df_with_state.alias("with_state") \
                .join(
                    states_regions_df.alias("states"),
                    col("with_state.state_initials") == col("states.state_initials"),
                    "left"
                ) \
                .select(
                    col("with_state.terminal_id"),
                    col("with_state.volume"),
                    col("with_state.value"),
                    col("with_state.msc"),
                    col("with_state.merchant_id"),
                    col("with_state.merchant"),
                    col("with_state.date"),
                    col("with_state.bank"),
                    col("states.state").alias("state"),
                    col("states.region").alias("region")
                )
            
            print("Successfully added state and region columns")
            df_with_region.show(5)
            return df_with_region
        except Exception as e:
            print(f"Error adding state/region columns: {e}")
            raise
    
    def write_to_postgres(self, df, table_name: str):
        """Write results to PostgreSQL"""
        try:
            df.write \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("dbtable", f"{PTSP_REPORTS_SCHEMA}.{table_name}") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            print(f"Successfully wrote data to {table_name}")
        except Exception as e:
            print(f"Error writing to PostgreSQL: {e}")
            raise
    
    def process_transactions(self, processing_date: datetime = None):
        """Main method to process transactions"""
        if processing_date is None:
            processing_date = datetime.now() - timedelta(days=3)
        
        try:
            # Generate table names
            source_table = self.generate_source_table_name(processing_date)
            target_table = self.generate_target_table_name(processing_date)
            
            print(f"Processing data for {processing_date.strftime('%Y-%m-%d')}")
            print(f"Source table: {source_table}")
            print(f"Target table: {target_table}")
            print(f"Using schemas: {PTSP_REPORTS_SCHEMA} for reports, {PTSP_SCHEMA} for RCA data")
            
            # Read all required data
            transactions_df = self.read_transaction_data(source_table, processing_date)
                        
            # Check if there's data for today
            if transactions_df.count() == 0:
                print(f"No transaction data found for {processing_date.strftime('%Y-%m-%d')}. Skipping processing.")
                return None
            
            bank_codes_df = self.read_ims_bank_codes()
            rca_df = self.read_merchant_terminal_rca()
            states_regions_df = self.read_states_regions()

            # FIRST: Update the terminals table with latest RCA data
            print("Updating terminals table in ptsp_reports schema with latest RCA merchants table data in ptsp_schema ...")
            self.upsert_terminals_to_postgres(rca_df)
            
            # THEN: Read the updated terminals table
            terminals_df = self.read_terminals_table()
            
            # Process data
            transactions_df = self.process_terminal_ids(transactions_df)
            transactions_df = self.calculate_msc(transactions_df)
            
            # Show data after processing
            print("Data after MSC calculation:")
            transactions_df.show(5)
            
            # Aggregate transactions
            aggregated_df = self.aggregate_transactions(transactions_df)
            
            # Add bank information
            df_with_bank = self.add_bank_column(aggregated_df, bank_codes_df)
            df_with_bank = self.update_nova_bank(df_with_bank)
            
            # Add state and region information (using UPDATED terminals table)
            final_df = self.add_state_region_columns(df_with_bank, terminals_df, states_regions_df)
            
            # Show final processed data
            print("Final processed data sample:")
            final_df.show(10)
            
            # Write to PostgreSQL
            self.write_to_postgres(final_df, target_table)
            
            print("Transaction processing completed successfully!")
            
            return final_df
            
        except Exception as e:
            print(f"Error in transaction processing: {e}")
            raise
    
    def stop_spark(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            print("Spark session stopped")

def main():
    """Main execution function"""
    processor = None
    try:
        # Display connection info
        print("Starting Postgres Transaction Processor")
        print(f"Connecting to: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")
        print(f"Using schemas: {PTSP_REPORTS_SCHEMA} for reports, {PTSP_SCHEMA} for RCA data")
        
        # Initialize processor
        processor = PostgresTransactionProcessor()
        
        # Process transactions for current day
        result_df = processor.process_transactions()
        
        # Show summary
        if result_df:
            print(f"Processing completed. Total records: {result_df.count()}")
        else:
            print("No data to process for today.")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        
    finally:
        # Clean up
        if processor:
            processor.stop_spark()

if __name__ == "__main__":
    main()