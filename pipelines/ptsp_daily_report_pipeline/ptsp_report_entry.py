from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, first, lit, round, substring, when
from pyspark.sql.types import DecimalType
import os
from datetime import datetime
from prefect.blocks.system import Secret

# PostgreSQL credentials as individual variables
POSTGRES_HOST = "192.168.0.244"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE = "data_warehouse"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "ITEX2024"
POSTGRES_SCHEMA = "ptsp_reports"

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
                .option("dbtable", f"{POSTGRES_SCHEMA}.ims_bank_codes") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print("Successfully loaded IMS bank codes table")
            return bank_codes_df
        except Exception as e:
            print(f"Error reading IMS bank codes: {e}")
            raise
    
    def read_transaction_data(self, table_name: str):
        """Read transaction data from PostgreSQL"""
        try:
            transactions_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("dbtable", f"{POSTGRES_SCHEMA}.{table_name}") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print(f"Successfully loaded transaction data from {table_name}")
            return transactions_df
        except Exception as e:
            print(f"Error reading transaction data: {e}")
            raise
    
    def process_terminal_ids(self, df):
        """Process terminal IDs - round to 5 decimal places if they contain decimal points"""
        try:
            # First, ensure terminal_id is string type
            df = df.withColumn("terminal_id", col("terminal_id").cast("string"))
            
            # Round terminal_ids with decimal points to 5 decimal places
            df = df.withColumn(
                "terminal_id", 
                when(col("terminal_id").rlike(r"\.\d+"), 
                     round(col("terminal_id").cast("double"), 5).cast("string"))
                .otherwise(col("terminal_id"))
            )
            
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
            df = df.withColumn("msc", col("amount") * lit(0.00125).cast(DecimalType(18, 6)))
            
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
                    sum("amount").alias("amount"),
                    sum("msc").alias("msc"),
                    first("merchant_id").alias("merchant_id"),
                    first("merchant").alias("merchant_name"),
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
            # Extract first 5 characters of terminal_id
            aggregated_df = aggregated_df.withColumn("bank_code_prefix", substring(col("terminal_id"), 1, 5))
            
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
                    col("t.amount"),
                    col("t.msc"),
                    col("t.merchant_id"),
                    col("t.merchant_name"),
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
    
    def write_to_postgres(self, df, table_name: str):
        """Write results to PostgreSQL"""
        try:
            df.write \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("dbtable", f"{POSTGRES_SCHEMA}.{table_name}") \
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
            processing_date = datetime.now()
        
        try:
            # Generate table names
            source_table = self.generate_source_table_name(processing_date)
            target_table = self.generate_target_table_name(processing_date)
            
            print(f"Processing data for {processing_date.strftime('%Y-%m')}")
            print(f"Source table: {source_table}")
            print(f"Target table: {target_table}")
            print(f"Database: {POSTGRES_DATABASE}")
            print(f"Host: {POSTGRES_HOST}")
            print(f"User: {POSTGRES_USER}")
            
            # Read data
            bank_codes_df = self.read_ims_bank_codes()
            transactions_df = self.read_transaction_data(source_table)
            
            # Select required columns
            selected_columns = ["terminal_id", "merchant", "merchant_id", "amount", "date"]
            transactions_df = transactions_df.select(*selected_columns)
            
            # Show initial data sample
            print("Initial data sample:")
            transactions_df.show(5)
            
            # Process data
            transactions_df = self.process_terminal_ids(transactions_df)
            transactions_df = self.calculate_msc(transactions_df)
            
            # Show data after processing
            print("Data after MSC calculation:")
            transactions_df.show(5)
            
            aggregated_df = self.aggregate_transactions(transactions_df)
            final_df = self.add_bank_column(aggregated_df, bank_codes_df)
            
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
        print(f"Using schema: {POSTGRES_SCHEMA}")
        
        # Initialize processor
        processor = PostgresTransactionProcessor()
        
        # Process transactions for current month
        result_df = processor.process_transactions()
        
        # Show summary
        if result_df:
            print(f"Processing completed. Total records: {result_df.count()}")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        
    finally:
        # Clean up
        if processor:
            processor.stop_spark()

if __name__ == "__main__":
    main()