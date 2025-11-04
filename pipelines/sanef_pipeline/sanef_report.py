import os
import json
import urllib.parse
import psycopg2
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, count, countDistinct, upper, when, trim
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# MongoDB Configuration
mdw_username = "vasuser"
mdw_password = "p@$$w0rd@1"
mdw_host = "192.168.0.35"
mdw_port = "11001"
mdw_database = "vas"

# PostgreSQL Configuration
postgres_host = "192.168.0.244"
postgres_port = "5432"
postgres_database = "data_warehouse"
postgres_user = "admin"
postgres_password = "ITEX2024"

def get_previous_month_range():
    """Get the first and last day of previous month"""
    today = datetime.today()
    # First day of current month
    first_day_current = today.replace(day=1)
    # Last day of previous month
    last_day_previous = first_day_current - timedelta(days=1)
    # First day of previous month
    first_day_previous = last_day_previous.replace(day=1)
    
    # Report date is the last day of previous month
    report_date = last_day_previous.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Format for MongoDB query (start of first day to end of last day)
    start_date = first_day_previous.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = last_day_previous.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    print(f"Previous month range: {start_date} to {end_date}")
    print(f"Report date: {report_date}")
    
    return start_date, end_date, report_date

def create_spark_session():
    """Create Spark session"""
    try:
        spark = SparkSession.builder \
            .appName("SANEF Monthly Report") \
            .master("local[*]") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.jars", 
                   r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\mongo-spark-connector_2.12-10.5.0.jar,"
                   r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\postgresql-42.6.0.jar") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        print("Failed to start Spark session:", e)
        raise

def get_postgres_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        dbname=postgres_database,
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port
    )

def extract_vas_transactions_previous_month(spark, start_date, end_date):
    """Extract VAS transactions from MongoDB for previous month"""
    try:
        print("Extracting VAS transactions from MongoDB for previous month...")
        
        # Define projection fields
        projection_fields = ["wallet", "terminal", "amount"]
        lowercase_columns = [c.lower() for c in projection_fields]
        
        # Create explicit schema
        explicit_schema = StructType([
            StructField("wallet", StringType(), True),
            StructField("terminal", StringType(), True),
            StructField("amount", StringType(), True)  # Read as string, convert later
        ])

        mongo_options = {
            "spark.mongodb.read.connection.uri": f"mongodb://{mdw_username}:{urllib.parse.quote_plus(mdw_password)}@{mdw_host}:{mdw_port}/{mdw_database}",
            "spark.mongodb.read.database": mdw_database,
            "spark.mongodb.read.collection": "vas_transaction",
            "inferSchema": "false"
        }

        # Create aggregation pipeline for date range and projection
        pipeline = [
            {
                "$match": {
                    "updated_at": {
                        "$gte": {"$date": start_date.isoformat() + 'Z'},
                        "$lte": {"$date": end_date.isoformat() + 'Z'}
                    }
                }
            },
            {
                "$project": {field: 1 for field in projection_fields}
            }
        ]

        mongo_options["spark.mongodb.read.aggregation.pipeline"] = json.dumps(pipeline)

        # Read data from MongoDB
        df = spark.read.format("mongodb") \
            .schema(explicit_schema) \
            .options(**mongo_options) \
            .load()
        
        df = df.toDF(*lowercase_columns)
        
        # Convert amount to numeric and handle potential errors
        df = df.withColumn("amount", col("amount").cast("double"))
        
        print(f"Extracted {df.count()} VAS transactions from MongoDB for previous month")
        print("VAS Transactions DataFrame:")
        df.show(10, truncate=False)
        
        return df
        
    except Exception as e:
        print(f"Error extracting VAS transactions from MongoDB: {e}")
        raise

def load_agent_details(spark):
    """Load agent details from PostgreSQL"""
    try:
        print("Loading agent details from PostgreSQL...")
        
        pg_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}"
        pg_properties = {
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver"
        }
        
        agent_df = spark.read.jdbc(
            url=pg_url,
            table="agent_reports.agent_details",
            properties=pg_properties
        )
        
        # Select only required columns
        agent_df = agent_df.select(
            "wallet_id",
            "is_active", 
            "state_of_residence"
        )
        
        # Convert "Federal Capital Territory" to "Abuja" and handle case sensitivity
        agent_df = agent_df.withColumn(
            "state_of_residence_clean",
            when(
                upper(trim(col("state_of_residence"))) == "FEDERAL CAPITAL TERRITORY", 
                "Abuja"
            ).otherwise(trim(col("state_of_residence")))
        )
        
        # Create uppercase version for case-insensitive join
        agent_df = agent_df.withColumn(
            "state_upper",
            upper(trim(col("state_of_residence_clean")))
        )
        
        print(f"Loaded {agent_df.count()} agent records")
        print("Agent Details DataFrame:")
        agent_df.show(10, truncate=False)
        
        return agent_df
        
    except Exception as e:
        print(f"Error loading agent details: {e}")
        raise

def load_states_regions(spark):
    """Load states and regions mapping from PostgreSQL"""
    try:
        print("Loading states and regions mapping from PostgreSQL...")
        
        pg_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}"
        pg_properties = {
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver"
        }
        
        states_df = spark.read.jdbc(
            url=pg_url,
            table="agent_reports.states_regions",
            properties=pg_properties
        )
        
        # Create uppercase version for case-insensitive join
        states_df = states_df.withColumn(
            "state_upper",
            upper(trim(col("state")))
        )
        
        print(f"Loaded {states_df.count()} state-region mappings")
        print("States Regions DataFrame:")
        states_df.show(10, truncate=False)
        
        return states_df
        
    except Exception as e:
        print(f"Error loading states and regions: {e}")
        raise

def process_sanef_report(vas_df, agent_df, states_df, report_date):
    """Process SANEF report by joining data and aggregating by state"""
    try:
        print("Processing SANEF report data...")
        
        # Step 1: Join VAS transactions with agent details on wallet
        print("Joining VAS transactions with agent details...")
        joined_df = vas_df.alias("vas").join(
            agent_df.alias("agent"),
            col("vas.wallet") == col("agent.wallet_id"),
            "left"
        )
        
        print("After VAS + Agent Details Join:")
        joined_df.show(10, truncate=False)
        print(f"Records after first join: {joined_df.count()}")
        
        # Step 2: Join with states_regions to get region (CASE-INSENSITIVE)
        print("Joining with states_regions to get regions (case-insensitive)...")
        final_joined_df = joined_df.alias("joined").join(
            states_df.alias("states"),
            col("joined.state_upper") == col("states.state_upper"),  # Case-insensitive join
            "left"
        )
        
        print("After States Regions Join:")
        final_joined_df.show(10, truncate=False)
        print(f"Records after second join: {final_joined_df.count()}")
        
        # Step 3: Group by state and calculate aggregates
        print("Aggregating data by state...")
        
        aggregated_df = final_joined_df.groupBy(
            col("states.state").alias("state"),
            col("states.region").alias("region")
        ).agg(
            count("*").alias("volume_of_transactions"),
            sum("joined.amount").alias("total_transaction_value"),
            countDistinct("joined.wallet").alias("unique_wallets")
        )
        
        print("After Aggregation:")
        aggregated_df.show(20, truncate=False)
        
        # Step 4: Add report date column
        final_df = aggregated_df.withColumn(
            "report_date", 
            lit(report_date.strftime("%Y-%m-%d"))
        )
        
        # Step 5: Select and reorder final columns
        final_df = final_df.select(
            "state",
            "region", 
            "volume_of_transactions",
            "total_transaction_value",
            "unique_wallets",
            "report_date"
        )
        
        print("Final SANEF report data:")
        final_df.show(20, truncate=False)
        
        return final_df
        
    except Exception as e:
        print(f"Error processing SANEF report: {e}")
        raise

def save_sanef_report(final_df):
    """Save final SANEF report to PostgreSQL"""
    try:
        print("Saving SANEF report to PostgreSQL...")
        
        pg_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}"
        
        final_df.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", "agent_reports.sanef_report") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print("SANEF report successfully saved to agent_reports.sanef_report")
        
        # Print summary
        record_count = final_df.count()
        print(f"Saved {record_count} state records to SANEF report")
        
    except Exception as e:
        print(f"Error saving SANEF report to PostgreSQL: {e}")
        raise

def process_sanef_report_pipeline():
    """Main function to process SANEF report"""
    spark = None
    try:
        # Get date range for previous month
        start_date, end_date, report_date = get_previous_month_range()
        
        # Create Spark session
        spark = create_spark_session()
        
        # Step 1: Extract VAS transactions from MongoDB for previous month
        vas_df = extract_vas_transactions_previous_month(spark, start_date, end_date)
        
        # Step 2: Load agent details from PostgreSQL
        agent_df = load_agent_details(spark)
        
        # Step 3: Load states and regions mapping
        states_df = load_states_regions(spark)
        
        # Step 4: Process and aggregate the data
        final_df = process_sanef_report(vas_df, agent_df, states_df, report_date)
        
        # Step 5: Save to PostgreSQL
        save_sanef_report(final_df)
        
        print("SANEF monthly report processing completed successfully!")
        
    except Exception as e:
        print(f"Error in SANEF report processing: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

def main():
    """Main execution function"""
    try:
        process_sanef_report_pipeline()
    except Exception as e:
        print(f"An error occurred in the SANEF report pipeline: {e}")

if __name__ == '__main__':
    main()