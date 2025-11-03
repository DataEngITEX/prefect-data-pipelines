import os
import json
import urllib.parse
import psycopg2
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum, count, first, to_date, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from prefect.blocks.system import Secret

# MongoDB Configuration
mongodb_secret=Secret.load("cbn-weekly-report-secret").get()["mongodb"]
mdw_username = mongodb_secret["username"]
mdw_password = mongodb_secret["password"]
mdw_host = mongodb_secret["host"]
mdw_port = mongodb_secret["port"]
mdw_database = mongodb_secret["database"] 

postgres_secret=Secret.load("cbn-weekly-report-secret").get()["postgres"]

postgres_host =postgres_secret["host"]
postgres_port = postgres_secret["port"]
postgres_database = postgres_secret["database"]
postgres_user = postgres_secret["user"]
postgres_password = postgres_secret["password"]

# Local paths

def get_previous_week_dates():
    """Get Monday of last week to Sunday of current week (7 days)"""
    today = datetime.today()
    
    # Find Monday of last week
    last_monday = today - timedelta(days=today.weekday() + 7)
    
    # Find Sunday of current week (yesterday if today is Monday)
    current_sunday = today - timedelta(days=(today.weekday() + 1) % 7)
    
    # Format as ISO strings for MongoDB
    start_date = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = current_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    report_date = current_sunday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"Report date: {report_date}")
    print(f"Total days: {(end_date - start_date).days + 1} days")  # Should print 7
    
    return start_date, end_date, report_date


def create_spark_session():
    """Create Spark session"""
    try:
        spark = SparkSession.builder \
            .appName("CBN Weekly Report from MongoDB") \
            .master("local[*]") \
            .config("spark.executor.memory", "3g") \
            .config("spark.driver.memory", "3g") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.hadoop.hadoop.security.authorization", "false") \
            .config("spark.hadoop.hadoop.security.authentication", "simple") \
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

def extract_vas_transactions_from_mongo(spark, start_date, end_date):
    """Extract VAS transactions from MongoDB for previous week"""
    try:
        print("Extracting VAS transactions from MongoDB...")
        
        # Define projection fields
        projection_fields = ["wallet", "updated_at", "category", "terminal", "amount"]
        lowercase_columns = [c.lower() for c in projection_fields]
        
        # Create explicit schema
        explicit_schema = StructType([
            StructField("wallet", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("category", StringType(), True),
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
                        "$gte": {"$date": start_date.isoformat(timespec='milliseconds') + 'Z'},
                        "$lte": {"$date": end_date.isoformat(timespec='milliseconds') + 'Z'}
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
        
        print(f"Extracted {df.count()} VAS transactions from MongoDB")
        df.show(5)
        
        return df
        
    except Exception as e:
        print(f"Error extracting VAS transactions from MongoDB: {e}")
        raise

def aggregate_vas_data(vas_df):
    """Aggregate VAS data by wallet, terminal, category and date part of updated_at"""
    try:
        print("Aggregating VAS data by wallet, terminal, category and date...")
        
        # Extract date part from updated_at (remove time component)
        vas_df = vas_df.withColumn("date_only", to_date(col("updated_at")))
        
        # Group by wallet, terminal, category and date, then calculate volume and value
        aggregated_df = vas_df.groupBy("wallet", "terminal", "category", "date_only") \
            .agg(
                count("*").alias("volume"),
                sum("amount").alias("value"),
                max("updated_at").alias("latest_updated_at"),  # Get the latest updated_at
                first("wallet").alias("wallet_first"),
                first("terminal").alias("terminal_first"),
                first("category").alias("category_first")
            )
        
        # Select and reorder columns to match expected format
        aggregated_df = aggregated_df.select(
            col("wallet_first").alias("wallet"),
            col("latest_updated_at").alias("updated_at"),
            col("category_first").alias("category"),
            col("terminal_first").alias("terminal"),
            lit("").alias("address"),
            lit("").alias("phone_number"),
            lit("").alias("bvn"),
            "volume",
            "value"
        )
        
        print(f"Aggregated data: {aggregated_df.count()} records")
        aggregated_df.show(5)
        
        return aggregated_df
        
    except Exception as e:
        print(f"Error aggregating VAS data: {e}")
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
            "bvn", 
            "nin",
            "phone_number",
            "address"
        )
        
        print(f"Loaded {agent_df.count()} agent records")
        agent_df.show(5)
        
        return agent_df
        
    except Exception as e:
        print(f"Error loading agent details: {e}")
        raise

def is_empty(col_expr):
    """Helper function to check for empty values"""
    return (col_expr.isNull()) | (col_expr == "") | (col_expr == "0")

def enrich_cbn_data(cbn_df, agent_df, report_date):
    """Enrich CBN data with agent details and add report date"""
    try:
        print("Enriching CBN data with agent details...")
        
        # Join CBN data with agent details on wallet and wallet_id
        joined_df = cbn_df.alias("cbn").join(
            agent_df.alias("agent"),
            col("cbn.wallet") == col("agent.wallet_id"),
            "left"
        )
        
        # Update BVN - use agent.bvn if not empty, else agent.nin if not empty
        updated_df = joined_df.withColumn(
            "_bvn",
            when(is_empty(col("cbn.bvn")),
                when(~is_empty(col("agent.bvn")), col("agent.bvn"))
                .when(~is_empty(col("agent.nin")), col("agent.nin"))
                .otherwise(col("cbn.bvn"))
            ).otherwise(col("cbn.bvn"))
        )
        
        # Update phone number - only if agent.phone_number is not empty
        updated_df = updated_df.withColumn(
            "_phone_number",
            when(is_empty(col("cbn.phone_number")) & ~is_empty(col("agent.phone_number")),
                col("agent.phone_number")
            ).otherwise(col("cbn.phone_number"))
        )
        
        # Update address - only if agent.address is not empty
        updated_df = updated_df.withColumn(
            "_address",
            when(is_empty(col("cbn.address")) & ~is_empty(col("agent.address")),
                col("agent.address")
            ).otherwise(col("cbn.address"))
        )
        
        # Rebuild final DataFrame with proper column names and add report date
        final_df = updated_df.select(
            "cbn.wallet",
            "cbn.updated_at",
            "cbn.category",
            "cbn.terminal",
            col("_bvn").alias("bvn"),
            col("_phone_number").alias("phone_number"),
            col("_address").alias("address"),
            "cbn.volume",
            "cbn.value"
        ).withColumn("report_date", lit(report_date.strftime("%Y-%m-%d")))
        
        print(f"Final enriched data: {final_df.count()} records")
        final_df.show(10)
        
        return final_df
        
    except Exception as e:
        print(f"Error enriching CBN data: {e}")
        raise

def save_to_postgres(final_df):
    """Save final DataFrame to PostgreSQL"""
    try:
        print("Saving data to PostgreSQL...")
        
        pg_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}"
        
        final_df.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", "agent_reports.cbn_reports") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print("Data successfully saved to agent_reports.cbn_reports")
        
        
        
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")
        raise



def process_cbn_report():
    """Main function to process CBN report from MongoDB"""
    spark = None
    try:
        # Get date range for previous week
        start_date, end_date, report_date = get_previous_week_dates()
        
        # Create Spark session
        spark = create_spark_session()
        
        # Step 1: Extract VAS transactions from MongoDB
        vas_df = extract_vas_transactions_from_mongo(spark, start_date, end_date)
        
        # Step 2: Aggregate data by wallet, terminal, category and date
        cbn_df = aggregate_vas_data(vas_df)
        
        # Step 3: Load agent details from PostgreSQL
        agent_df = load_agent_details(spark)
        
        # Step 4: Enrich CBN data with agent details
        final_df = enrich_cbn_data(cbn_df, agent_df, report_date)
        
        # Step 5: Save to PostgreSQL
        save_to_postgres(final_df)
        
        print("CBN weekly report processing completed successfully!")
        
    except Exception as e:
        print(f"Error in CBN report processing: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

def main():
    """Main execution function"""
    try:
        process_cbn_report()
    except Exception as e:
        print(f"An error occurred in the CBN report pipeline: {e}")

if __name__ == '__main__':
    main()