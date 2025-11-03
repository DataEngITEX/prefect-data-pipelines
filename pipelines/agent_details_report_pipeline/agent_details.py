from pyspark.sql import SparkSession
import psycopg2
from prefect.blocks.system import Secret
# Payvice database connection details
payvice_config = Secret.load("agent-details-secret").get()["payvice_db"]

# Data Warehouse connection details
dw_config = Secret.load("agent-details-secret").get()["data_warehouse_db"]

def create_spark_session():
    # Create Spark session with optimized settings for local execution
    try:
        spark = SparkSession.builder \
            .appName("Payvice to Agent Details ETL") \
            .master("local[*]") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.hadoop.hadoop.security.authorization", "false") \
            .config("spark.hadoop.hadoop.security.authentication", "simple") \
            .config("spark.jars", r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\postgresql-42.6.0.jar") \
            .getOrCreate()

        # Reduce log level to avoid excessive output
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        print("Failed to start Spark session:", e)
        raise

def refresh_payvice_materialized_view():
    # Refresh the source materialized view to get latest data
    try:
        # Connect to Payvice database
        conn = psycopg2.connect(
            host=payvice_config['host'],
            port=payvice_config['port'],
            database=payvice_config['database'],
            user=payvice_config['user'],
            password=payvice_config['password']
        )
        cursor = conn.cursor()
        
        print("Refreshing materialized view: public.users_wallet_detailed")
        # Execute refresh command
        cursor.execute("REFRESH MATERIALIZED VIEW public.users_wallet_detailed")
        conn.commit()
        
        print("Materialized view refreshed successfully")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print("Error refreshing materialized view:", e)
        raise

def read_from_payvice_materialized_view(spark):
    # Read data from Payvice materialized view using Spark JDBC
    try:
        print("Reading from materialized view: public.users_wallet_detailed")
        
        # Build JDBC URL with credentials
        payvice_url = f"jdbc:postgresql://{payvice_config['host']}:{payvice_config['port']}/{payvice_config['database']}"
        
        # Read data using Spark JDBC with individual options (not properties)
        df = spark.read \
            .format("jdbc") \
            .option("url", payvice_url) \
            .option("dbtable", "public.users_wallet_detailed") \
            .option("user", payvice_config["user"]) \
            .option("password", payvice_config["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        # Count records for verification
        record_count = df.count()
        print("Successfully read", record_count, "records from materialized view")
        
        # Display schema and sample data for debugging
        print("DataFrame Schema:")
        df.printSchema()
        print("Sample data first 5 rows:")
        df.show(5)
        
        return df
        
    except Exception as e:
        print("Error reading from materialized view:", e)
        raise

def write_to_agent_details_table(spark, df):
    # Write DataFrame to target table in data warehouse
    try:
        print("Writing to agent_reports.agent_details table...")
        
        # Build JDBC URL for data warehouse
        dw_url = f"jdbc:postgresql://{dw_config['host']}:{dw_config['port']}/{dw_config['database']}"
        
        # Write data with overwrite mode to replace existing data
        df.write \
            .format("jdbc") \
            .option("url", dw_url) \
            .option("dbtable", "agent_reports.agent_details") \
            .option("user", dw_config["user"]) \
            .option("password", dw_config["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print("Data successfully written to agent_reports.agent_details")
        
    except Exception as e:
        print("Error writing to agent_details table:", e)
        raise

def refresh_agent_details_table():
    # Refresh the target table to ensure data consistency
    try:
        # Connect to data warehouse with autocommit enabled for VACUUM
        conn = psycopg2.connect(
            host=dw_config['host'],
            port=dw_config['port'],
            database=dw_config['database'],
            user=dw_config['user'],
            password=dw_config['password']
        )
        
        # Set autocommit to True for VACUUM command
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("Refreshing agent_reports.agent_details table...")
        
        # Vacuum and analyze to update table statistics and reclaim space
        # VACUUM requires autocommit=True as it cannot run in transaction block
        cursor.execute("VACUUM ANALYZE agent_reports.agent_details")
        
        print("Table vacuumed and analyzed successfully")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print("Error refreshing agent_details table:", e)
        raise

def main():
    # Main ETL process function
    spark = None
    try:
        print("Starting Payvice to Agent Details ETL process...")
        
        # Step 1: Initialize Spark session
        spark = create_spark_session()
        
        # Step 2: Refresh source materialized view
        refresh_payvice_materialized_view()
        
        # Step 3: Read data from source
        df = read_from_payvice_materialized_view(spark)
        
        # Step 4: Write data to target table
        write_to_agent_details_table(spark, df)
        
        # Step 5: Refresh target table
        refresh_agent_details_table()
        
        print("ETL process completed successfully!")
        
    except Exception as e:
        print("ETL process failed:", e)
        raise
    finally:
        # Always stop Spark session to free resources
        if spark:
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()