from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, rpad, split
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
import psycopg2
import json
from datetime import datetime, timedelta
from rca_utils import generate_journal_name, to_snake_case, load_to_postgres_database
import urllib
from prefect.blocks.secrets import Secret
# Configuration
postgres_secret=Secret.load("rca-postgres-secret").get()["postgres"]
postgres_database = postgres_secret["database"]
postgres_user = postgres_secret["user"]
postgres_host = postgres_secret["host"]
postgres_port = postgres_secret["port"]
postgres_password = postgres_secret["password"]

mongodb_secret=Secret.load("rca-postgres-secret").get()["mongodb"]
mdw_username = mongodb_secret["user"]
mdw_password = mongodb_secret["password"]
mdw_host = mongodb_secret["host"]
mdw_port = mongodb_secret["port"]
mdw_database = mongodb_secret["database"]
local_download_path = r"C:\Users\data.engineer\Documents\prefect-reports\rca-pipeline-report"
sharepoint_folder_path = '/sites/NIBSS-ITEXrepo/Shared Documents/Web_APP_RCA'

# SharePoint credentials


# Initialize Spark session with aggressive memory optimization
try:
    spark = SparkSession.builder \
        .appName("RCA File Report Processing") \
        .master("local[2]") \
        .config("spark.executor.memory", "5g") \
        .config("spark.driver.memory", "5g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skew.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .config("spark.hadoop.hadoop.security.authorization", "false") \
        .config("spark.hadoop.hadoop.security.authentication", "simple") \
        .config("spark.jars", r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\spark-excel_2.12-3.5.1_0.20.4.jar") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")  # Set to ERROR to reduce logs
except Exception as e:
    print("Failed to start Spark session:", e)
    raise

def clean_terminal_id(df, column_name="terminal_id"):
    """Clean terminal ID - remove everything after E, keep decimal, pad to 8 characters"""
    return df.withColumn(
        column_name, 
        when(col(column_name).contains("."), 
             rpad(
                 split(col(column_name), "E").getItem(0),  # Take everything before E
                 8, "0"  # Pad to 8 characters with zeros on the RIGHT
             )
        ).otherwise(col(column_name))
    )

def process_terminal_id_column(df, column_name="terminal_id"):
    """Process terminal ID column with cleaning only"""
    # First ensure it's string type
    df = df.withColumn(column_name, col(column_name).cast(StringType()))
    df = df.withColumn(
        "terminal_id",
        F.coalesce(
            F.regexp_replace(F.trim(F.col("terminal_id")), " ", "")  # remove spaces
             .cast("bigint"),
            F.col("terminal_id")
        )
    )
    # Clean the terminal ID (scientific notation handling only)
    df = clean_terminal_id(df, column_name)
    
    return df




def load_and_cast(path, sheet_name):
    """Load Excel sheet and apply schema cleaning"""
    try:
        df = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("maxRowsInMemory", 2000)  # Reduced from 1000 to 500
            .option("inferSchema", "false")
            .option("dataAddress", f"'{sheet_name}'!A1")
            .load(path)
        )
        
        if df.rdd.isEmpty():
            print(f"Warning: No data found in sheet {sheet_name}")
            return None
            
        # Convert to snake_case
        df = df.toDF(*[to_snake_case(c) for c in df.columns])
        
        # Process terminal_id column (scientific notation handling only)
        if "terminal_id" in df.columns:
            df = process_terminal_id_column(df)
        
        print(f"Loaded {df.count()} records from {sheet_name}")
        return df.coalesce(1)  # Force single partition immediately
    except Exception as e:
        print(f"Error loading sheet {sheet_name}: {e}")
        return None

def extract_terminal_activity_from_mongo():
    """Extract terminalId + updatedAt from MongoDB journals (last 120 days)."""
    try:
        journal_name = generate_journal_name(datetime.today())

        # Compute 120-day window
        start_date = (datetime.today() - timedelta(days=120)).isoformat(timespec='milliseconds') + 'Z'
        stop_date = datetime.today().isoformat(timespec='milliseconds') + 'Z'
        print(f"Extracting terminal activity from {journal_name}, range {start_date} to {stop_date}")

        # Projection schema
        projection_fields = ["terminalId", "updatedAt"]
        new_columns = ["terminal_id", "last_transaction_time"]
        explicit_schema = StructType([
            StructField("terminalId", StringType(), True),
            StructField("updatedAt", StringType(), True)
        ])
        
        mongo_options = {
            "spark.mongodb.read.connection.uri": f"mongodb://{mdw_username}:{urllib.parse.quote_plus(mdw_password)}@{mdw_host}:{mdw_port}/{mdw_database}?authSource=admin",
            "spark.mongodb.read.database": mdw_database,
            "spark.mongodb.read.collection": journal_name,
            "inferSchema": "false"
        }

        # Aggregation pipeline
        match_stage = {
            "transactionTime": {"$gte": {"$date": start_date}, "$lte": {"$date": stop_date}}
        }
        project_stage = {"$project": {f: 1 for f in projection_fields}}
        pipeline = [{"$match": match_stage}, project_stage]

        mongo_options["spark.mongodb.read.aggregation.pipeline"] = json.dumps(pipeline)

        # Load data
        df = spark.read.format("mongodb") \
            .schema(explicit_schema) \
            .options(**mongo_options) \
            .load()

        if df.rdd.isEmpty():
            print("No journal records found for last 120 days.")
            return None

        df = df.toDF(*new_columns)
        
        # Process terminal_id column (scientific notation handling only)
        df = process_terminal_id_column(df, "terminal_id")
        
        df = df.dropDuplicates(["terminal_id"])
        df = df.orderBy(col("last_transaction_time").desc())

        print(f"Fetched {df.count()} unique terminal records from MongoDB journals.")
        return df.coalesce(1)  # Single partition
    except Exception as e:
        print(f"Error extracting data from MongoDB: {e}")
        return None

def merge_dataframes(registered_df, connected_df, active_df, inactive_df, mongo_df):
    """Merge all dataframes together with optimized joins - FIXED VERSION"""
    try:
        # Start with registered terminals and preserve ALL columns
        base_df = registered_df \
            .withColumn("connected", lit(0)) \
            .withColumn("active", lit(0)) \
            .withColumn("inactive", lit(0))
        
        # Cache the base dataframe to avoid recomputation
        base_df.cache()
        
        # Store original columns from registered_df (excluding the new status columns we just added)
        original_columns = [col for col in registered_df.columns if col != "terminal_id"]
        
        # Perform LEFT joins to preserve all data from registered_df
        if connected_df is not None:
            connected_ids = connected_df.select("terminal_id").distinct()
            base_df = base_df.join(
                connected_ids.withColumn("is_connected", lit(1)),
                on="terminal_id", 
                how="left"
            ).withColumn("connected", 
                       when(col("is_connected") == 1, lit(1))
                       .otherwise(col("connected"))) \
             .drop("is_connected")
        
        if active_df is not None:
            active_ids = active_df.select("terminal_id").distinct()
            base_df = base_df.join(
                active_ids.withColumn("is_active", lit(1)),
                on="terminal_id", 
                how="left"
            ).withColumn("active", 
                       when(col("is_active") == 1, lit(1))
                       .otherwise(col("active"))) \
             .drop("is_active")
        
        if inactive_df is not None:
            inactive_ids = inactive_df.select("terminal_id").distinct()
            base_df = base_df.join(
                inactive_ids.withColumn("is_inactive", lit(1)),
                on="terminal_id", 
                how="left"
            ).withColumn("inactive", 
                       when(col("is_inactive") == 1, lit(1))
                       .otherwise(col("inactive"))) \
             .drop("is_inactive")
        
        # Join with MongoDB data if available - preserve all terminal records
        if mongo_df is not None:
            mongo_data = mongo_df.select("terminal_id", "last_transaction_time")
            base_df = base_df.join(
                mongo_data, 
                on="terminal_id", 
                how="left"  # LEFT join to preserve all terminals
            )
        else:
            # Add last_transaction_time column if mongo_df is None
            base_df = base_df.withColumn("last_transaction_time", lit(None).cast(StringType()))
        
        #print(f"Data b4 dropping duplicates: {base_df.count()} records")
        # Remove duplicates and force single partition
        base_df = base_df.dropDuplicates(["terminal_id"])
        
        # Unpersist cached data
        #base_df.unpersist()
        
        print(f"Merged dataframe has {base_df.count()} records")
        #print(f"Columns in final dataframe: {final_df.columns}")
        return base_df
    except Exception as e:
        print(f"Error merging dataframes: {e}")
        raise

def process_rca_files_first_load():
    """Process RCA files for initial load"""
    try:
        files = [f for f in os.listdir(local_download_path) if f.endswith(('.xlsx', '.xls'))]
        if not files:
            print("No Excel files found in download directory")
            return False
            
        for filename in files:
            print(f"Processing file: {filename}")
            full_local_download_path = os.path.join(local_download_path, filename)
            
            # Load all sheets with error handling
            registered_df = load_and_cast(full_local_download_path, "REGISTERED TERMINALS")
            connected_df = load_and_cast(full_local_download_path, "CONNECTED TERMINALS")
            active_df = load_and_cast(full_local_download_path, "ACTIVE TERMINALS")
            inactive_df = load_and_cast(full_local_download_path, "INACTIVE TERMINALS")
            mongo_df = extract_terminal_activity_from_mongo()
            
            if registered_df is None:
                print(f"Skipping file {filename} - no registered terminals data")
                continue
            
            # Merge all data
            final_df = merge_dataframes(registered_df, connected_df, active_df, inactive_df, mongo_df)
            
            if final_df.count() == 0:
                print(f"No data to load from {filename}")
                continue
            
            # Load to PostgreSQL with optimized settings
            print(f"Loading {final_df.count()} records to PostgreSQL...")
            success = load_to_postgres_database(
                final_df, "merchant_terminal_rca", 
                postgres_host, postgres_port, postgres_database, postgres_user, postgres_password
            )
            
            if not success:
                print(f"Failed to load data from {filename}")
                return False
            else:
                print(f"Successfully loaded data from {filename}")
            
        return True
    except Exception as e:
        print(f"Error in first load processing: {e}")
        return False

def get_postgres_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        dbname=postgres_database,
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port
    )

def process_rca_files_subsequent_load():
    """Process RCA files for subsequent loads (merge operations) - FIXED VERSION"""
    try:
        files = [f for f in os.listdir(local_download_path) if f.endswith(('.xlsx', '.xls'))]
        if not files:
            print("No Excel files found in download directory")
            return False
            
        overall_success = True
            
        for filename in files:
            print(f"Processing file: {filename}")
            full_local_download_path = os.path.join(local_download_path, filename)
            
            # Load all sheets
            registered_df = load_and_cast(full_local_download_path, "REGISTERED TERMINALS")
            connected_df = load_and_cast(full_local_download_path, "CONNECTED TERMINALS")
            active_df = load_and_cast(full_local_download_path, "ACTIVE TERMINALS")
            inactive_df = load_and_cast(full_local_download_path, "INACTIVE TERMINALS")
            mongo_df = extract_terminal_activity_from_mongo()
            
            if registered_df is None:
                print(f"Skipping file {filename} - no registered terminals data")
                continue
            
            # Merge all data
            final_df = merge_dataframes(registered_df, connected_df, active_df, inactive_df, mongo_df)
            
            if final_df.count() == 0:
                print(f"No data to load from {filename}")
                continue
            
            # Load to staging table with optimized settings
            print(f"Loading {final_df.count()} records to staging table...")
            success = load_to_postgres_database(
                final_df, "merchant_terminal_rca_staging",
                postgres_host, postgres_port, postgres_database, postgres_user, postgres_password
            )
            
            if not success:
                print(f"Failed to load staging data from {filename}")
                overall_success = False
                continue  # Continue with next file instead of returning immediately
        
            # Prepare merge SQL - FIXED to include all columns
            columns = final_df.columns
            cols = [f'"{c}"' for c in columns]
            col_list = ", ".join(cols)
            update_set = ", ".join([f'"{c}" = source."{c}"' for c in columns if c != "terminal_id"])
            values_list = ", ".join([f"source.{c}" for c in cols])
            
            merge_sql = f"""
                MERGE INTO ptsp_schema.merchant_terminal_rca AS target
                USING ptsp_schema.merchant_terminal_rca_staging AS source
                ON target."terminal_id" = source."terminal_id"
                
                WHEN MATCHED THEN
                    UPDATE SET 
                        {update_set}
                
                WHEN NOT MATCHED THEN
                    INSERT ({col_list})
                    VALUES ({values_list});
            """
           
            # Optional: Delete terminals that are no longer in registered list
            delete_sql = """
                DELETE FROM ptsp_schema.merchant_terminal_rca AS target
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM ptsp_schema.merchant_terminal_rca_staging AS source
                    WHERE target."terminal_id" = source."terminal_id"
                );
            """
            
            conn = get_postgres_connection()
            
            try:
                with conn.cursor() as cursor:
                    print("Executing MERGE statement...")
                    cursor.execute(merge_sql)
                    merged_count = cursor.rowcount
                    
                    print("Executing DELETE statement...")
                    cursor.execute(delete_sql)
                    deleted_count = cursor.rowcount
                    
                    conn.commit()
                    print(f"Transaction committed successfully. Merged: {merged_count}, Deleted: {deleted_count}")
                
            except Exception as e:
                conn.rollback()
                print(f"Transaction failed. Rolling back changes. Error: {e}")
                overall_success = False
            finally:
                if conn:
                    conn.close()
                    
            # Clean up staging table after successful merge
            if overall_success:
                conn = get_postgres_connection()
                try:
                    with conn.cursor() as cursor:
                        #cursor.execute("TRUNCATE TABLE ptsp_schema.merchant_terminal_rca_staging;")
                        #conn.commit()
                        #print("Staging table truncated successfully.")
                        vacuum_merchant_staging_table()
                except Exception as e:
                    print(f"Error truncating staging table: {e}")
                finally:
                    if conn:
                        conn.close()
                    
        return overall_success  # Return overall success status
    except Exception as e:
        print(f"Error in subsequent load processing: {e}")
        return False

def vacuum_merchant_staging_table():
    """Vacuum and analyze the target table"""
    try:
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            database=postgres_database,
            user=postgres_user,
            password=postgres_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("Vacuuming and analyzing merchant_terminal_rca_staging table...")
        cursor.execute("VACUUM ANALYZE ptsp_schema.merchant_terminal_rca_staging")
        
        print("Table maintenance completed successfully")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error during table maintenance: {e}")



def check_table_exists(table_name):
    """Check if a table exists in the PostgreSQL database."""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'ptsp_schema' 
                AND table_name = '{table_name}'
            );
        """)
        exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"Error checking if table exists: {e}")
        return False

def cleanup_downloaded_files():
    """Clean up downloaded files to free disk space"""
    try:
        for filename in os.listdir(local_download_path):
            file_path = os.path.join(local_download_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Cleaned up: {filename}")
    except Exception as e:
        print(f"Error cleaning up files: {e}")

def main():
    """Main execution function"""
    try:
        print("Starting RCA pipeline...")
        
      
        # Process based on table existence
        if not check_table_exists("merchant_terminal_rca"):
            print("Performing first load...")
            success = process_rca_files_first_load()
            if success:
                print("First load completed successfully")
            else:
                print("First load failed")
                return
        else:
            print("Performing subsequent load...")
            success = process_rca_files_subsequent_load()
            if success:
                print("Subsequent load completed successfully")
            else:
                print("Subsequent load failed")
                return
        
        # Cleanup downloaded files
        cleanup_downloaded_files()
        
        print("RCA pipeline completed successfully.")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        # Clean up Spark session
        if 'spark' in globals():
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()