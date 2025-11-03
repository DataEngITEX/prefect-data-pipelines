from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import psycopg2
import urllib.parse
import json

# Database configurations as individual variables
MIDDLEWARE_MONGODB_HOST = "197.253.19.77"
MIDDLEWARE_MONGODB_PORT = "22002"
MIDDLEWARE_MONGODB_DATABASE = "eftEngine"
MIDDLEWARE_MONGODB_USERNAME = "dataeng"
MIDDLEWARE_MONGODB_PASSWORD = "4488qwe"

TAMS_HOST = "192.168.0.134"
TAMS_PORT = "5432"
TAMS_DATABASE = "tams"
TAMS_USER = "admin"
TAMS_PASSWORD = "tams"

DATA_WAREHOUSE_HOST = "192.168.0.244"
DATA_WAREHOUSE_PORT = "5432"
DATA_WAREHOUSE_DATABASE = "data_warehouse"
DATA_WAREHOUSE_USER = "admin"
DATA_WAREHOUSE_PASSWORD = "ITEX2024"

def create_spark_session():
    """Create and configure Spark session"""
    try:
        spark = SparkSession.builder \
            .appName("Terminals Data ETL - Batched Processing") \
            .master("local[*]") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.jars", 
                   r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\postgresql-42.6.0.jar,"
                   r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\mongo-spark-connector_2.12-10.5.0.jar") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        print(f"Failed to start Spark session: {e}")
        raise

def generate_journal_name(date: datetime) -> str:
    """Generate journal name string based on input date quarter"""
    year_short = date.strftime("%y")
    month = date.month
    if 1 <= month <= 3:
        start_month, end_month = 1, 3
    elif 4 <= month <= 6:
        start_month, end_month = 4, 6
    elif 7 <= month <= 9:
        start_month, end_month = 7, 9
    else:
        start_month, end_month = 10, 12
    return f"journals_{year_short}_{start_month:02d}_{end_month:02d}"

def clean_terminal_id(terminal_id_col):
    """Clean terminal ID - remove everything after E, keep decimal, pad to 9 characters"""
    return when(terminal_id_col.contains("."), 
               rpad(
                   split(terminal_id_col, "E").getItem(0),
                   9, "0"
               )
           ).otherwise(terminal_id_col)

def extract_tams_data(spark):
    """Extract terminal data from TAMS database"""
    try:
        tams_url = f"jdbc:postgresql://{TAMS_HOST}:{TAMS_PORT}/{TAMS_DATABASE}"
        
        print("Extracting data from TAMS terminals table...")
        
        df = spark.read \
            .format("jdbc") \
            .option("url", tams_url) \
            .option("dbtable", """
                (SELECT 
                    trm_termid, 
                    trm_lastconnectdate, 
                    trm_connectcount, 
                    trm_version,
                    trm_serialno, 
                    trm_model,
                    trm_session, 
                    q__version, 
                    trm_geolong, 
                    trm_geolat
                FROM terminals) AS t
            """) \
            .option("user", TAMS_USER) \
            .option("password", TAMS_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        df = df.withColumn("trm_termid", clean_terminal_id(col("trm_termid")))
        
        record_count = df.count()
        print(f"Successfully extracted {record_count} records from TAMS terminals")
        return df
        
    except Exception as e:
        print(f"Error extracting TAMS data: {e}")
        raise

def read_ims_bank_codes(spark):
    """Read IMS bank codes table from PostgreSQL"""
    try:
        dw_url = f"jdbc:postgresql://{DATA_WAREHOUSE_HOST}:{DATA_WAREHOUSE_PORT}/{DATA_WAREHOUSE_DATABASE}"
        
        bank_codes_df = spark.read \
            .format("jdbc") \
            .option("url", dw_url) \
            .option("dbtable", "ptsp_reports.ims_bank_codes") \
            .option("user", DATA_WAREHOUSE_USER) \
            .option("password", DATA_WAREHOUSE_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        print("Successfully loaded IMS bank codes table")
        return bank_codes_df
    except Exception as e:
        print(f"Error reading IMS bank codes: {e}")
        raise

def add_bank_column(df, bank_codes_df):
    """Add bank column based on first 5 characters of terminal_id matching bank_code"""
    try:
        df = df.withColumn(
            "bank_code_prefix", 
            when(col("terminal_id").contains("."), substring(col("terminal_id"), 1, 5))
            .otherwise(substring(col("terminal_id"), 1, 4))
        )            
        
        result_df = df.alias("t") \
            .join(
                bank_codes_df.alias("b"),
                col("t.bank_code_prefix") == col("b.bank_code"),
                "left"
            ) \
            .select(
                col("t.*"),
                col("b.bank").alias("bank_name")
            )
        
        return result_df.drop("bank_code_prefix")
    except Exception as e:
        print(f"Error adding bank column: {e}")
        raise

def transform_and_join_data(middleware_df, tams_df, bank_codes_df):
    """Transform and join the datasets for a single journal"""
    try:
        print("Transforming and joining data...")
        
        joined_df = middleware_df.alias("mw") \
            .join(
                tams_df.alias("tams"), 
                col("mw.terminalid") == col("tams.trm_termid"), 
                "left"
            )
        
        final_df = joined_df.select(
            col("mw.terminalid").alias("terminal_id"),
            col("mw.last_transaction_time"),
            col("mw.last_connect_date").alias("last_connect_date"),
            col("tams.trm_serialno").alias("serial_number"),
            col("tams.trm_model").alias("terminal_model"),
            col("tams.trm_version").alias("terminal_version"),
            col("tams.q__version").alias("q_version"),
            col("tams.trm_connectcount").alias("connect_count"),
            col("tams.trm_session").alias("terminal_session"),
            col("tams.trm_geolong").alias("longitude"),
            col("tams.trm_geolat").alias("latitude")
        )
        
        final_with_banks = add_bank_column(final_df, bank_codes_df)
        record_count = final_with_banks.count()
        print(f"Transformation complete. Final record count: {record_count}")
        
        return final_with_banks
        
    except Exception as e:
        print(f"Error transforming data: {e}")
        raise

def write_to_data_warehouse_incremental(df, is_first_batch=False):
    """Write data to data warehouse incrementally"""
    try:
        dw_url = f"jdbc:postgresql://{DATA_WAREHOUSE_HOST}:{DATA_WAREHOUSE_PORT}/{DATA_WAREHOUSE_DATABASE}"
        
        if is_first_batch:
            print("First batch - overwriting terminals_serialno table...")
            df.write \
                .format("jdbc") \
                .option("url", dw_url) \
                .option("dbtable", "terminals_serialno") \
                .option("user", DATA_WAREHOUSE_USER) \
                .option("password", DATA_WAREHOUSE_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .option("truncate", "true") \
                .mode("overwrite") \
                .save()
        else:
            print("Appending batch to terminals_serialno table...")
            df.write \
                .format("jdbc") \
                .option("url", dw_url) \
                .option("dbtable", "terminals_serialno") \
                .option("user", DATA_WAREHOUSE_USER) \
                .option("password", DATA_WAREHOUSE_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        
        print("Data successfully written to terminals_serialno table")
        
    except Exception as e:
        print(f"Error writing to data warehouse: {e}")
        raise

def process_journal_in_batches(spark, journal, tams_df, bank_codes_df, is_first_journal=False, batch_size=1000000):
    """Process journal data in batches using _id field for pagination"""
    try:
        print(f"Processing journal: {journal} in batches of {batch_size}")
        
        # Schema for the data
        schema = StructType([
            StructField("_id", StringType(), True),
            StructField("terminalId", StringType(), True),
            StructField("transactionTime", StringType(), True),
            StructField("updatedAt", StringType(), True)
        ])
        
        mongo_options = {
            "spark.mongodb.read.connection.uri": f"mongodb://{MIDDLEWARE_MONGODB_USERNAME}:{urllib.parse.quote_plus(MIDDLEWARE_MONGODB_PASSWORD)}@{MIDDLEWARE_MONGODB_HOST}:{MIDDLEWARE_MONGODB_PORT}/{MIDDLEWARE_MONGODB_DATABASE}?authSource=admin&socketTimeoutMS=3600000",
            "spark.mongodb.read.database": MIDDLEWARE_MONGODB_DATABASE,
            "spark.mongodb.read.collection": journal,
            "inferSchema": "false"
        }
        
        # First, get the min and max _id to determine batches
        print("Getting _id range for batching...")
        id_range_pipeline = [
            {"$match": {"transactionType": "Purchase"}},
            {"$group": {
                "_id": None,
                "minId": {"$min": "$_id"},
                "maxId": {"$max": "$_id"}
            }}
        ]
        
        mongo_options["spark.mongodb.read.aggregation.pipeline"] = json.dumps(id_range_pipeline)
        
        id_range_schema = StructType([
            StructField("minId", StringType(), True),
            StructField("maxId", StringType(), True)
        ])
        
        id_range_df = spark.read.format("mongodb") \
            .schema(id_range_schema) \
            .options(**mongo_options) \
            .load()
        
        if id_range_df.rdd.isEmpty():
            print(f"No purchase records found in {journal}")
            return 0
        
        id_range = id_range_df.collect()[0]
        min_id = id_range['minId']
        max_id = id_range['maxId']
        
        print(f"ID range: {min_id} to {max_id}")
        
        # Process in batches
        all_batches_agg = []
        total_processed = 0
        current_min = min_id
        
        while current_min <= max_id:
            print(f"Processing batch starting from _id: {current_min}")
            
            # Get batch of data using _id range
            batch_pipeline = [
                {"$match": {
                    "transactionType": "Purchase",
                    "_id": {"$gte": current_min}
                }},
                {"$sort": {"_id": 1}},
                {"$limit": batch_size},
                {"$project": {
                    "_id": 1,
                    "terminalId": 1,
                    "transactionTime": 1,
                    "updatedAt": 1
                }}
            ]
            
            mongo_options["spark.mongodb.read.aggregation.pipeline"] = json.dumps(batch_pipeline)
            
            batch_df = spark.read.format("mongodb") \
                .schema(schema) \
                .options(**mongo_options) \
                .load()
            
            if batch_df.rdd.isEmpty():
                print("No more records in batch")
                break
            
            # Convert to lowercase column names
            batch_df = batch_df.toDF(*[col.lower() for col in batch_df.columns])
            
            # Get the max _id in this batch for next iteration
            max_id_in_batch = batch_df.agg(max("_id")).collect()[0][0]
            
            # Group by terminalId for this batch
            batch_agg = batch_df.groupBy("terminalid") \
                .agg(
                    max("transactiontime").alias("last_transaction_time"),
                    max("updatedat").alias("last_connect_date")
                )
            
            # Clean terminal IDs
            batch_agg = batch_agg.withColumn(
                "terminalid", 
                clean_terminal_id(col("terminalid"))
            )
            
            batch_count = batch_agg.count()
            total_processed += batch_count
            print(f"Batch processed: {batch_count} terminal records (Total: {total_processed})")
            
            all_batches_agg.append(batch_agg)
            
            # Move to next batch
            if max_id_in_batch == max_id:
                break
            current_min = max_id_in_batch
            
            # Clear cache to free memory
            spark.catalog.clearCache()
        
        if not all_batches_agg:
            print("No batches were processed")
            return 0
        
        # Combine all batches with final aggregation
        print("Combining all batches...")
        if len(all_batches_agg) == 1:
            final_agg = all_batches_agg[0]
        else:
            # Union all batches
            combined_agg = all_batches_agg[0]
            for batch in all_batches_agg[1:]:
                combined_agg = combined_agg.union(batch)
            
            # Final aggregation to get true max values
            final_agg = combined_agg.groupBy("terminalid") \
                .agg(
                    max("last_transaction_time").alias("last_transaction_time"),
                    max("last_connect_date").alias("last_connect_date")
                )
        
        final_count = final_agg.count()
        print(f"Final aggregation: {final_count} unique terminal records")
        
        # Transform and join data
        final_df = transform_and_join_data(final_agg, tams_df, bank_codes_df)
        
        # Write to database
        write_to_data_warehouse_incremental(final_df, is_first_journal)
        
        return final_count
        
    except Exception as e:
        print(f"Error processing journal {journal} in batches: {e}")
        return 0

def vacuum_target_table():
    """Vacuum and analyze the target table"""
    try:
        conn = psycopg2.connect(
            host=DATA_WAREHOUSE_HOST,
            port=DATA_WAREHOUSE_PORT,
            database=DATA_WAREHOUSE_DATABASE,
            user=DATA_WAREHOUSE_USER,
            password=DATA_WAREHOUSE_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("Vacuuming and analyzing terminals_serialno table...")
        cursor.execute("VACUUM ANALYZE terminals_serialno")
        
        print("Table maintenance completed successfully")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error during table maintenance: {e}")

def main():
    """Main ETL process with batched journal processing"""
    spark = None
    try:
        print("Starting Terminals ETL Process - Batched Mode...")
        print(f"Execution date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Initialize Spark
        spark = create_spark_session()
        
        # Step 1: Extract TAMS terminal data
        tams_df = extract_tams_data(spark)
        
        # Step 2: Get bank codes
        bank_codes_df = read_ims_bank_codes(spark)
        
        # Step 3: Define journals to process
        journals = ['journals_25_01_03', 'journals_25_04_06', 'journals_25_07_09', 'journals_25_10_12']
        print(f"Processing journals in batches: {journals}")
        
        # Step 4: Process each journal in batches
        total_records = 0
        successful_journals = []
        failed_journals = []
        
        for i, journal in enumerate(journals):
            print(f"\n{'='*60}")
            print(f"Processing journal {i+1}/{len(journals)}: {journal}")
            print(f"{'='*60}")
            
            is_first_journal = (i == 0)
            
            try:
                records_processed = process_journal_in_batches(
                    spark, journal, tams_df, bank_codes_df, is_first_journal, batch_size=1000000
                )
                
                if records_processed > 0:
                    total_records += records_processed
                    successful_journals.append(journal)
                    print(f"[SUCCESS] Completed {journal} - {records_processed} records")
                else:
                    failed_journals.append(journal)
                    print(f"[FAILED] {journal} - 0 records processed")
                
            except Exception as e:
                failed_journals.append(journal)
                print(f"[FAILED] Error processing {journal}: {e}")
                continue
        
        print(f"\n=== FINAL PROCESSING SUMMARY ===")
        print(f"Total records processed: {total_records}")
        print(f"Successful journals ({len(successful_journals)}): {successful_journals}")
        if failed_journals:
            print(f"Failed journals ({len(failed_journals)}): {failed_journals}")
        
        # Step 5: Perform table maintenance
        if successful_journals:
            vacuum_target_table()
            print("ETL process completed successfully!")
        else:
            print("ETL process failed - no journals were processed successfully")
        
    except Exception as e:
        print(f"ETL process failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()