# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, TimestampType, StructType, StructField
import urllib.parse
from pymongo import MongoClient
import psycopg2 as psy
from psycopg2 import IntegrityError
from psycopg2.errors import UniqueViolation
import sqlalchemy
from sqlalchemy import create_engine
import json
import os
from datetime import timedelta, datetime
from dateutil import parser
from prefect import flow, task
from prefect.blocks.system import Secret
from bson.objectid import ObjectId

# Load configuration from Prefect secrets blocks
secrets = Secret.load("middleware-purchases-vas-merchants-pipeline-secret").get()
postgres_database=secrets["postgres_database"]
postgres_user=secrets["postgres_user"]
postgres_host=secrets["postgres_host"]
postgres_port=secrets["postgres_port"]
postgres_password=secrets["postgres_password"]
mdw_username=secrets["mdw_username"]
mdw_host=secrets["mdw_host"]
mdw_port=secrets["mdw_port"]
mdw_password=secrets["mdw_password"]
mdw_database=secrets["mdw_database"]



# Initialize Spark Session
spark = SparkSession.builder \
    .appName("MIDDLEWARE(MDW) MongoDb-Data-Processing Purchases and MDW_VAS") \
    .config("spark.jars",
            r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\mongo-spark-connector_2.12-10.5.0.jar,"
            r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\bson-5.5.1.jar,"
            r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\mongodb-driver-sync-5.5.1.jar,"
            r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\mongodb-driver-core-5.5.1.jar"
            ) \
    .config("spark.executor.memory", "5g") \
    .config("spark.driver.memory", "5g") \
    .getOrCreate()
#spark.sparkContext.setLogLevel("DEBUG")

# Helper functions
def generate_journal_name(date: datetime) -> str:
    """Generate journal name string based on input date quarter."""
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

def generate_mdw_quarterly_table_name_from_date(date: datetime) -> str:
    """Returns the quarterly table name in format mdw_purchases_YY_MM"""
    """This function generates the table name that we connect to on postgress database.
    The database contains tables for each quarter of the year"""
    
    quarter_start_month = 3 * ((date.month - 1) // 3) + 1
    print(f"The quarterly table to insert data into is: mdw_purchases_{date.strftime('%y')}_{quarter_start_month:02d}")

    return f"mdw_purchases_{date.strftime('%y')}_{quarter_start_month:02d}"

# PostgreSQL connection helper
def get_postgres_connection():
    """Create PostgreSQL connection"""
    return psy.connect(
        dbname=postgres_database,
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port
    )


def extract_mdw_purchases():
    latest_time = get_latest_transaction_time(generate_mdw_quarterly_table_name_from_date(datetime.today()))
    journal_name = generate_journal_name(datetime.today())

    start_iso = latest_time if isinstance(latest_time,str) else  latest_time.isoformat(timespec='milliseconds') + 'Z'
    stop_iso = datetime.now().isoformat(timespec='milliseconds') + 'Z'
    print(f"ISO range: {start_iso} to {stop_iso}")

    projection_fields = [
      "_id", "customData", "posEntryMode","posDataCode", "profiledIntlTid", "prrn", "deliveryStatus",
      "receipt", "receiptSent", "printedReceiptData", "rrn","onlinePin","merchantName","merchantAddress",
      "merchantId","terminalId","STAN", "transactionTime","merchantCategoryCode","handlerName", "MTI","maskedPan",
    "processingCode", "amount", "currencyCode", "messageReason", "originalDataElements", "customerRef",
    "cardExpiry", "handlerUsed", "cardName", "isContactless", "tvr", "crim", "createdAt", "updatedAt", "__v",
    "fiic","authCode","failOverRrn","handlerResponseTime", "interSwitchResponse", "oldResCode","responseCode",
    "script","tamsBatchNo","tamsMessage","tamsRrn","tamsStatus","tamsTransNo", "upslTerminalIdUsed", "write2pos",
    "timeFromNibss","timeToNibss","timeToPos","totalTrnxTime","pfmNotified","ejournalData","notified",
    "isNotified","customTransactionId"
    ]
    lowercase_columns = [c.lower() for c in projection_fields]


    explicit_schema = StructType([StructField(f, StringType(), True) for f in projection_fields])

    mongo_options = {
        "spark.mongodb.read.connection.uri": f"mongodb://{mdw_username}:{urllib.parse.quote_plus(mdw_password)}@{mdw_host}:{mdw_port}/{mdw_database}?authSource=admin",
        "spark.mongodb.read.database": mdw_database,
        "spark.mongodb.read.collection": journal_name,
        "inferSchema": "false"
        
    }

    project_stage = {"$project": {f: 1 for f in projection_fields}}
    last_id = None
    batch_size = 50000
    total_records, batch_num = 0, 0
    print(f"Starting batch process extraction from: {journal_name} into {generate_mdw_quarterly_table_name_from_date(datetime.today())} table")
    
    while True:
        match_stage = {
            "transactionTime": {"$gt": {"$date": start_iso}, "$lte": {"$date": stop_iso}},
            "transactionType": "Purchase"
        }
        if last_id:
            match_stage["_id"] = {"$gt": {"$oid": str(last_id)}}

        pipeline = [
            {"$match": match_stage},
            {"$limit": batch_size},
            project_stage
        ]
        mongo_options["spark.mongodb.read.aggregation.pipeline"] = json.dumps(pipeline)

        df = spark.read.format("mongodb") \
            .schema(explicit_schema) \
            .options(**mongo_options) \
            .load()
        df = df.toDF(*lowercase_columns)
        if df.rdd.isEmpty():
            print("Batch load complete. No more records to read from middleware.")
            break
        
        df = df.withColumn("amount", col("amount") / 100)
        

        last_row = df.orderBy(col("_id").desc()).limit(1).collect()[0]
        last_id = last_row["_id"]

        cnt = df.count()
        batch_num += 1
        total_records += cnt
        print(f"Batch {batch_num}: {cnt} records (Total: {total_records}), last ID: {last_id}")

        load_to_postgres_database(df, generate_mdw_quarterly_table_name_from_date(datetime.today()))

def load_to_postgres_database(df, table_name):
    """
    Loads a Spark DataFrame into a PostgreSQL table."""
    
    
    print("Loading data into Data Warehouse (Postgres)...")
    
    # 1. Get the DataFrame schema
    schema = df.schema
    
    # 2. Build the column type string for the JDBC writer
    # This is crucial for Spark to correctly create the table if it doesn't exist.
    column_types = []
    for field in schema.fields:
        # Map Spark data types to PostgreSQL data types.
        # This is a simplified example; you might need a more comprehensive mapping.
        if isinstance(field.dataType, StringType):
            db_type = "TEXT" # Changed from VARCHAR(255) to TEXT for unlimited length
        else:
            db_type = "TEXT" # Defaulting to a generic type for other types
            
        column_types.append(f'"{field.name}" {db_type}')
    
    create_table_column_types_string = ", ".join(column_types)

    writer = df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}") \
        .option("dbtable", f"vas_schema.{table_name}") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .option("createTableColumnTypes", create_table_column_types_string) \
        .mode("append") # Use append mode, which creates the table if it doesn't exist

    try:
        writer.save()
        print(f"Data successfully loaded into vas_schema.{table_name}.")
    except Exception as e:
        print(f"Error loading data: {e}")
        # Add more specific error handling here if needed.
def extract_mdw_vas():
    """function to extract VAS data from MongoDB using Spark, similar to the mdw_purchases function."""
    

    
    # Get the latest transaction time from the new dynamic table
    warehouse_table_name = "mdw_vas"
    latest_time = get_latest_transaction_time(warehouse_table_name)
    journal_name = generate_journal_name(datetime.today())

    start_iso = latest_time if isinstance(latest_time,str) else latest_time.isoformat(timespec='milliseconds') + 'Z'
    stop_iso = datetime.now().isoformat(timespec='milliseconds') + 'Z'
    print(f"ISO range: {start_iso} to {stop_iso}")

    # Define projection fields as a list of strings, consistent with the purchases function.
    projection_fields = [
        "_id","vasData","receipt","receiptSent","ejournalData","customData","prrn", "rrn", "onlinePin",
    "merchantName","merchantAddress","merchantId","terminalId","STAN","transactionTime", "merchantCategoryCode",
    "handlerName","MTI","maskedPan","processingCode","amount","currencyCode","messageReason","originalDataElements",
    "customerRef","cardExpiry","isVasComplete","handlerUsed", "tvr", "crim","__v","fiic","authCode","failOverRrn","handlerResponseTime",
    "interSwitchResponse","oldResCode","responseCode","script","tamsBatchNo","tamsMessage","tamsRrn","tamsStatus",
    "tamsTransNo","upslTerminalIdUsed","write2pos","notified","pfmNotified","posEntryMode","isContactless",
    "cardName","updatedAt","createdAt","posDataCode","isNotified","customTransactionId","virtualMerchantName","virtualTid",
    "profiledIntlTid","medussaRequestMid","medussaRequestTid","medussaResponse","medussaRequestMerchantName",
    "timeFromNibss","timeToNibss","deliveryStatus","totalTrnxTime","printedReceiptData","timeToPos"
    ]
    lowercase_columns = [c.lower() for c in projection_fields]
    explicit_schema = StructType([StructField(f, StringType(), True) for f in projection_fields])

    mongo_options = {
        "spark.mongodb.read.connection.uri": f"mongodb://{mdw_username}:{urllib.parse.quote_plus(mdw_password)}@{mdw_host}:{mdw_port}/{mdw_database}?authSource=admin",
        "spark.mongodb.read.database": mdw_database,
        "spark.mongodb.read.collection": journal_name,
        "inferSchema": "false"
    }

    project_stage = {"$project": {f: 1 for f in projection_fields}}
    
    match_stage = {
            "transactionTime": {"$gt": {"$date": start_iso}, "$lte": {"$date": stop_iso}},
            "transactionType": "VAS"
        }
    last_id = None
    batch_size = 25000
    total_records, batch_num = 0, 0
    
    print(f"Starting batch process extraction from: {journal_name} into mdw_vas table")
    
    while True:
       
        if last_id:
            match_stage["_id"] = {"$gt": {"$oid": str(last_id)}}

        pipeline = [
            {"$match": match_stage},
            {"$limit": batch_size},
            project_stage
        ]
        mongo_options["spark.mongodb.read.aggregation.pipeline"] = json.dumps(pipeline)

        df = spark.read.format("mongodb") \
            .schema(explicit_schema) \
            .options(**mongo_options) \
            .load()
        
        if df.rdd.isEmpty():
            print("Batch load complete. No more records to read from middleware.")
            break
            
        df = df.toDF(*lowercase_columns)
        
        # Consistent data transformations
        df = df.withColumn("amount", col("amount") / 100)
        
        last_row = df.orderBy(col("_id").desc()).limit(1).collect()[0]
        last_id = last_row["_id"]

        cnt = df.count()
        batch_num += 1
        total_records += cnt
        print(f"Batch {batch_num}: {cnt} records (Total: {total_records}), last ID: {last_id}")

        load_to_postgres_database(df, warehouse_table_name)



def extract_and_load_mdw_merchants():
    """Extract merchants from MongoDB using Spark, deduplicate in Spark, and load to PostgreSQL"""
    try:
      

        print("Extracting merchants from MongoDB...")

        merchants_df = spark.read.format("mongodb") \
            .option("spark.mongodb.read.connection.uri", f"mongodb://{mdw_username}:{urllib.parse.quote_plus(mdw_password)}@"
                            f"{mdw_host}:{mdw_port}/{mdw_database}?authSource=admin") \
            .option("spark.mongodb.read.database", mdw_database) \
            .option("spark.mongodb.read.collection", "merchants") \
            .load()

        if merchants_df.rdd.isEmpty():
            print("No merchants found.")
            return

        # Cast all columns to string, lowercase column names
        for column in merchants_df.columns:
            merchants_df = merchants_df.withColumn(column, col(column).cast(StringType()))
        merchants_df = merchants_df.toDF(*[c.lower() for c in merchants_df.columns])

        print(f"Fetched {merchants_df.count()} merchant records.")

        # Drop MongoDB's _id column if it exists
        if "_id" in merchants_df.columns:
            merchants_df = merchants_df.drop("_id")

        # Remove duplicates using Spark
        # Deduplication key can be adjusted; assumed to be 'merchant_id'
        if 'merchant_id' in merchants_df.columns:
            merchants_df = merchants_df.dropDuplicates(["merchant_id"])
            print(f"Deduplicated to {merchants_df.count()} unique merchants.")
        else:
            print("Warning: 'merchant_id' not found. Skipping deduplication.")


        # Load to PostgreSQL
        print("Loading merchants into PostgreSQL warehouse...")

        merchants_df.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}") \
            .option("dbtable", "vas_schema.mdw_merchants") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        print("Merchants loaded successfully.")

    except Exception as e:
        print("An error occurred during merchant ETL:", str(e))

def get_latest_transaction_time(table_name, schema='vas_schema', timestamp_col='transactiontime'):
    """Get latest transaction time from PostgreSQL"""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        query = f"SELECT MAX({timestamp_col}) FROM {schema}.{table_name}"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        conn.close()

        if result is None:
            today = datetime.today()
            first_day_of_month = datetime(today.year, today.month, 1)
            fallback = first_day_of_month - timedelta(days=1)
            return fallback.replace(hour=23, minute=59, second=59, microsecond=999000)
        
        if isinstance(result, str):
            return result
        return result

    except Exception as e:
        print(f"Failed to fetch latest timestamp for {table_name}: {e}")
        today = datetime.today()
        first_day_of_month = datetime(today.year, today.month, 1)
        fallback = first_day_of_month - timedelta(days=1)
        return fallback.replace(hour=23, minute=59, second=59, microsecond=999000)

# Main Prefect flow

def mdw_data_pipeline():
    # Extract and load purchase data
    extract_mdw_purchases()
    
    # Extract and load VAS data
    extract_mdw_vas()
    
    # Extract and load merchants data
    extract_and_load_mdw_merchants()
    
 

if __name__ == '__main__':
    try:
        mdw_data_pipeline()
    except Exception as e:
        print(f"An error occurred in the MDW data pipeline: {e}")
        
