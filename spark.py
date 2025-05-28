from pyspark.sql import SparkSession


# 1. Set critical environment variables


# 2. Configure Spark session with error handling
try:
    spark = SparkSession.builder \
    .appName("DataFrame Test") \
    .master("local[1]") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.default.parallelism", "1") \
    .getOrCreate()

    
    spark.sparkContext.setLogLevel("ERROR")

    # 3. Create sample data with explicit schema handling
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35}
    ]

    # 4. Create DataFrame with explicit error handling
    try:
        df = spark.createDataFrame(data)
        
        # 5. Test operations with progress checks
        print("\nStep 1: Printing columns - ", end="")
        print(df.columns)
        
        print("Step 2: Showing limited data - ")
        
        print("Step 3: Showing full data - ")
        df.show()
        
        print("SUCCESS: All operations completed")
        
    except Exception as df_error:
        print(f"\nDATA PROCESSING ERROR: {str(df_error)}")
        raise

except Exception as spark_error:
    print(f"\nSPARK INITIALIZATION ERROR: {str(spark_error)}")
    raise

finally:
    spark.stop()
    print("Spark session stopped")