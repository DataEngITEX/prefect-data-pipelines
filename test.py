from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws

# PostgreSQL config
pg_config = {
    "host": "192.168.0.191",
    "port": "5432",
    "database": "payvice",
    "user": "dataengr",
    "password": "bts8nI0KuAEFbg"
}
pg_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
pg_properties = {
    "user": pg_config["user"],
    "password": pg_config["password"],
    "driver": "org.postgresql.Driver"
}

# File paths
input_file = r"C:\Users\data.engineer\Documents\Prefect\data.csv"
output_dir = r"C:\Users\data.engineer\Documents\Prefect\data_processed.csv"

# Start Spark
spark = SparkSession.builder \
    .appName(" Fill Missing") \
    .master("local[*]") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Read CBN file
cbn_df = spark.read.csv(input_file, header=True, inferSchema=True)

# Read materialized view from PostgreSQL
view_df = spark.read.jdbc(
    url=pg_url,
    table="public.users_wallet_detailed",
    properties=pg_properties
)

# Join on Wallet ID
joined = cbn_df.alias("cbn").join(
    view_df.alias("view"),
    col("cbn.Wallet ID") == col("view.wallet_id"),
    "left"
)

# Define helper for empty values
def is_empty(c):
    return (c.isNull()) | (c == "") | (c == "0")

# Fill missing fields using view data
filled = joined.select(
    col("cbn.Wallet ID").alias("Wallet ID"),
    when(is_empty(col("cbn.Name")), concat_ws(" ", col("view.first_name"), col("view.middle_name"), col("view.last_name"))
).otherwise(col("cbn.Name")).alias("Name"),
    when(is_empty(col("cbn.Email address")), col("view.email")).otherwise(col("cbn.Email address")).alias("Email address"),
    when(is_empty(col("cbn.Phone number")), col("view.phone_number")).otherwise(col("cbn.Phone number")).alias("Phone number"),
    when(is_empty(col("cbn.Address")), col("view.address")).otherwise(col("cbn.Address")).alias("Address"),
    when(is_empty(col("cbn.State")), col("view.state_of_residence")).otherwise(col("cbn.State")).alias("State"),
    when(is_empty(col("cbn.Region")), col("view.lga_of_residence")).otherwise(col("cbn.Region")).alias("Region"),
    col("cbn.DOB"),
    col("cbn.MONTH (DOB)"),
    col("cbn.STATUS")
)

# Save to CSV
filled.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

print(f"Cleaned file written to {output_dir}")
