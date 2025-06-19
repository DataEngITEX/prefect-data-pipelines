
import os
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower
import psycopg2
from office365.runtime.auth.user_credential import UserCredential
from prefect.blocks.system import Secret
import sys

#from openpyxl.utils.exceptions import InvalidFileException
 #loacl path to downloa 

secret_json= Secret.load("cbn-reports-secret").get()  
local_download_path = secret_json["local_download_path"]
full_local_download_file_path=None
sharepoint_folder_path = secret_json["sharepoint_folder_path"]
os.environ["PYSPARK_PYTHON"] =secret_json["PYSPARK_PYTHON"]

def login_and_get_context_from_sharepoint():

    
    sharepoint_username = secret_json["sharepoint_username"]
    sharepoint_password = secret_json["sharepoint_password"]
    sharepoint_site_url=secret_json["sharepoint_site_url"]
        # Authenticate using UserCredential
    ctx = ClientContext(sharepoint_site_url).with_credentials(
            UserCredential(sharepoint_username, sharepoint_password)
        )
    return ctx
def download_cbn_report_from_sharepoint():

    try:
   
        
        ctx=login_and_get_context_from_sharepoint()
         # Server-relative path to the folder
        # Access the folder and list files
        folder = ctx.web.get_folder_by_server_relative_url(sharepoint_folder_path)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        # Step 1: Download the CBN report
        print("Downloading cbn report from sharepoint...")
        for file in files:
            file_name = file.properties["Name"]
            local_file_path = os.path.join(local_download_path, file_name)
            global full_local_download_file_path
            full_local_download_file_path=local_file_path #assigns the downloaded file location

            with open(local_file_path, "wb") as f:
                file.download(f).execute_query()
                print(f"Downloaded cbn report: {file_name}   {local_file_path}")
    except Exception as e:
        print(f" Error downloading cbn report from SharePoint: {e}")


'''def upload_processed_cbn_report_to_sharepoint():
    print("Uploading updated report...")
    with open(local_output_file, 'rb') as content_file:
            content = content_file.read()
            target_folder = ctx.web.get_folder_by_server_relative_url(f"/sites/yoursite/{remote_output_folder}")
            target_folder.upload_file("bvn_report_updated.csv", content).execute_query()
            print("Updated report uploaded to SharePoint.")'''

def process_cbn_report():
    global full_local_download_file_path
    if not full_local_download_file_path:
        print("No file found. Ensure sharepoint contains cbn report.")
        sys.exit()

    # Step 2: PostgreSQL connection info
    pg_config = secret_json["pg_config"]
    pg_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    pg_properties = {
        "user": pg_config["user"],
        "password": pg_config["password"],
        "driver": "org.postgresql.Driver"
    }

    try:
    # Step 2: Create Spark session
        spark = SparkSession.builder \
            .appName("CBN Report Processing") \
            .master("local[*]") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.hadoop.hadoop.security.authorization", "false") \
            .config("spark.hadoop.hadoop.security.authentication", "simple") \
            .config("spark.jars",r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\spark-excel_2.12-3.5.1_0.20.4.jar") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        print("Failed to start Spark session:", e)
        
        # Step 3: Refresh the materialized view
    print("Updating CBN report")
    def refresh_materialized_view():
        conn = psycopg2.connect(
            dbname=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"],
            host=pg_config["host"],
            port=pg_config["port"]
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("REFRESH MATERIALIZED VIEW public.users_wallet_detailed;")
        cur.close()
        conn.close()
    
    refresh_materialized_view()

    # Step 4: Load CBN report CSV
    cbn_df = read_downloaded_file(spark) #reads based on file type csv or xlsx

    # Step 5: Load materialized view
    view_df = spark.read.jdbc(
        url=pg_url,
        table="public.users_wallet_detailed",
        properties=pg_properties
    )
   
    
    desired_columns = ["wallet", "updated_at", "category", "terminal", "address", "phone_number", "bvn", "volume", "value"]
    cbn_df = cbn_df.toDF(*desired_columns)

    # Step 6: Join both datasets on wallet_id
    joined_df = cbn_df.alias("cbn").join(
        view_df.alias("view"),
        col("cbn.wallet") == col("view.wallet_id"),
        "left"
    )

    # Step 7: Define helper for "empty" values (null, empty string, or "0")
    def is_empty(col_expr):
        return (col_expr.isNull()) | (col_expr == "") | (col_expr == "0")

    # Step 8: Replace BVN – use view.bvn if not empty, else view.nin if not empty
    updated_df = joined_df.withColumn(
        "_bvn",
        when(is_empty(col("cbn.bvn")),
            when(~is_empty(col("view.bvn")), col("view.bvn"))
            .when(~is_empty(col("view.nin")), col("view.nin"))
            .otherwise(col("cbn.bvn"))
        ).otherwise(col("cbn.bvn"))
    )

    # Step 9: Replace phone number – only if view.phone_number is not empty
    updated_df = updated_df.withColumn(
        "_phone_number",
        when(is_empty(col("cbn.phone_number")) & ~is_empty(col("view.phone_number")),
            col("view.phone_number")
        ).otherwise(col("cbn.phone_number"))
    )

    # Step 10: Replace address – only if view.address is not empty
    updated_df = updated_df.withColumn(
        "_address",
        when(is_empty(col("cbn.address")) & ~is_empty(col("view.address")),
            col("view.address")
        ).otherwise(col("cbn.address"))
    )
   
    # Step 11: Rebuild final DataFrame in original CBN column order
    
    final_df = updated_df.select(
    "cbn.wallet",
    "cbn.updated_at",
    "cbn.category",
    "cbn.terminal",
    col("_bvn"),
    col("_phone_number"),
    col("_address"),
    col("volume"),
    col("value")
    )
    final_df=final_df.withColumnRenamed("_phone_number","phone_number").withColumnRenamed("_bvn","bvn").withColumnRenamed("_address","address")
    final_df.show()
    # Step 12: Save output

    final_df.coalesce(1).write.csv(local_download_path, header=True, mode="overwrite")

    for filename in os.listdir(local_download_path):
        if (filename.startswith("part-") and filename.endswith(".csv")) or (filename.startswith("part-") and filename.endswith(".xlsx")):
            
            output_filename = os.path.basename(full_local_download_file_path)
            name, ext = os.path.splitext(output_filename)
            processed_filename = f"{name}_PROCESSED.csv"
            spark_part_file_path = os.path.join(local_download_path, filename)
            output_file_path = os.path.join(local_download_path, processed_filename)
            full_local_download_file_path=output_file_path
            os.rename(spark_part_file_path, output_file_path)  # Rename in-place
            break

    print("CBN report has been successfully updated.")

def upload_cbn_report_to_sharepoint():
    print("Uploading CBN report to Sharepoint folder")
    ctx=login_and_get_context_from_sharepoint()
    sharepoint_upload_folder = ctx.web.get_folder_by_server_relative_url(f'{sharepoint_folder_path}/PROCESSED')
    with open(full_local_download_file_path, "rb") as f:
        content = f.read()
        uploaded_file = sharepoint_upload_folder.upload_file(os.path.basename(full_local_download_file_path), content)
        ctx.execute_query()
        print(f"Successfuly uploaded processed file to SharePoint: {uploaded_file.serverRelativeUrl}")

def delete_all_files_in_sharepoint_folder():
    ctx=login_and_get_context_from_sharepoint()
    folder = ctx.web.get_folder_by_server_relative_url(sharepoint_folder_path)
    files = folder.files
    ctx.load(files)
    ctx.execute_query()

    for file in files:
        file_name = file.properties["Name"]
        file.delete_object()
        print(f"Deleted file from SharePoint: {file_name}")
    ctx.execute_query()
def read_downloaded_file(spark):
    if full_local_download_file_path.endswith(".xlsx"):
        return spark.read \
        .format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(full_local_download_file_path)
        
    return spark.read.csv(full_local_download_file_path, header=True, inferSchema=True)

def remove_files_from_local_download_path():
    
    if len(os.listdir(local_download_path)) < 1:
        print('No files to remove')
        return
    else:
        for file in os.listdir(local_download_path):
            file_to_del = os.path.join(local_download_path, file)

            # Proceed with deletion
            try:
                # Delete the files
                os.remove(file_to_del)
                #shutil.move(file_to_mov,transit_path)
                print(f"{file} removed successfully.")
            except FileNotFoundError:
                print(f"File '{file}' not found.")
            except Exception as e:
                print(f"An error occurred while deleting the file: {e}")



def main():
    remove_files_from_local_download_path()
    download_cbn_report_from_sharepoint()
    process_cbn_report()
    delete_all_files_in_sharepoint_folder()
    upload_cbn_report_to_sharepoint()


if __name__ == '__main__':
    main()