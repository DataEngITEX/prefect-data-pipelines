
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from ftplib import FTP, error_perm
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.user_credential import UserCredential
import shutil
import openpyxl
import sys
from prefect.blocks.system import Secret
#import pandas as pd

# SharePoint Details
secret_json= Secret.load("nibss-pipeline-secret").get() 
print(secret_json)
sharepoint_site_url = secret_json["sharepoint_site_url"]
sharepoint_username = secret_json["sharepoint_username"]
sharepoint_password = secret_json["sharepoint_password"]
local_download_path = secret_json["local_download_path"]
write_path = secret_json["write_path"]
sharepoint_rca_path = secret_json["sharepoint_rca_path"]
sharepoint_process_path = secret_json["sharepoint_process_path"] #the to process folder on sharepoint pos
sharepoint_nibss_path = secret_json["sharepoint_nibss_path"]#nibss raw folder

# Function to connect to NIBSS FTP and retrieve files
'''
def retrieve_nibss_file():
    ctx_auth = UserCredential(sharepoint_username, sharepoint_password)
    ctx = ClientContext(sharepoint_site_url).with_credentials(ctx_auth)

    

    # Get the folder and list of files in it
    folder = ctx.web.get_folder_by_server_relative_url(sharepoint_nibss_path)
    files = folder.files
    ctx.load(files)
    ctx.execute_query()
    
    try:
        # Download each file in the folder
        for file in files:
            file_name = file.properties["Name"]
            download_path = os.path.join(local_download_path, file_name)
            
            with open(download_path, "wb") as local_file:
                file.download(local_file).execute_query()
                
            print(f"Downloaded: {file_name} from sharepoint")
            file.delete_object()
            ctx.execute_query()  # Confirm deletion
            print(f"Deleted: {file_name} in NIBSS raw folder on sharepoint")

    except Exception as e:
        print(f"An error occurred while downloading {file} from SharePoint: {e}")

    print("All files downloaded.")
'''
def get_day_suffix(day):
    if 10 <= day % 100 <= 20:
        suffix = 'TH'
    else:
        suffix = {1: 'ST', 2: 'ND', 3: 'RD'}.get(day % 10, 'TH')
    return suffix



def load_to_sharepoint():
    #check if there are any nibss pos/rca files downloaded
    check_if_files_downloaded=[file for file in os.listdir(local_download_path)if os.path.isfile(os.path.join(local_download_path, file))]
    if not check_if_files_downloaded:
        print("No files in directory. Stopping processing now")
        sys.exit(0)
    try:
       
        spark = SparkSession.builder \
            .appName("NIBSS pipeline processing") \
            .master("local[*]") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "3") \
            .config("spark.memory.offHeap.enabled", "false") \
            .config("spark.memory.offHeap.size", "8g") \
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1HeapRegionSize=16m") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1HeapRegionSize=16m") \
            .config("spark.hadoop.hadoop.security.authorization", "false") \
            .config("spark.hadoop.hadoop.security.authentication", "simple") \
            .config("spark.pyspark.python", "C:/Users/data.engineer/Documents/Prefect/prefect-venv/Scripts/python.exe")\
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.ManagedCommitProtocol") \
            .config("spark.sql.parquet.output.committer.class","org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
            .config("spark.jars",r"C:\Spark\spark-3.5.5-bin-hadoop3\jars\spark-excel_2.12-3.5.1_0.20.4.jar") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("DEBUG")
    except Exception as e:
        print("Failed to start spark session: ",e)
       

        
     
    for file in os.listdir(local_download_path):
        local_file_path = os.path.join(local_download_path, file)
        #ctx_auth = UserCredential(sharepoint_username, sharepoint_password)
        #ctx = ClientContext(sharepoint_site_url).with_credentials(ctx_auth)
        
        if 'RCA' not in file:
            if file.endswith('.xlsx'):
                workbook = openpyxl.load_workbook(local_file_path)
                sheet_names = workbook.sheetnames
                num_sheets = len(sheet_names)
                workbook.close()
              
                try:
                    if num_sheets > 1:
                        # Read and combine sheets using Spark
                        failed_df = spark.read.format("com.crealytics.spark.excel") \
                            .option("header", "true") \
                            .option("inferSchema", "true") \
                            .option("dataAddress", "'POS_FAILED'!A1") \
                            .option("treatEmptyValuesAsNulls", "true") \
                            .option("addColorColumns", "false") \
                            .option("maxRowsInMemory", 100) \
                            .load(local_file_path)
                        print("Reading failed pos excel files" )

                        successful_df = spark.read.format("com.crealytics.spark.excel") \
                            .option("header", "true") \
                            .option("inferSchema", "true") \
                            .option("dataAddress", "'POS_SUCCESSFULL'!A1") \
                            .option("treatEmptyValuesAsNulls", "true") \
                            .option("addColorColumns", "false") \
                            .option("maxRowsInMemory", 100) \
                            .load(local_file_path)
                        print("Reading succesful pos excel files" )

                        combined_POS = failed_df.unionByName(successful_df) \
                            .dropDuplicates(["RetrievalReferenceNo"])
                        
                        combined_POS.show()
                        
                        #os.makedirs(write_path, exist_ok=True)
                        # Write back to Excel
                        combined_POS.coalesce(1).write.csv(write_path, header=True, mode="overwrite")

                    elif num_sheets == 1:
                        # Read single sheet with Spark
                        pos_df = spark.read.format("com.crealytics.spark.excel") \
                            .option("header", "true") \
                            .option("inferSchema", "true") \
                            .option("treatEmptyValuesAsNulls", "true") \
                            .option("addColorColumns", "false") \
                            .option("maxRowsInMemory", 100) \
                            .load(local_file_path)

                        deduped_df = pos_df.dropDuplicates(["RetrievalReferenceNo"])
                        
                        deduped_df.coalesce(1).write.csv(write_path, header=True, mode="overwrite")
                        print("Successfully saved file to local processed folder as CSV")
                except Exception as e:
                    print(f"Error processing Excel file {file}: {str(e)}")
                  

            elif file.endswith('.csv'):
                try:
                    # Process CSV with Spark
                    header_row = [
                        'Merchant', 'TerminalID', 'IssuingBank', 'AcquiringBank', 'BIN', 'PAN', 
                        'Date', 'merchant_id', 'Amount', 'ResponseCode', 'System Trace Number',
                        'RetrievalReferenceNo', 'Acquirer_name', 'Tran_Status', 'Acquirer_Processor', 'PTSP'
                    ]
                    
                    pos_df = spark.read.csv(local_file_path, header=True, inferSchema=True)
                    
                    # Rename columns if needed
                    if pos_df.columns != header_row:
                        for new, old in zip(header_row, pos_df.columns):
                            pos_df = pos_df.withColumnRenamed(old, new)
                    
                    # Drop unnecessary columns
                    cols_to_drop = ['Acquirer_name', 'Tran_Status', 'PTSP']
                    for column in cols_to_drop:
                        if column in pos_df.columns:
                            pos_df = pos_df.drop(column)
                    
                    deduped_df = pos_df.dropDuplicates(["RetrievalReferenceNo"])
                    
                    #write to local path
                    deduped_df.coalesce(1).write.csv(write_path, header=True, mode="overwrite")
                    print("Successfully saved file to local processed folder as CSV")
                    
                except Exception as e:
                    print(f"Error processing CSV file {file}: {str(e)}")
                     
        
'''''
        try:
            # Upload to SharePoint
            
            if 'RCA' in file:
                
                target_folder = ctx.web.get_folder_by_server_relative_url(sharepoint_rca_path)
                with open(local_file_path, 'rb') as f:
                    content = f.read()
                    target_file = target_folder.upload_file(file, content).execute_query()
           
            else:
                for filename in os.listdir(write_path):
                    if (filename.startswith("part-") and filename.endswith(".csv")) or (filename.startswith("part-") and filename.endswith(".xlsx")):
                        
                        name, ext = os.path.splitext(filename)
                        new_filename = f"{os.path.splitext(file)[0]}{ext}"
                        spark_part_file_path = os.path.join(write_path, filename)
                        new_file_path = os.path.join(write_path, new_filename)
                        os.rename(spark_part_file_path, new_file_path)  # Rename in-place
                    
                        # Upload to final directory
                        target_folder = ctx.web.get_folder_by_server_relative_url(sharepoint_process_path)
                        with open(new_file_path, 'rb') as f:
                            content = f.read()
                            target_file = target_folder.upload_file(new_filename, content).execute_query()
                        
                        print(f"{file} uploaded successfully")

        except Exception as e:
            print(f"Upload error for {file}: {str(e)}")
            '''

def generate_table_name_from_date(date: datetime):
        """Returns the table name in the format transactions_YYYY_MM. This will be the table name in the database"""
        return f"transactions_{date.year}_{date.month:02d}"   

def clean_files():
    if len(os.listdir(local_download_path)) < 1:
        print('No files to remove before processing')
        return
    else:
        for filename in os.listdir(local_download_path):
            file_to_del = os.path.join(local_download_path, filename)

            try:
                os.remove(file_to_del)
                print(f"{filename} removed successfully from local download path.")
            except FileNotFoundError:
                print(f"File '{filename}' not found.")
            except Exception as e:
                print(f"An error occurred while deleting the file: {e} from local download path")
    if len(os.listdir(write_path)) < 1:
        print('No files to remove')
        return
    else:
        for filename in os.listdir(write_path):
            file_to_del = os.path.join(write_path,filename)

            try:
                os.remove(file_to_del)
                print(f"{filename} removed successfully. from write path")
            except FileNotFoundError:
                print(f"File '{filename}' not found.")
            except Exception as e:
                print(f"An error occurred while deleting the file: {e} write path")


def main():
    clean_files() #clear local directory before starting pipeline
    retrieve_nibss_file() 
    load_to_sharepoint()

if __name__ == '__main__':
    main()