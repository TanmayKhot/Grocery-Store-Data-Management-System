import datetime
import os.path
import shutil

from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

from resources.dev import config
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import dimension_tables_join
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

################### Get S3 client ##################
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# Now you can use S3_client for your S3 operations
response = s3_client.list_buckets()
print(response)
logger.info("List of buckets: %s", response['Buckets'])

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
logger.info("Establishing connection with MySQL Server")
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    query = f"""
            SELECT DISTINCT file_name
            FROM {config.database}.{config.product_staging_table}
            WHERE file_name in ({str(total_csv_files)[1:-1]}) AND status='A' -- check for active files
            """
    # Inactive files == files which were processed fully and completed
    # Active files == files which are being processed/could not be processed completely due to any error

    logger.info(f"dynamically query created: {query}")
    cursor.execute(query)
    data = cursor.fetchall()
    if data: # We have active files
        logger.error("Last run was failed. Please check again.")
    else: # Once the file is processed successfully, it is marked inactive
        logger.error("No matching records found")

else:
    logger.info("Last run was successful!")

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path=folder_path)
    logger.info("Absolute path for S3 bucket for csv file %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process")

except Exception as e:
    logger.error("Exited with error:- %s", e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]

logging.info("File path available on S3 under %s bucket and folder name is %s", bucket_name, file_paths)
try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

all_files = os.listdir(local_directory)
logger.info(f"List of all files at your local directory after downloading files from S3: {all_files}")

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))

else:
    logger.error("There is no data to process")
    raise Exception("No data to process")

logger.info("****************************** Listing the Files ******************************")
logger.info("List of csv files to be processed %s", csv_files)

logger.info("****************************** Creating the Spark session ******************************")

spark = spark_session()

logger.info("****************************** Spark session created ******************************")

# Schema validation
# Move the incorrect files to error_files
# Union all files with correct schema into one dataframe

logger.info("****************************** Checking Schema for data loaded in S3 ******************************")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header", "true")\
        .load(data).columns

    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"NO missing column for {data}")
        correct_files.append(data)

logger.info(f"****************************** List of correct files ******************************")
for filename in correct_files:
    logger.info(f"{filename}")
logger.info(f"****************************** List of error files ******************************\n{error_files}")
for filename in error_files:
    logger.info(f"{filename}")
logger.info(f"****************************** Moving error data to error directory (if any) ******************************")

# Move the error data
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(config.error_folder_path_local, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_name}' from S3 path to '{destination_path}'")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' does not exist")
else:
    logger.info("****************************** No error files found ******************************")

# Updating the staging table with status of the files - Active or Inactive

logger.info(f"****************************** Updating the product_staging_table to denote start of process ******************************")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        query = f"""
                        INSERT INTO {db_name}.{config.product_staging_table}
                        (file_name, file_location, created_date, status)
                        VALUES ('{file_name}','{file_name}','{formatted_date}', 'A')
                      """
        insert_statements.append(query)

    logger.info(f"Insert statement created for staging table --- {insert_statements}")
    logger.info("****************************** Connecting MySQL server ******************************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("****************************** MySQL server connected successfully")

    for query in insert_statements:
        cursor.execute(query)
        connection.commit()

    cursor.close()
    connection.close()
else:
    logger.error("****************************** There is no files to process ******************************")
    raise Exception("****************************** No data available for correct files ******************************")

logger.info("****************************** Staging table updated successfully ******************************")
logger.info("****************************** Fixing extra column coming from source ******************************")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(),True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

final_df_to_process = spark.createDataFrame([], schema=schema)
for data in correct_files:
    data_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present at source is {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        logger.info(f"processed {data} and added 'additional_column")
    else:
        data_df = data_df.withColumn("additional_column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")

    final_df_to_process = final_df_to_process.union(data_df)

logger.info("****************************** Final Dataframe from source to be processed ******************************")
final_df_to_process.show()

# Connecting with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)

# Create df for all tables

# customer table
logger.info("****************************** Loading customer table into customer_table_df ******************************")
customer_table_df = database_client.create_dataframe(spark, config.customer_table)

# product table
logger.info("****************************** Loading product table into product_table_df ******************************")
product_table_df = database_client.create_dataframe(spark, config.product_table)

# product_staging_table table
logger.info("****************************** Loading staging table into product_staging_table_df ******************************")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# sales_team table
logger.info("****************************** Loading sales team table into sales_team_table_df ******************************")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# store table
logger.info("****************************** Loading store table into store_table_df ******************************")
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimension_tables_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)

logger.info("****************************** Final enriched data ******************************")
s3_customer_store_sales_df_join.show()
logger.info(f"Final columns : {s3_customer_store_sales_df_join.columns}")
# Write the customer data into customer data mart in parquet format
# File will be written to local first
# Move the raw data to S3 bucket for reporting tool
# Write reporting data into MySQL table as well

logger.info("****************************** Write the data into Customer Data Mart ******************************")

final_customer_data_mart_df = s3_customer_store_sales_df_join \
        .select("ct.customer_id",
                "ct.first_name",
                "ct.last_name",
                "ct.address",
                "ct.pincode",
                "phone_number",
                "sales_date",
                "total_cost")

logger.info("****************************** Final data for customer data mart ******************************")
final_customer_data_mart_df.show()
parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

logger.info(f"****************************** Customer data written to local disk at {config.customer_data_mart_local_file} ******************************")

logger.info("****************************** Moving customer data from local to S3 ******************************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

logger.info("****************************** Write the data into sales team data mart ******************************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
        .select("store_id",
                "sales_person_id","sales_person_first_name", "sales_person_last_name",
                "store_manager_name", "manager_id", "is_manager",
                "sales_person_address", "sales_person_pincode",
                "sales_date", "total_cost", expr("SUBSTRING(sales_date,1,7) as sales_month"))

logger.info("****************************** Final data for customer data mart ******************************")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)

logger.info("****************************** Moving customer data from local to S3 ******************************")
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# Also writing the data in partitions since data files could be huge, hence partitioning by month and sotre_id
final_sales_team_data_mart_df.write.format("parquet") \
        .option("header", "true")\
        .mode("overwrite")\
        .partitionBy("sales_month", "store_id")\
        .option("path", config.sales_team_data_mart_partitioned_local_file)\
        .save()

# Move partitioned data to S3
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000

for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path,
                                             config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)





