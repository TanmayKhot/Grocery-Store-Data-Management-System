import os.path

from resources.dev import config
from src.main.download.aws_file_download import S3FileDownloader
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session

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
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    query = f"""
            SELECT DISTINCT file_name
            FROM {config.database}.{config.product_staging_table}
            WHERE file_name in ({str(total_csv_files)[1:-1]}) AND status='I' -- check for inactive files
            """

    logger.info(f"dynamically query created: {query}")
    cursor.execute(query)
    data = cursor.fetchall()
    if data:
        logger.error("Last run was failed. Please check again.")
    else:
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

logger.info("****************Listing the Files***************")
logger.info("List of csv files to be processed %s", csv_files)

logger.info("****************Creating the Spark session*********")

spark = spark_session()

logger.info("***************** spark session created ***************")


















