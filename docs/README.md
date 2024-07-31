---

# Grocery Store Data Management System: A Data Engineering Project

This project demonstrates the application of data engineering principles to handle large-scale data processing, storage, and analysis in a retail context, providing insights into customer behavior and store operations. The main outcome is 
- To analyze customer's monthly spending patterns to tailor personalized offers, enhancing customer retention through targeted discounts and promotions.
- To identify top-performing sales employees and calculate their monthly incentives based on highest sales achievements.

## Project Overview

- **Technologies**: Python, MySQL, AWS, PySpark
- **Data**: Synthetically generated to simulate real-world grocery store operations
  - Customer transactions (scalable to 500,000+ rows)
  - Store information
  - Products information
  - Employee information
- **Key Features**:
  - Spark transformations and optimizations
  - Efficient data partitioning and storage
  - Robust data modeling
  - File status management (active/inactive)
  - Automated file cleanup post-processing
  - Schema validation
  - Secure AWS credential handling (encryption/decryption)
  - Modular functions with detailed logging for easy debugging

## Work flow
![](https://github.com/TanmayKhot/spark-project/blob/main/docs/project_flow.png)

## Data modelling
![](https://github.com/TanmayKhot/spark-project/blob/main/docs/data_diagram.png)
## Project structure
```plaintext
spark_project/
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── sql_scripts/
│   │    └── table_scripts.sql
├── src/
│   ├── main/
│   │    ├── __init__.py
│   │    └── delete/
│   │    │      ├── aws_delete.py
│   │    │      └── local_file_delete.py
│   │    └── download/
│   │    │      └── aws_file_download.py
│   │    └── move/
│   │    │      └── move_files.py
│   │    └── read/
│   │    │      ├── aws_read.py
│   │    │      └── database_read.py
│   │    └── transformations/
│   │    │      └── jobs/
│   │    │      │     ├── customer_mart_sql_transform_write.py
│   │    │      │     ├── dimension_tables_join.py
│   │    │      │     ├── main.py
│   │    │      │     └──sales_mart_sql_transform_write.py
│   │    └── upload/
│   │    │      └── upload_to_s3.py
│   │    └── utility/
│   │    │      ├── encrypt_decrypt.py
│   │    │      ├── logging_config.py
│   │    │      ├── s3_client_object.py
│   │    │      ├── spark_session.py
│   │    │      └── my_sql_session.py
│   │    └── write/
│   │    │      ├── database_write.py
│   │    │      └── parquet_write.py
│   ├── data/
│   │    ├── generate_csv_data.py
│   │    ├── extra_column_csv_generated_data.py
│   │    ├── generate_customer_table_data.py
│   │    ├── generate_datewise_sales_data.py
│   │    ├── less_column_csv_generated_data.py
│   │    └── sales_data_upload_s3.py
```

### File descriptions

| File/Directory | Description |
|----------------|-------------|
| `config.py` | Contains essential variables for database connections, S3 bucket details, MySQL credentials, local directory, etc. |
| `requirement.txt` | Contains required packages and tools to run the project. |
| **sql_scripts/** |
| `table_scripts.sql` | Contains SQL queries to create MySQL tables and insert data in them  |
| **delete/** |
| `aws_delete.py` | Used to delete successfully processed files from AWS S3. |
| `database_delete.py` | Used to delete successfully processed files from MySQL tables. |
| `local_file_delete.py` | Used to delete successfully processed files from local storage. |
| **download/** |
| `aws_file_download.py` | Used to download files from S3. |
| **move/** |
| `move_files.py` | Used to copy data from source to destination in the S3 bucket. |
| **read/** |
| `aws_read.py` | Used to list all files present in the S3 bucket. |
| `database_read.py` | Read data from MySQL table and create a Spark Dataframe. |
| **transformations/jobs/** |
| `customer_mart_sql_transform_write.py` | Analyzes customer's monthly spending patterns to tailor personalized offers, enhancing customer retention through targeted discounts and promotions. |
| `sales_mart_sql_transform_write.py` | Identifies top-performing sales employees and calculates their monthly incentives based on highest sales achievements. |
| `dimension_tables_join.py` | Used to join MySQL tables into a single Spark dataframe. |
| **upload/** |
| `upload_to_s3.py` | Used to upload files to S3 bucket. |
| **utility/** |
| `encrypt_decrypt.py` | Used to encrypt and decrypt AWS secret credentials. |
| `logging_config.py` | Default config for logging messages. |
| `s3_client_object.py` | Get S3 client object. |
| `spark_session.py` | To create a spark session. |
| `my_sql_session.py` | To create MySQL connection. |
| **write/** |
| `database_write.py` | Used to write data into MySQL tables. |
| `parquet_write.py` | Used to store Spark dataframes in Parquet format. |
| **data/** |
| `extra_column_csv_generated_data.py` | Generate a .csv file with custom rows having an extra column than the expected schema. |
| `generate_csv_data.py` | Generate a .csv file with custom data and number of rows for product and customer data. |
| `generate_customer_table_data.py` | Generate fake customer data. |
| `generate_datewise_sales_data.py` | Generate sales data over a time period. |
| `less_column_csv_generated_data.py` | Generate .csv file with lesser columns than expected schema (will be considered as an error file). |
| `sales_data_upload_s3.py` | Upload data to S3 from local storage. |
  
