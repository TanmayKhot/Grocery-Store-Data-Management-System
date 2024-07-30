---

# Grocery Store Management System: A Data Engineering Project

This project demonstrates the application of data engineering principles to handle large-scale data processing, storage, and analysis in a retail context, providing insights into customer behavior and store operations.

## Project Overview

- **Technologies**: Python, MySQL, AWS, PySpark
- **Data**: Synthetically generated to simulate real-world grocery store operations
  - Customer transactions (scalable to 500,000+ rows)
  - Store and employee information
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
│   └── qa/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── prod/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── sql_scripts/
│   │    └── table_scripts.sql
├── src/
│   ├── main/
│   │    ├── __init__.py
│   │    └── delete/
│   │    │      ├── aws_delete.py
│   │    │      ├── database_delete.py
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
│   ├── test/
│   │    ├── generate_csv_data.py
│   │    ├── extra_column_csv_generated_data.py
│   │    ├── generate_customer_table_data.py
│   │    ├── generate_datewise_sales_data.py
│   │    ├── less_column_csv_generated_data.py
│   │    └── sales_data_upload_s3.py
```
