from pyspark.sql.functions import *
from src.main.utility.logging_config import *
#enriching the data from different table
def dimension_tables_join(final_df_to_process,
                         customer_table_df,store_table_df,sales_team_table_df):

    #step 1: Checking the resulting schema of joining final_df with customer_data
    # final_df_to_process.alias("s3_data") \
    #     .join(customer_table_df.alias("ct"),
    #           col("s3_data.customer_id") == col("ct.customer_id"),"inner") \
    #     .show()

    #But i do not need all the columns so dropping it
    #save the result into s3_customer_df_join

    s3_customer_df_join = final_df_to_process.alias("s3_data") \
        .join(customer_table_df.alias("ct"),
              col("s3_data.customer_id") == col("ct.customer_id"),"inner") \
        .drop("product_name","price","quantity","additional_column",
              "s3_data.customer_id","customer_joining_date")

    s3_customer_df_join.printSchema()

    #step 2: Checking the resulting schema of adding store df
    # s3_customer_df_join.join(store_table_df,
    #                          store_table_df["id"]==s3_customer_df_join["store_id"],
    #                          "inner").show()

    #But i do not need all the columns so dropping it
    #save the result into s3_customer_store_df_join

    s3_customer_store_df_join= s3_customer_df_join.join(store_table_df,
                             store_table_df["id"]==s3_customer_df_join["store_id"],
                             "inner")\
                        .drop("id","store_pincode","store_opening_date","reviews")
    s3_customer_df_join.printSchema()

    #step 3 where i am adding sales team table details
    # s3_customer_store_df_join.join(sales_team_table_df,
    #                          sales_team_table_df["id"]==s3_customer_store_df_join["sales_person_id"],
    #                          "inner").show()


    #But i do not need all the columns so dropping it
    #save the result into s3_customer_store_sales_df_join
    s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
                             col("st.id")==s3_customer_store_df_join["sales_person_id"],
                             "inner")\
                .withColumn("sales_person_first_name",col("st.first_name"))\
                .withColumn("sales_person_last_name",col("st.last_name"))\
                .withColumn("sales_person_address",col("st.address"))\
                .withColumn("sales_person_pincode",col("st.pincode"))\
                .drop("id","st.first_name","st.last_name","st.address","st.pincode")

    logger.info("Final schema after all joins: ")
    s3_customer_df_join.printSchema()

    return s3_customer_store_sales_df_join

