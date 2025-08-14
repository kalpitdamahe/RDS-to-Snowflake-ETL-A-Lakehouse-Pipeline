import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from utils import get_snowflake_cred
import os

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



sales_details_silver_df = glueContext.create_dynamic_frame.from_catalog(database="crm_rnd_db", table_name="sales_details_silver").toDF()

product_data_gold_df = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"paths": ["s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/gold/product-data/"], "recurse": True}
).toDF()

customers_data_gold_df = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"paths": ["s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/gold/customers-data/"], "recurse": True}
).toDF()


#LEFT JOIN between crm_sales_details and dim_products
joined_df = sales_details_silver_df.alias("sd") \
    .join(product_data_gold_df.alias("pr"), F.col("sd.sls_prd_key") == F.col("pr.product_number"), "left")\
    .join(customers_data_gold_df.alias("cu"), F.col("sd.sls_cust_id") == F.col("cu.customer_id"), "left")


#Selecting the relevant columns
fact_sales_gold_df = joined_df.select(
    F.col("sd.sls_ord_num").alias("order_number"),        # AS order_number
    F.col("pr.product_key").alias("product_key"),         # AS product_key
    F.col("cu.customer_key").alias("customer_key"),       # AS customer_key
    F.col("sd.sls_order_dt").alias("order_date"),         # AS order_date
    F.col("sd.sls_ship_dt").alias("shipping_date"),       # AS shipping_date
    F.col("sd.sls_due_dt").alias("due_date"),             # AS due_date
    F.col("sd.sls_sales").alias("sales_amount"),          # AS sales_amount
    F.col("sd.sls_quantity").alias("quantity"),           # AS quantity
    F.col("sd.sls_price").alias("price")                  # AS price
)


fact_sales_gold = DynamicFrame.fromDF(fact_sales_gold_df, glueContext, "fact_sales_gold_df")

glueContext.write_dynamic_frame.from_options(
        frame=fact_sales_gold, 
        connection_type="s3", 
        format="glueparquet", 
        connection_options={"path": "s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/gold/sales-data/", "partitionKeys": []}, 
        format_options={"compression": "snappy"}
    )


# Fetch credentials from Secrets Manager
secret_name = os.getenv("secret_name")
secrete_region = os.getenv("secrete_region")

SNOW_USER, SNOW_PSW = get_snowflake_cred(secret_name, secrete_region)

# Snowflake connection options read from environment variables
sfoptions = {
    "sfURL": os.getenv("SNOWFLAKE_URL"),              # Snowflake URL 
    "sfUser": SNOW_USER,                              # Snowflake username
    "sfPassword": SNOW_PSW,                           # Snowflake password
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),    # Snowflake database
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),  # Snowflake warehouse
}


# Write to Snowflake table
fact_sales_gold_df.write.format("snowflake").options(**sfoptions).options('dbtable', 'sales_sch.fact_sales').mode('overwrite').save()

# Commit the job
job.commit()