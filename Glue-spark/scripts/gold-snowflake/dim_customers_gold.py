import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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



cust_info_silver_df = glueContext.create_dynamic_frame.from_catalog(database="crm_rnd_db", table_name="cust_info_silver").toDF()

cust_az12_silver_df = glueContext.create_dynamic_frame.from_catalog(database="erp_rnd_db", table_name="cust_az12_silver").toDF()

loc_a101_silver_df = glueContext.create_dynamic_frame.from_catalog(database="erp_rnd_db", table_name="loc_a101_silver").toDF()


# Step 1: Perform the LEFT JOINs
joined_df = cust_info_silver_df.alias('ci') \
    .join(cust_az12_silver_df.alias('ca'), F.col('ci.cst_key') == F.col('ca.cid'), 'left') \
    .join(loc_a101_silver_df.alias('la'), F.col('ci.cst_key') == F.col('la.cid'), 'left')

# Step 2: Define the window specification for ROW_NUMBER() (without partition, ordered by 'cst_id')
window_spec = Window.orderBy(F.col('ci.cst_id'))

# Step 3: Apply transformations and selections
cust_gold_df = joined_df.withColumn(
    'customer_key', F.row_number().over(window_spec)  # ROW_NUMBER() OVER (ORDER BY cst_id)
).select(
    F.col('ci.cst_id').alias('customer_id'),        # AS customer_id
    F.col('ci.cst_key').alias('customer_number'),   # AS customer_number
    F.col('ci.cst_firstname').alias('first_name'),  # AS first_name
    F.col('ci.cst_lastname').alias('last_name'),    # AS last_name
    F.col('la.cntry').alias('country'),             # AS country
    F.col('ci.cst_marital_status').alias('marital_status'),  # AS marital_status
    F.when(F.col('ci.cst_gndr') != 'n/a', F.col('ci.cst_gndr'))  # IF cst_gndr != 'n/a', use cst_gndr
    .otherwise(F.coalesce(F.col('ca.gen'), F.lit('n/a'))).alias('gender'),  # ELSE use ERP data gen, fallback to 'n/a'
    F.col('ca.bdate').alias('birthdate'),           # AS birthdate
    F.col('ci.cst_create_date').alias('create_date')  # AS create_date
)

cust_gold = DynamicFrame.fromDF(cust_gold_df, glueContext, "gold_cust_df")

glueContext.write_dynamic_frame.from_options(
        frame=cust_gold, 
        connection_type="s3", 
        format="glueparquet", 
        connection_options={"path": "s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/gold/customers-data/", "partitionKeys": []}, 
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
cust_gold_df.write.format("snowflake").options(**sfoptions).options('dbtable', 'customer_sch.dim_customers').mode('overwrite').save()

# Commit the job
job.commit()