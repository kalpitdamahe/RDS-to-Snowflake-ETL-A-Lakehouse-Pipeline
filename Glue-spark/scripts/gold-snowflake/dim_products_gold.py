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



prd_info_silver_df = glueContext.create_dynamic_frame.from_catalog(database="crm_rnd_db", table_name="prd_info_silver").toDF()

px_cat_g1v2_silver_df = glueContext.create_dynamic_frame.from_catalog(database="erp_rnd_db", table_name="px_cat_g1v2_silver").toDF()

# Step 1: Define the LEFT JOIN between prd_info_silver_df and px_cat_g1v2_silver_df
joined_df = prd_info_silver_df.alias("pn") \
    .join(px_cat_g1v2_silver_df.alias("pc"), F.col("pn.cat_id") == F.col("pc.id"), "left")

# Step 2: Define the window specification for ROW_NUMBER() (ordered by 'prd_start_dt' and 'prd_key')
window_spec = Window.orderBy(F.col("pn.prd_start_dt"), F.col("pn.prd_key"))

# Step 3: Apply transformations and selections
prd_gold_df = joined_df.withColumn(
    "product_key", F.row_number().over(window_spec)  # ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key)
).select(
    F.col("pn.prd_id").alias("product_id"),          # AS product_id
    F.col("pn.prd_key").alias("product_number"),     # AS product_number
    F.col("pn.prd_nm").alias("product_name"),        # AS product_name
    F.col("pn.cat_id").alias("category_id"),         # AS category_id
    F.col("pc.cat").alias("category"),               # AS category
    F.col("pc.subcat").alias("subcategory"),         # AS subcategory
    F.col("pc.maintenance").alias("maintenance"),    # AS maintenance
    F.col("pn.prd_cost").alias("cost"),              # AS cost
    F.col("pn.prd_line").alias("product_line"),      # AS product_line
    F.col("pn.prd_start_dt").alias("start_date")     # AS start_date
).filter(
    F.col("pn.prd_end_dt").isNull()                  # Filter out all historical data (prd_end_dt IS NULL)
)

prd_gold = DynamicFrame.fromDF(prd_gold_df, glueContext, "gold_prd_df")

glueContext.write_dynamic_frame.from_options(
        frame=prd_gold, 
        connection_type="s3", 
        format="glueparquet", 
        connection_options={"path": "s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/gold/product-data/", "partitionKeys": []}, 
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
prd_gold_df.write.format("snowflake").options(**sfoptions).options('dbtable', 'product_sch.dim_products').mode('overwrite').save()

# Commit the job
job.commit()