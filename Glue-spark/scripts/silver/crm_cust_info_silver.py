import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, col, row_number
from pyspark.sql.window import Window

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue contexts
spark = SparkSession.builder.appName("SilverLayerLoading").getOrCreate()
glueContext = GlueContext(spark)
logger = glueContext.get_logger()

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Logging for tracking
logger.info("================================================")
logger.info("Loading Silver Layer")
logger.info("================================================")

logger.info("------------------------------------------------")
logger.info("Loading CRM Tables")
logger.info("------------------------------------------------")

# Start the batch loading
batch_start_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Glue Data Catalog
cust_info_bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database="crm_rnd_db", 
    table_name="cust_info_bronze"
).toDF()


# Transformations:
# 1. Remove leading/trailing whitespaces
# 2. Normalize marital status and gender
# 3. Select the most recent record for each customer using ROW_NUMBER window function

window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())

# Apply transformations
cust_info_silver_df = cust_info_bronze_df.withColumn("cst_firstname", trim(col("cst_firstname"))) \
    .withColumn("cst_lastname", trim(col("cst_lastname"))) \
    .withColumn("cst_marital_status", 
                upper(trim(col("cst_marital_status")))
                .when(col("cst_marital_status") == 'S', 'Single')
                .when(col("cst_marital_status") == 'M', 'Married')
                .otherwise('n/a')
    ) \
    .withColumn("cst_gndr", 
                upper(trim(col("cst_gndr")))
                .when(col("cst_gndr") == 'F', 'Female')
                .when(col("cst_gndr") == 'M', 'Male')
                .otherwise('n/a')
    ) \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")  # Keep only the most recent records

cust_info_silver_df = cust_info_silver_df.select(
    "cst_id", 
    "cst_key", 
    "cst_firstname", 
    "cst_lastname", 
    "cst_marital_status", 
    "cst_gndr", 
    "cst_create_date"
)


cust_info_silver= DynamicFrame.fromDF(cust_info_silver_df, glueContext, "silver_cust_info")


EvaluateDataQuality().process_rows(
    frame=cust_info_silver, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)


AmazonS3_node_01= glueContext.getSink(
    path="s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/crm/crm_cust_info/silver/", 
    connection_type="s3", 
    updateBehavior="LOG", 
    partitionKeys=[], 
    enableUpdateCatalog=True
)

AmazonS3_node_01.setCatalogInfo(catalogDatabase="crm_rnd_db",catalogTableName="cust_info_silver")
AmazonS3_node_01.setFormat("glueparquet", compression="snappy")
AmazonS3_node_01.writeFrame(cust_info_silver)

# End the batch loading
batch_end_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]
load_duration = (batch_end_time - batch_start_time).seconds

logger.info(f">> Load Duration: {load_duration} seconds")

# End the Glue job
job.commit()