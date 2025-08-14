import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
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
logger.info("Loading ERP Tables")
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
loc_a101_bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database="erp_rnd_db", 
    table_name="loc_a101_bronze"
).toDF()

# Step 1: Remove hyphens from 'cid' using F.regexp_replace
# Step 2: Apply a CASE-like conditional logic using F.when for the 'cntry' column

loc_a101_silver_df = loc_a101_bronze_df.withColumn(
        'cid', 
        F.regexp_replace(F.col('cid'), '-', '')  # Remove hyphens from 'cid'
    ) \
    .withColumn(
        'cntry',
        F.when(F.trim(F.col('cntry')) == 'DE', 'Germany')  # If 'DE', then 'Germany'
         .when(F.trim(F.col('cntry')).isin('US', 'USA'), 'United States')  # If 'US' or 'USA', then 'United States'
         .when(F.trim(F.col('cntry')) == '', 'n/a')  # If empty string, then 'n/a'
         .when(F.col('cntry').isNull(), 'n/a')  # If NULL, then 'n/a'
         .otherwise(F.trim(F.col('cntry')))  # Otherwise, trim the country code
    )

loc_a101_silver_df = loc_a101_silver_df.select(
    "cid",
    "cntry"
)

loc_a101_silver = DynamicFrame.fromDF(loc_a101_silver_df , glueContext, "silver_loc_a101")



EvaluateDataQuality().process_rows(
    frame=loc_a101_silver, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)


AmazonS3_node_01= glueContext.getSink(
    path="s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/erp/erp_loc_a101/silver/", 
    connection_type="s3", 
    updateBehavior="LOG", 
    partitionKeys=[], 
    enableUpdateCatalog=True
)

AmazonS3_node_01.setCatalogInfo(catalogDatabase="erp_rnd_db",catalogTableName="loc_a101_silver")
AmazonS3_node_01.setFormat("glueparquet", compression="snappy")
AmazonS3_node_01.writeFrame(loc_a101_silver)

# End the batch loading
batch_end_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]
load_duration = (batch_end_time - batch_start_time).seconds

logger.info(f">> Load Duration: {load_duration} seconds")

# End the Glue job
job.commit()