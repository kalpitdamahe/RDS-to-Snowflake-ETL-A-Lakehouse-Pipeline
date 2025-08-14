import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime


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
cust_az12_bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database="erp_rnd_db", 
    table_name="cust_az12_bronze"
).toDF()


cust_az12_silver_df = (cust_az12_bronze_df
    # Handle 'cid' transformation
    .withColumn(
        "cid",
        F.when(F.col("cid").like("NAS%"), F.expr("substring(cid, 4, length(cid))"))
         .otherwise(F.col("cid"))
    )
    # Handle 'bdate' transformation
    .withColumn(
        "bdate",
        F.when(F.col("bdate") > F.lit(datetime.datetime.now()), None)
         .otherwise(F.col("bdate"))
    )
    # Normalize 'gen' (gender) values
    .withColumn(
        "gen",
        F.when(F.upper(F.trim(F.col("gen"))).isin("F", "FEMALE"), "Female")
         .when(F.upper(F.trim(F.col("gen"))).isin("M", "MALE"), "Male")
         .otherwise("n/a")
    )
)

cust_az12_silver_df = cust_az12_silver_df.select(
    "cid",
    "bdate",
    "gen"
)

cust_az12_silver= DynamicFrame.fromDF(cust_az12_silver_df, glueContext, "silver_cust_az12")


# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=cust_az12_silver, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)


AmazonS3_node_01= glueContext.getSink(
    path="s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/erp/erp_cust_az12/silver/", 
    connection_type="s3", 
    updateBehavior="LOG", 
    partitionKeys=[], 
    enableUpdateCatalog=True
)

AmazonS3_node_01.setCatalogInfo(catalogDatabase="erp_rnd_db",catalogTableName="cust_az12_silver")
AmazonS3_node_01.setFormat("glueparquet", compression="snappy")
AmazonS3_node_01.writeFrame(cust_az12_silver)

# End the batch loading
batch_end_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]
load_duration = (batch_end_time - batch_start_time).seconds

logger.info(f">> Load Duration: {load_duration} seconds")

# End the Glue job
job.commit()