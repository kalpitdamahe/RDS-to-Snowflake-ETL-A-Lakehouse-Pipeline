import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F


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
prd_info_bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database="crm_rnd_db", 
    table_name="prd_info_bronze"
).toDF()

# Define the window specification for lead function
window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")

prd_info_silver_df = prd_info_bronze_df.withColumn(
        "cat_id", F.regexp_replace(F.substring("prd_key", 1, 5), "-", "_")  # Extract category ID
    ).withColumn(
        "prd_key", F.substring("prd_key", 7, F.length("prd_key"))  # Extract product key
    ).withColumn(
        "prd_cost", F.coalesce("prd_cost", F.lit(0))  # Handle null product cost by replacing with 0
    ).withColumn(
        "prd_line", F.when(F.upper(F.trim("prd_line")) == 'M', 'Mountain')
                    .when(F.upper(F.trim("prd_line")) == 'R', 'Road')
                    .when(F.upper(F.trim("prd_line")) == 'S', 'Other Sales')
                    .when(F.upper(F.trim("prd_line")) == 'T', 'Touring')
                    .otherwise('n/a')  # Map product line codes to descriptive values
    ).withColumn(
        "prd_start_dt", F.col("prd_start_dt").cast("date")  # Cast product start date to date type
    ).withColumn(
        "prd_end_dt", F.lead("prd_start_dt").over(window_spec) - F.expr("INTERVAL 1 DAY")  # Calculate end date
    )

prd_info_silver_df = prd_info_silver_df.select(
    "prd_id",
    "cat_id",
    "prd_key",
    "prd_nm",
    "prd_cost",
    "prd_line",
    "prd_start_dt",
    "prd_end_dt"
)


prd_info_silver= DynamicFrame.fromDF(prd_info_silver_df, glueContext, "silver_prd_info")


EvaluateDataQuality().process_rows(
    frame=prd_info_silver, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)


AmazonS3_node_01= glueContext.getSink(
    path="s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/crm/crm_prd_info/silver/", 
    connection_type="s3", 
    updateBehavior="LOG", 
    partitionKeys=[], 
    enableUpdateCatalog=True
)

AmazonS3_node_01.setCatalogInfo(catalogDatabase="crm_rnd_db",catalogTableName="prd_info_silver")
AmazonS3_node_01.setFormat("glueparquet", compression="snappy")
AmazonS3_node_01.writeFrame(prd_info_silver)

# End the batch loading
batch_end_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]
load_duration = (batch_end_time - batch_start_time).seconds

logger.info(f">> Load Duration: {load_duration} seconds")

# End the Glue job
job.commit()