import sys
from pyspark.sql import SparkSession
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
sales_details_bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database="crm_rnd_db", 
    table_name="sales_details_bronze"
).toDF()

sales_details_silver_df = sales_details_bronze_df.withColumn(
        "sls_order_dt",
        F.when((F.col("sls_order_dt") == 0) | (F.length("sls_order_dt") != 8), None)
         .otherwise(F.to_date(F.col("sls_order_dt").cast("string"), "yyyyMMdd"))
    )\
    .withColumn(
        "sls_ship_dt",
        F.when((F.col("sls_ship_dt") == 0) | (F.length("sls_ship_dt") != 8), None)
         .otherwise(F.to_date(F.col("sls_ship_dt").cast("string"), "yyyyMMdd"))
    )\
    .withColumn(
        "sls_due_dt",
        F.when((F.col("sls_due_dt") == 0) | (F.length("sls_due_dt") != 8), None)
         .otherwise(F.to_date(F.col("sls_due_dt").cast("string"), "yyyyMMdd"))
    )\
    .withColumn(
        "sls_sales",
        F.when(
            (F.col("sls_sales").isNull()) | (F.col("sls_sales") <= 0) | 
            (F.col("sls_sales") != F.col("sls_quantity") * F.abs(F.col("sls_price"))),
            F.col("sls_quantity") * F.abs(F.col("sls_price"))
        ).otherwise(F.col("sls_sales"))
    )\
    .withColumn(
        "sls_price",
        F.when(
            (F.col("sls_price").isNull()) | (F.col("sls_price") <= 0),
            F.col("sls_sales") / F.when(F.col("sls_quantity") != 0, F.col("sls_quantity")).otherwise(1)
        ).otherwise(F.col("sls_price"))
    )

sales_details_silver_df = sales_details_silver_df.select(
    "sls_ord_num",
    "sls_prd_key",
    "sls_cust_id",
    "sls_order_dt",
    "sls_ship_dt",
    "sls_due_dt",
    "sls_sales",
    "sls_quantity",
    "sls_price"
)

sales_details_silver = DynamicFrame.fromDF(sales_details_silver_df, glueContext, "silver_sales_details")


EvaluateDataQuality().process_rows(
    frame=sales_details_silver, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)


AmazonS3_node_01= glueContext.getSink(
    path="s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/crm/crm_sales_details/silver/", 
    connection_type="s3", 
    updateBehavior="LOG", 
    partitionKeys=[], 
    enableUpdateCatalog=True
)

AmazonS3_node_01.setCatalogInfo(catalogDatabase="crm_rnd_db",catalogTableName="sales_details_silver")
AmazonS3_node_01.setFormat("glueparquet", compression="snappy")
AmazonS3_node_01.writeFrame(sales_details_silver)

# End the batch loading
batch_end_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]
load_duration = (batch_end_time - batch_start_time).seconds

logger.info(f">> Load Duration: {load_duration} seconds")

# End the Glue job
job.commit()