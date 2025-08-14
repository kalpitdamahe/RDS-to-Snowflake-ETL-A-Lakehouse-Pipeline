import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Glue Data Catalog _erp_cust_az12
erp_cust_az12 = glueContext.create_dynamic_frame.from_catalog(
    database="de-erpdb", 
    table_name="erpdb_erpsch_erp_cust_az12"
)


EvaluateDataQuality().process_rows(
    frame=erp_cust_az12, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

AmazonS3_node1= glueContext.getSink(
    path="s3://aws-de-rnd-ap-south-1/rnd-data-lake/data/erp/erp_cust_az12/bronze/", 
    connection_type="s3", 
    updateBehavior="LOG", 
    partitionKeys=[], 
    enableUpdateCatalog=True
)

AmazonS3_node1.setCatalogInfo(catalogDatabase="erp_rnd_db",catalogTableName="cust_az12_bronze")
AmazonS3_node1.setFormat("glueparquet", compression="snappy")
AmazonS3_node1.writeFrame(erp_cust_az12)
job.commit()