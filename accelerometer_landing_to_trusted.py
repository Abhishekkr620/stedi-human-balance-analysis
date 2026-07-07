import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1783404270992 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1783404270992")

# Script generated for node Customer Trusted
CustomerTrusted_node1783404274736 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1783404274736")

# Script generated for node SQL Query
SqlQuery3036 = '''
select distinct accelerometer_landing.* from accelerometer_landing 
INNER JOIN customer_trusted 
ON accelerometer_landing.user = customer_trusted.email;
'''
SQLQuery_node1783404282602 = sparkSqlQuery(glueContext, query = SqlQuery3036, mapping = {"accelerometer_landing":AccelerometerLanding_node1783404270992, "customer_trusted":CustomerTrusted_node1783404274736}, transformation_ctx = "SQLQuery_node1783404282602")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783404282602, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783403547947", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783404287945 = glueContext.getSink(path="s3://stedi-bucket-ak/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783404287945")
AmazonS3_node1783404287945.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1783404287945.setFormat("glueparquet", compression="snappy")
AmazonS3_node1783404287945.writeFrame(SQLQuery_node1783404282602)
job.commit()