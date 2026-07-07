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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1783405579431 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1783405579431")

# Script generated for node Customer Trusted
CustomerTrusted_node1783405580588 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1783405580588")

# Script generated for node SQL Query Filter
SqlQuery2930 = '''
select distinct customer_trusted.* from customer_trusted
INNER JOIN accelerometer_trusted
ON customer_trusted.email = accelerometer_trusted.user
'''
SQLQueryFilter_node1783405583927 = sparkSqlQuery(glueContext, query = SqlQuery2930, mapping = {"customer_trusted":CustomerTrusted_node1783405580588, "accelerometer_trusted":AccelerometerTrusted_node1783405579431}, transformation_ctx = "SQLQueryFilter_node1783405583927")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQueryFilter_node1783405583927, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783405076205", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1783405587500 = glueContext.getSink(path="s3://stedi-bucket-ak/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1783405587500")
CustomerCurated_node1783405587500.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1783405587500.setFormat("glueparquet", compression="snappy")
CustomerCurated_node1783405587500.writeFrame(SQLQueryFilter_node1783405583927)
job.commit()