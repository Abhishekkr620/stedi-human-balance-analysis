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

# Script generated for node Customer Landing
CustomerLanding_node1783402653250 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1783402653250")

# Script generated for node SQL Query
SqlQuery2960 = '''
select * from customer_landing 
where shareWithResearchAsOfDate is not null

'''
SQLQuery_node1783402685146 = sparkSqlQuery(glueContext, query = SqlQuery2960, mapping = {"customer_landing":CustomerLanding_node1783402653250}, transformation_ctx = "SQLQuery_node1783402685146")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783402685146, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783402626662", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1783402695482 = glueContext.getSink(path="s3://stedi-bucket-ak/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1783402695482")
CustomerTrusted_node1783402695482.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1783402695482.setFormat("glueparquet", compression="snappy")
CustomerTrusted_node1783402695482.writeFrame(SQLQuery_node1783402685146)
job.commit()