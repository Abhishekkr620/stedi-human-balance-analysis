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

# Script generated for node Customer Curated
CustomerCurated_node1783415596197 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1783415596197")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1783415594940 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1783415594940")

# Script generated for node SQL Query
SqlQuery3013 = '''
select distinct step_trainer_landing.* from step_trainer_landing
INNER JOIN customer_curated on
step_trainer_landing.serialNumber = customer_curated.serialNumber
'''
SQLQuery_node1783415599003 = sparkSqlQuery(glueContext, query = SqlQuery3013, mapping = {"customer_curated":CustomerCurated_node1783415596197, "step_trainer_landing":StepTrainerLanding_node1783415594940}, transformation_ctx = "SQLQuery_node1783415599003")

# Script generated for node Step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783415599003, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783415562036", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Step_trainer_trusted_node1783415601975 = glueContext.getSink(path="s3://stedi-bucket-ak/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Step_trainer_trusted_node1783415601975")
Step_trainer_trusted_node1783415601975.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
Step_trainer_trusted_node1783415601975.setFormat("glueparquet", compression="snappy")
Step_trainer_trusted_node1783415601975.writeFrame(SQLQuery_node1783415599003)
job.commit()