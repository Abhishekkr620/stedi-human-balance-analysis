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
AccelerometerTrusted_node1783416482138 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1783416482138")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1783416482952 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1783416482952")

# Script generated for node SQL Query
SqlQuery2923 = '''
select distinct accelerometer_trusted.user,step_trainer_trusted.*,
accelerometer_trusted.x,accelerometer_trusted.y,
accelerometer_trusted.z from accelerometer_trusted
INNER JOIN step_trainer_trusted on
accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
'''
SQLQuery_node1783416485597 = sparkSqlQuery(glueContext, query = SqlQuery2923, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1783416482952, "accelerometer_trusted":AccelerometerTrusted_node1783416482138}, transformation_ctx = "SQLQuery_node1783416485597")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783416485597, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783415562036", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1783416489032 = glueContext.getSink(path="s3://stedi-bucket-ak/machine_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1783416489032")
MachineLearningCurated_node1783416489032.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1783416489032.setFormat("glueparquet", compression="snappy")
MachineLearningCurated_node1783416489032.writeFrame(SQLQuery_node1783416485597)
job.commit()