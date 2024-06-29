import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1719693130988 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1719693130988")

# Script generated for node CustomerLanding
CustomerLanding_node1719694005604 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1719694005604")

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1719693700698 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="acceloremeter_landing", transformation_ctx="AccelerometerLanding_node1719693700698")

# Script generated for node SQL Query
SqlQuery2722 = '''
WITH customer_trusted AS (
    SELECT *
    FROM customer_landing
    WHERE shareWithResearchAsOfDate != 0
),
customer_curated AS (
    SELECT * 
    FROM customer_trusted 
    WHERE email IN (
        SELECT DISTINCT user 
        FROM accelerometer_landing)
)

SELECT *
FROM step_trainer_landing
WHERE serialnumber IN (
    SELECT DISTINCT serialnumber
    FROM customer_curated)
'''
SQLQuery_node1719693778960 = sparkSqlQuery(glueContext, query = SqlQuery2722, mapping = {"accelerometer_landing":AccelerometerLanding_node1719693700698, "step_trainer_landing":StepTrainerLanding_node1719693130988, "customer_landing":CustomerLanding_node1719694005604}, transformation_ctx = "SQLQuery_node1719693778960")

# Script generated for node StediTrusted
StediTrusted_node1719695431779 = glueContext.getSink(path="s3://stedi-data/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="StediTrusted_node1719695431779")
StediTrusted_node1719695431779.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="step_trainer_trusted")
StediTrusted_node1719695431779.setFormat("json")
StediTrusted_node1719695431779.writeFrame(SQLQuery_node1719693778960)
job.commit()