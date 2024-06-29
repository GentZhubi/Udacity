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
StepTrainerLanding_node1719696111790 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1719696111790")

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1719696091476 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="acceloremeter_landing", transformation_ctx="AccelerometerLanding_node1719696091476")

# Script generated for node CustomerLanding
CustomerLanding_node1719696065349 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1719696065349")

# Script generated for node SQL Query
SqlQuery2644 = '''
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
),
accelerometer_trusted AS (
    SELECT *
    FROM accelerometer_landing
    JOIN customer_trusted 
    ON accelerometer_landing.user = customer_trusted.email
),
step_trainer_trusted AS (
    SELECT *
    FROM step_trainer_landing
    WHERE serialnumber IN (
        SELECT DISTINCT serialnumber
        FROM customer_curated)
)
SELECT * 
FROM step_trainer_trusted 
JOIN accelerometer_trusted 
ON step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timestamp
'''
SQLQuery_node1719696131992 = sparkSqlQuery(glueContext, query = SqlQuery2644, mapping = {"customer_landing":CustomerLanding_node1719696065349, "accelerometer_landing":AccelerometerLanding_node1719696091476, "step_trainer_landing":StepTrainerLanding_node1719696111790}, transformation_ctx = "SQLQuery_node1719696131992")

# Script generated for node Amazon S3
AmazonS3_node1719697924801 = glueContext.getSink(path="s3://stedi-data/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719697924801")
AmazonS3_node1719697924801.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="machine_learning_curated")
AmazonS3_node1719697924801.setFormat("json")
AmazonS3_node1719697924801.writeFrame(SQLQuery_node1719696131992)
job.commit()