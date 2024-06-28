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

# Script generated for node CustomerLanding
CustomerLanding_node1719616736301 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1719616736301")

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1719616767569 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="acceloremeter_landing", transformation_ctx="AccelerometerLanding_node1719616767569")

# Script generated for node SQL Query
SqlQuery2592 = '''
WITH customer_trusted_tmp AS (
    SELECT *
    FROM customer_landing
    WHERE shareWithResearchAsOfDate != 0
)
SELECT *
FROM accelerometer_landing
JOIN customer_trusted_tmp
ON accelerometer_landing.user = customer_trusted_tmp.email

'''
SQLQuery_node1719616789375 = sparkSqlQuery(glueContext, query = SqlQuery2592, mapping = {"accelerometer_landing":AccelerometerLanding_node1719616767569, "customer_landing":CustomerLanding_node1719616736301}, transformation_ctx = "SQLQuery_node1719616789375")

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1719616854089 = glueContext.getSink(path="s3://stedi-data/acceloremeter/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1719616854089")
AccelerometerTrusted_node1719616854089.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="acceloremeter_trusted")
AccelerometerTrusted_node1719616854089.setFormat("json")
AccelerometerTrusted_node1719616854089.writeFrame(SQLQuery_node1719616789375)
job.commit()