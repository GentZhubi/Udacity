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

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1719692254738 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="acceloremeter_landing", transformation_ctx="AccelerometerLanding_node1719692254738")

# Script generated for node CustomerLanding
CustomerLanding_node1719690956136 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1719690956136")

# Script generated for node SQL Query
SqlQuery2423 = '''
WITH customer_trusted AS (
    SELECT *
    FROM customer_landing
    WHERE shareWithResearchAsOfDate != 0
)
SELECT * 
FROM customer_trusted 
WHERE email IN (
    SELECT DISTINCT user 
    FROM accelerometer_landing
)
'''
SQLQuery_node1719691014253 = sparkSqlQuery(glueContext, query = SqlQuery2423, mapping = {"customer_landing":CustomerLanding_node1719690956136, "accelerometer_landing":AccelerometerLanding_node1719692254738}, transformation_ctx = "SQLQuery_node1719691014253")

# Script generated for node CustomersCurated
CustomersCurated_node1719691632757 = glueContext.getSink(path="s3://stedi-data/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomersCurated_node1719691632757")
CustomersCurated_node1719691632757.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customers_curated")
CustomersCurated_node1719691632757.setFormat("json")
CustomersCurated_node1719691632757.writeFrame(SQLQuery_node1719691014253)
job.commit()