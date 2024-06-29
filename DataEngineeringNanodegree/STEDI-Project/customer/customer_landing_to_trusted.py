import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer-Landing
CustomerLanding_node1719520108348 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1719520108348")

# Script generated for node Landing to Trusted
LandingtoTrusted_node1719520205430 = Filter.apply(frame=CustomerLanding_node1719520108348, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="LandingtoTrusted_node1719520205430")

# Script generated for node Customer-Trusted
CustomerTrusted_node1719520279627 = glueContext.getSink(path="s3://stedi-data/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1719520279627")
CustomerTrusted_node1719520279627.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_trusted")
CustomerTrusted_node1719520279627.setFormat("json")
CustomerTrusted_node1719520279627.writeFrame(LandingtoTrusted_node1719520205430)
job.commit()
