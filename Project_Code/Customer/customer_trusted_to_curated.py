import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1717948179253 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://roisin-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrustedZone_node1717948179253")

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1717948180988 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://roisin-bucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrustedZone_node1717948180988")

# Script generated for node Join
Join_node1717948147333 = Join.apply(frame1=CustomerTrustedZone_node1717948179253, frame2=AccelerometerTrustedZone_node1717948180988, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1717948147333")

# Script generated for node Drop Fields
DropFields_node1717948156636 = DropFields.apply(frame=Join_node1717948147333, paths=["z", "user", "y", "x", "timestamp"], transformation_ctx="DropFields_node1717948156636")

# Script generated for node Drop Duplicates
DropDuplicates_node1717948159156 =  DynamicFrame.fromDF(DropFields_node1717948156636.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1717948159156")

# Script generated for node customer_curated
customer_curated_node1717948162965 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1717948159156, connection_type="s3", format="json", connection_options={"path": "s3://roisin-bucket/customer/curated/", "partitionKeys": []}, transformation_ctx="customer_curated_node1717948162965")

job.commit()