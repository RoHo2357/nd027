import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1717945502593 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://roisin-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrustedZone_node1717945502593")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1717945335606 = glueContext.create_dynamic_frame.from_catalog(database="roisin-database", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1717945335606")

# Script generated for node Join
Join_node1717945403349 = Join.apply(frame1=Accelerometerlanding_node1717945335606, frame2=CustomerTrustedZone_node1717945502593, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1717945403349")

# Script generated for node Drop Fields
DropFields_node1717945731658 = DropFields.apply(frame=Join_node1717945403349, paths=["email", "phone","serialNumber","birthDay","shareWithResearchAsOfDate","registrationDate","customerName","lastUpdateDate","shareWithFriendsAsOfDate","shareWithPublicAsOfDate"], transformation_ctx="DropFields_node1717945731658")

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1717945436398 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1717945731658, connection_type="s3", format="json", connection_options={"path": "s3://roisin-bucket/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AccelerometerTrustedZone_node1717945436398")

job.commit()