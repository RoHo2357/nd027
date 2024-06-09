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

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1717949072648 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://roisin-bucket/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCuratedZone_node1717949072648")

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1717949071444 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://roisin-bucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLandingZone_node1717949071444")

# Script generated for node Join
Join_node1717949075512 = Join.apply(frame1=StepTrainerLandingZone_node1717949071444, frame2=CustomerCuratedZone_node1717949072648, keys1=["serialNumber"], keys2=["serialNumber"], transformation_ctx="Join_node1717949075512")

# Script generated for node Drop Fields
DropFields_node1717949076826 = DropFields.apply(frame=Join_node1717949075512, paths=["shareWithPublicAsOfDate", "phone", "lastUpdateDate", "shareWithFriendsAsOfDate", "customerName", "registrationDate", "shareWithResearchAsOfDate", "birthDay", "`.serialNumber`", "email"], transformation_ctx="DropFields_node1717949076826")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1717949078472 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1717949076826, connection_type="s3", format="json", connection_options={"path": "s3://roisin-bucket/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="step_trainer_trusted_node1717949078472")

job.commit()