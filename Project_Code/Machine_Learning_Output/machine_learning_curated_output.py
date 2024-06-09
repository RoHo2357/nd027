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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1717950980215 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://roisin-bucket/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1717950980215")

# Script generated for node accelerometer trusted
accelerometertrusted_node1717950980913 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://roisin-bucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1717950980913")

# Script generated for node Join
Join_node1717950983561 = Join.apply(frame1=step_trainer_trusted_node1717950980215, frame2=accelerometertrusted_node1717950980913, keys1=["sensorReadingTime"], keys2=["timestamp"], transformation_ctx="Join_node1717950983561")

# Script generated for node Drop Fields
DropFields_node1717956690147 = DropFields.apply(frame=Join_node1717950983561, paths=["user", "timestamp"], transformation_ctx="DropFields_node1717956690147")

# Script generated for node machine_learning_curated
machine_learning_curated_node1717950985554 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1717956690147, connection_type="s3", format="json", connection_options={"path": "s3://roisin-bucket/spark_output/", "partitionKeys": []}, transformation_ctx="machine_learning_curated_node1717950985554")

job.commit()