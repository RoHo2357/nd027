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

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1717932531772 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://roisin-bucket/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingZone_node1717932531772")

# Script generated for node Filter-research
Filterresearch_node1717933149848 = Filter.apply(frame=CustomerLandingZone_node1717932531772, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filterresearch_node1717933149848")

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1717932627032 = glueContext.write_dynamic_frame.from_options(frame=Filterresearch_node1717933149848, connection_type="s3", format="json", connection_options={"path": "s3://roisin-bucket/customer/trusted/", "partitionKeys": []}, transformation_ctx="CustomerTrustedZone_node1717932627032")

job.commit()