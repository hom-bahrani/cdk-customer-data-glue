import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
dynamic_frame_read = glueContext.create_dynamic_frame.from_catalog(
    database="customer_database",
    table_name="customers_csv",
    transformation_ctx="dynamic_frame_read",
)

data_frame = dynamic_frame_read.toDF()

# Script generated for node Rename Field
dataframe_changed_field = data_frame.apply(
    frame=dynamic_frame_read,
    old_name="customerid",
    new_name="customerUniqueID",
    transformation_ctx="dataframe_changed_field",
)

# Script generated for node Amazon S3
S3_frame = glueContext.write_dynamic_frame.from_options(
    frame=dataframe_changed_field,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://appflow-test-ash/glue/data/customers_database/customer_json/",
        "partitionKeys": [],
    },
    transformation_ctx="S3_frame",
)

job.commit()
