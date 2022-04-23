import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Data Catalog: database and table name
db_name = "customer_database"
tbl_name = "customers_csv"
output_dir = "s3://appflow-test-ash/glue/data/customers_database/customers_json/"

# Read data into a DynamicFrame using the Data Catalog metadata
customer_frame = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_name)

# Remove erroneous records
customer_df = customer_frame.toDF()
customer_df = customer_df.where("customerid is NOT NULL")

# Remove the namestyle column
customer_df.drop('namestyle')

customer_frame = DynamicFrame.fromDF(customer_df, glueContext, "customer")

# write output to S3
glueContext.write_dynamic_frame.from_options(frame = customer_frame, connection_type = "s3", connection_options = {"path": output_dir}, format = "json")

job.commit()
