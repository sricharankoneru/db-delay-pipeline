import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

# ------------------------------------------------------------
# Standard Glue job boilerplate — sets up the Spark/Glue environment
# ------------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------
# STEP 1: Read the raw table from the Glue Data Catalog
# This does NOT load raw files manually — it uses the table
# definition your crawler (or manual DDL) already created.
# ------------------------------------------------------------
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="db_delay_pipeline",
    table_name="raw"
)

# Convert to a Spark DataFrame — easier to work with for filtering/deduping
raw_df = raw_dyf.toDF()

print(f"Raw record count: {raw_df.count()}")

# ------------------------------------------------------------
# STEP 2: Deduplicate
# Since Lambda runs every 10 min, the same (trip_id, stop_id) can
# appear multiple times with different delay values as the train
# progresses. For the curated layer, we keep the MOST RECENT
# delay reading per trip+stop+day (not every single snapshot).
# ------------------------------------------------------------
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc

window_spec = Window.partitionBy("trip_id", "stop_id", "year", "month", "day") \
                     .orderBy(desc("pulled_at"))

deduped_df = raw_df.withColumn("row_num", row_number().over(window_spec)) \
                    .filter(col("row_num") == 1) \
                    .drop("row_num")

print(f"Deduplicated record count: {deduped_df.count()}")

# ------------------------------------------------------------
# STEP 3: Write to curated zone as partitioned Parquet
# Parquet is columnar and compressed — much faster and cheaper
# to query in Athena than raw CSV, especially as data grows.
# ------------------------------------------------------------
curated_dyf = DynamicFrame.fromDF(deduped_df, glueContext, "curated_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=curated_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://db-delay-pipeline-sricharan-274535057566-eu-central-1-an/curated/",
        "partitionKeys": ["year", "month", "day"]
    },
    format="parquet"
)

job.commit()