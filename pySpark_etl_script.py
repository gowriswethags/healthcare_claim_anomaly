import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = "s3://healthcare-capstone-project/new_datasets/bronze/providers/"
providers_schema = StructType([
    StructField("accepting_new_patients", BooleanType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("license_number", StringType(), True),
    StructField("license_state", StringType(), True),
    StructField("name", StringType(), True),
    StructField("npi_number", StringType(), True),
    StructField("organization", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("specialty", StringType(), True),
    StructField("state", StringType(), True),
    StructField("years_of_experience", LongType(), True),
    StructField("zip_code", StringType(), True)
])


df = spark.read.schema().json(input_path)

# === Step 2: Data Cleaning & Transformation ===
df = df.dropDuplicates(["provider_id", "npi_number"])

df = df.filter(col("provider_id").isNotNull() & col("npi_number").isNotNull() & col("name").isNotNull())

df = df.withColumn("email", when(col("email").isNull() | (col("email") == ""), "unknown@example.com").otherwise(col("email"))) \
       .withColumn("phone_number", when(col("phone_number").isNull() | (col("phone_number") == ""), "0000000000").otherwise(col("phone_number")))

for field in ["name", "city", "specialty", "organization"]:
    df = df.withColumn(field, initcap(trim(col(field))))

df = df.withColumn("state", upper(trim(col("state"))))

df = df.withColumn("specialty", initcap(trim(lower(col("specialty")))))

df = df.withColumn("accepting_new_patients", when(lower(col("accepting_new_patients").cast("string")) == "yes", lit(True)).otherwise(lit(False)))

valid_email_df = df.filter(col("email").rlike("^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"))
invalid_email_df = df.filter(~col("email").rlike("^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"))

valid_phone_df = valid_email_df.filter(col("phone_number").rlike("^\\+?[0-9]{10,15}$"))
invalid_phone_df = valid_email_df.filter(~col("phone_number").rlike("^\\+?[0-9]{10,15}$"))

valid_zip_df = valid_phone_df.filter(col("zip_code").rlike("^\\d{6}$"))
invalid_zip_df = valid_phone_df.filter(~col("zip_code").rlike("^\\d{6}$"))

df = df.withColumn("senior_providers", when(col("years_of_experience") > 25, True).otherwise(False))

grp_df = df.groupBy("specialty").agg(count("provider_id").alias("num_providers"))

df = df.withColumn("is_cardiologist", when(col("specialty").like("%Cardiology%"), 1).otherwise(0))

df = df.withColumn("experience_level", when(col("years_of_experience") < 5, "Junior")
                   .when(col("years_of_experience") < 15, "Mid")
                   .otherwise("Senior"))

org_grouped = df.groupBy("organization").agg(count("provider_id").alias("providers_count"))

spec_count = df.groupBy("specialty").count()
window_spec = Window.orderBy(col("count").desc())
spec_count_ranked = spec_count.withColumn("rank", dense_rank().over(window_spec))

df = df.withColumn("risk_score", when(col("years_of_experience") > 40, 0.9)
                   .when(col("specialty") == "Surgery", 0.8)
                   .otherwise(0.3))

df = df.filter(col("npi_number").rlike("^\\d{10}$"))

df = df.withColumn("license_state_match", col("license_state") == col("state"))

duplicate_license = df.groupBy("license_number").count().filter(col("count") > 1)

outliers = df.filter(col("years_of_experience") > 60)

missing_license_state = df.filter(col("license_state").isNull())

duplicate_phone = df.groupBy("phone_number").count().filter(col("count") > 1)

df = df.withColumn("masked_email", regexp_replace(col("email"), "@.*", "@*****.com"))


output_path = "s3://healthcare-capstone-project/new_datasets/silver/providers/"
df.coalesce(1).write.mode("overwrite").parquet(output_path)

# === Step 4: Commit Job ===
job.commit()
