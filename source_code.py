import sys
import logging
import boto3
import json
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, sum
from awsglue.utils import getResolvedOptions

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataAssuranceValidation")

# Initialize GlueContext and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

# AWS Secrets Manager Configuration
SECRET_NAME = "dataquality-db-credentials"  # Change this to your AWS Secrets Manager secret name
REGION_NAME = "us-west-2"  # Update this based on your AWS region

# Function to fetch credentials from AWS Secrets Manager
def get_db_credentials(secret_name, region_name):
    try:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        secret_value = client.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as e:
        logger.error(f"Failed to retrieve secrets: {str(e)}")
        #sys.exit(1)

# Retrieve database credentials
secrets = get_db_credentials(SECRET_NAME, REGION_NAME)

# Configurations
params = {
    "SOURCE_PATH": "s3://etldq/sample_employee_data.csv",
    "TARGET_PATH": "s3://etldq/Target_File.csv",  # No longer needed for file-to-table validation
    "DISCREPANCY_REPORT": "s3://etldq/discrepancy_report.csv",
    "MISSING_DATA_REPORT": "s3://etldq/missing_data_report.csv",
    "COMPARISON_TYPE": "file-to-file",  # Options: file-to-file, file-to-table, table-to-table
    #"DB_CONNECTION": f"jdbc:postgresql://{secrets['host']}:{secrets['port']}/{secrets['dbname']}",
    #"DB_TABLE_NAME": "employee__v",
    #"DB_USER": secrets["username"],
    #"DB_PASSWORD": secrets["password"],
}

# Initialize Glue Job
job = Job(glueContext)
job.init("DataValidationJob", params)

def file_exists(path):
    try:
        df = spark.read.format("csv").option("header", "true").load(path)
        return df.count() > 0
    except Exception:
        return False

def load_file(path):
    return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

def load_table():
    return spark.read.format("jdbc") \
        .option("url", params["DB_CONNECTION"]) \
        .option("dbtable", params["DB_TABLE_NAME"]) \
        .option("user", params["DB_USER"]) \
        .option("password", params["DB_PASSWORD"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()

try:
    # Handle the comparison based on the comparison type
    if params["COMPARISON_TYPE"] == "file-to-file":
        # If it's file-to-file, load both source and target files
        source_df = load_file(params['SOURCE_PATH'])
        target_df = load_file(params['TARGET_PATH'])
    elif params["COMPARISON_TYPE"] == "file-to-table":
        # If it's file-to-table, load the source file and the target table
        source_df = load_file(params['SOURCE_PATH'])
        target_df = load_table()  # Load from database table, no TARGET_PATH needed
    else:  # table-to-table or other comparisons
        # If it's table-to-table, load both source and target from database
        source_df = load_table()
        target_df = load_table()

    # Standardize column names
    source_df = source_df.toDF(*[c.lower().replace(" ", "_") for c in source_df.columns])
    target_df = target_df.toDF(*[c.lower().replace(" ", "_") for c in target_df.columns])

    # Schema validation
    source_columns = set(source_df.columns)
    target_columns = set(target_df.columns)
    schema_mismatch = list(source_columns.symmetric_difference(target_columns))

    if not schema_mismatch:
        logger.info("✅ Source and target columns match perfectly.")
    else:
        logger.warning("⚠️ Source and target columns differ:")
        for column in schema_mismatch:
            logger.warning(f"- {column}")

    # Row count validation
    source_row_count = source_df.count()
    target_row_count = target_df.count()
    row_count_mismatch = source_row_count != target_row_count

    # Column count validation
    column_count_mismatch = len(source_columns) != len(target_columns)

    # Calculate missing rows in target
    missing_data_in_target_df = source_df.join(target_df, on=source_df.columns, how='left_anti')
    missing_row_count = missing_data_in_target_df.count()

    # Duplicate handling in target
    duplicate_count = target_df.groupBy(target_df.columns).agg(count("*").alias("count")).filter(col("count") > 1).count()
    has_duplicates = duplicate_count > 0

    # Not Null Check
    not_null_violations = target_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in target_df.columns])
    not_null_dict = not_null_violations.collect()[0].asDict()
    not_null_issues = {k: v for k, v in not_null_dict.items() if v > 0}

    # Generate discrepancy report
    discrepancy_data = [
        {"Validation": "Schema Validation", "Discrepancy": ", ".join(schema_mismatch) if schema_mismatch else "✅ Source and target columns match perfectly"},
        {"Validation": "Row Count Validation", "Discrepancy": f"⚠️ Source: {source_row_count} rows, Target: {target_row_count} rows" if row_count_mismatch else "✅ Record count matches."},
        {"Validation": "Column Count Validation", "Discrepancy": f"⚠️ Source: {len(source_columns)}, Target: {len(target_columns)}" if column_count_mismatch else "✅ No Issue"},
        {"Validation": "Missing Data", "Discrepancy": f"⚠️ Rows missing in Target: {missing_row_count}" if missing_row_count > 0 else "✅ No missing rows"},
        {"Validation": "Duplicate Check", "Discrepancy": f"⚠️ Duplicate count: {duplicate_count}" if has_duplicates else "✅ No Duplicates found"},
        {"Validation": "Not Null Check", "Discrepancy": str(not_null_issues) if not_null_issues else "✅ No null values found"}
    ]

    # Save missing data report if needed
    if missing_row_count > 0:
        logger.warning(f"⚠️ Found {missing_row_count} rows missing in the target dataset.")
        missing_data_in_target_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(params['MISSING_DATA_REPORT'])
        logger.info(f"📂 Missing data report saved at: {params['MISSING_DATA_REPORT']}")
    else:
        logger.info("✅ No missing rows found in the target dataset.")

    # Create discrepancy report
    discrepancy_df = spark.createDataFrame(discrepancy_data)
    discrepancy_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(params['DISCREPANCY_REPORT'])
    logger.info(f"📂 Discrepancy report saved at: {params['DISCREPANCY_REPORT']}")

    # Commit Glue job
    job.commit()
    logger.info("✅ AWS Glue job completed successfully.")

except KeyError as e:
    logger.error(f"❌ Missing required parameter: {str(e)}")
except Exception as e:
    logger.error(f"❌ Unexpected error: {str(e)}")
