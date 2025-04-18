import os, sys, time, json
from pyspark.sql import SparkSession
import boto3
from urllib.parse import urlparse

# Define versions
ICEBERG_VERSION = "1.4.0"
HADOOP_AWS_VERSION = "3.3.4"

# Define package dependencies
PACKAGES = f"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:{ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"

def initialize_spark():
    # Set Submit Arguments environment variable for package dependencies
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {PACKAGES} pyspark-shell"

    # Add required S3A configuration with JAR dependencies
    return SparkSession.builder \
        .appName("IcebergReadExample") \
        .config("spark.jars.packages", PACKAGES) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


# Manifest creation
def create_pending_manifest(s3_client, bucket_name, raw_prefix, pending_prefix, uri_scheme, max_files):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=raw_prefix)

    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            files.append(f"{uri_scheme}{bucket_name}/{obj['Key']}")
            if len(files) >= max_files:
                break
        if len(files) >= max_files:
            break

    if not files:
        return None

    manifest_content = '\n'.join(files)
    unix_ts = int(time.time())
    manifest_key = f"{pending_prefix}{unix_ts}.pending"
    s3_client.put_object(Bucket=bucket_name, Key=manifest_key, Body=manifest_content)
    print(f"Created manifest with {len(files)} files: {manifest_key}")
    return f"{uri_scheme}{bucket_name}/{manifest_key}"


def process_data_with_spark(spark, manifest_path, file_type="parquet"):
    manifest_df = spark.read.text(manifest_path)
    file_paths = manifest_df.rdd.map(lambda r: r[0]).collect()

    # Print file paths for debugging
    print(f"Processing files: {file_paths}")

    if file_type == "parquet":
        data_df = spark.read.parquet(*file_paths)
    elif file_type == "csv":
        data_df = spark.read.csv(*file_paths, sep='\t', header=True, inferSchema=True)
    else:
        raise Exception("invalid file format")
    return data_df


def parse_s3_path(s3_uri):
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip('/')


def archive_file(s3_client, bucket_name, old_key, new_key):
    try:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': old_key},
            Key=new_key
        )
        s3_client.delete_object(Bucket=bucket_name, Key=old_key)
    except Exception as e:
        print(f"Error archiving {old_key}: {str(e)}")


def move_to_error(s3_client, bucket_name, old_key, error_prefix):
    try:
        error_key = f"{error_prefix}{os.path.basename(old_key)}"
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': old_key},
            Key=error_key
        )
        print(f"Moved problematic file to error folder: {error_key}")
    except Exception as e:
        print(f"Error moving file to error folder {old_key}: {str(e)}")


def archive_processed_files(s3_client, bucket_name, manifest_path, archived_prefix, error_prefix):
    _, manifest_key = parse_s3_path(manifest_path)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
        content = response['Body'].read().decode('utf-8')
        file_paths = [path.strip() for path in content.split('\n') if path.strip()]

        for file_path in file_paths:
            _, old_key = parse_s3_path(file_path)
            new_key = f"{archived_prefix}{old_key.split('/', 1)[1] if '/' in old_key else old_key}"
            archive_file(s3_client, bucket_name, old_key, new_key)

        s3_client.delete_object(Bucket=bucket_name, Key=manifest_key)
        print(f"Successfully archived {len(file_paths)} files and deleted manifest")
    except Exception as e:
        print(f"Error during archiving: {str(e)}")
        # Move manifest to error folder
        move_to_error(s3_client, bucket_name, manifest_key, error_prefix)


def create_partitioned_iceberg_table_if_not_exists(spark, table_name):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        tenant STRING,
        id INT,
        name STRING,
        amount DOUBLE,
        event_time TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (tenant)
    """)
    print(f"Table {table_name} created or confirmed to exist")


def merge_data_into_iceberg_table(spark, spark_df, table_name):
    # Create a temporary view of all data
    temp_view_name = "staging_data_all"
    spark_df.createOrReplaceTempView(temp_view_name)

    # Merge entire dataset into the partitioned table
    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY tenant, id ORDER BY event_time DESC) AS row_num
                FROM {temp_view_name}
            ) AS deduped
            WHERE row_num = 1
        ) AS source
        ON target.tenant = source.tenant AND target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)
    spark.catalog.dropTempView(temp_view_name)
    print("Merge completed for all data")


def main():
    # Hardcoded parameters
    bucket_name = "warehouse"
    raw_prefix = "data/"
    pending_prefix = "pending/"
    archived_prefix = "archived/"
    error_prefix = "error/"
    file_type = "parquet"
    table_name = "demo.db.multi_tenant"
    uri_scheme = "s3a://"
    max_files = 10000

    # Configure S3 client for MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",    # MinIO endpoint
        aws_access_key_id="admin",           # MinIO access key
        aws_secret_access_key="password",    # MinIO secret key
        region_name="us-east-1",             # Default region
        config=boto3.session.Config(signature_version='s3v4')
    )

    spark = initialize_spark()

    # Explicitly configure Hadoop filesystem for S3A
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # Print current Spark configuration for debugging
    print("Current Spark Configuration:")
    print(f"Spark Version: {spark.version}")
    print(f"Available JARs: {spark.sparkContext.getConf().get('spark.jars', 'None')}")
    print(f"Available Packages: {spark.sparkContext.getConf().get('spark.jars.packages', 'None')}")

    # Create the manifest file
    manifest_path = create_pending_manifest(
        s3_client=s3,
        bucket_name=bucket_name,
        raw_prefix=raw_prefix,
        pending_prefix=pending_prefix,
        uri_scheme=uri_scheme,
        max_files=max_files
    )

    if manifest_path is None:
        print("No files to process. Exiting.")
        return

    # Process the data
    try:
        # Create the partitioned table if it doesn't exist
        create_partitioned_iceberg_table_if_not_exists(spark, table_name)

        # Read the data from manifest
        print(f"Reading manifest from: {manifest_path}")
        spark_df = process_data_with_spark(spark=spark, manifest_path=manifest_path, file_type=file_type)

        # Get list of tenants for logging purposes
        tenants = [row["tenant"] for row in spark_df.select("tenant").distinct().collect()]
        print(f"Processing data for {len(tenants)} tenants: {tenants}")

        # Merge all data into the partitioned table at once
        merge_data_into_iceberg_table(spark, spark_df, table_name)

        # Archive processed files
        archive_processed_files(
            s3_client=s3,
            bucket_name=bucket_name,
            manifest_path=manifest_path,
            archived_prefix=archived_prefix,
            error_prefix=error_prefix
        )

    except Exception as e:
        import traceback
        print(f"Error in main processing: {str(e)}")
        print(traceback.format_exc())
        _, manifest_key = parse_s3_path(manifest_path)
        move_to_error(s3, bucket_name, manifest_key, error_prefix)


if __name__ == "__main__":
    main()
    print("PROCESS COMPLETE")