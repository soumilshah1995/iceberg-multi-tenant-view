from pyspark.sql import SparkSession
import os


def initialize_spark():
    ICEBERG_VERSION = "1.4.0"
    HADOOP_AWS_VERSION = "3.3.4"
    PACKAGES = f"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:{ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"

    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {PACKAGES} pyspark-shell"

    return SparkSession.builder \
        .appName("IcebergPartitionViews") \
        .config("spark.jars.packages", PACKAGES) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def create_partition_views(spark):
    table = "demo.db.multi_tenant"

    # Get distinct partition values with proper column access
    partitions = spark.sql("""
        SELECT DISTINCT partition.tenant AS tenant_value
        FROM demo.db.multi_tenant.files
    """).collect()

    # Create views for each partition
    for row in partitions:
        tenant_value = row.tenant_value
        view_name = f"demo.views.view_{tenant_value}"

        spark.sql(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM {table}
            WHERE tenant = '{tenant_value}'
        """)
        print(f"Created view: {view_name}")


def main():
    spark = initialize_spark()

    # Configure Hadoop
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    create_partition_views(spark)


if __name__ == "__main__":
    main()
