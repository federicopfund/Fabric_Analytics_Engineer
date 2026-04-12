"""
IBM Cloud - Shared configuration for the Medallion Pipeline.
COS endpoints, credentials, bucket names, and SparkSession builder.
"""
import os

# ---------------------------------------------------------------------------
# IBM Cloud Object Storage
# ---------------------------------------------------------------------------
COS_CONFIG = {
    "access_key": os.environ.get("COS_ACCESS_KEY", "786065478ff34d3b84016125490a4d12"),
    "secret_key": os.environ.get("COS_SECRET_KEY", "838da9c9a9cd2521d51e856da0dd876884f8108226f5bd7c"),
    "endpoint": os.environ.get("COS_ENDPOINT", "s3.us-south.cloud-object-storage.appdomain.cloud"),
    "region": "us-south",
}

BUCKETS = {
    "raw": "datalake-raw-us-south",
    "bronze": "datalake-bronze-us-south",
    "silver": "datalake-silver-us-south",
    "gold": "datalake-gold-us-south",
}

# ---------------------------------------------------------------------------
# IBM Db2 on Cloud
# ---------------------------------------------------------------------------
DB2_CONFIG = {
    "hostname": os.environ.get("DB2_HOST", "6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud"),
    "port": os.environ.get("DB2_PORT", "30376"),
    "database": "bludb",
    "username": os.environ.get("DB2_USER", "qtn87286"),
    "password": os.environ.get("DB2_PASS", "G5VYX4VkCRqrKeNz"),
}

DB2_JDBC_URL = (
    f"jdbc:db2://{DB2_CONFIG['hostname']}:{DB2_CONFIG['port']}"
    f"/{DB2_CONFIG['database']}:sslConnection=true;"
)

DB2_JDBC_PROPS = {
    "user": DB2_CONFIG["username"],
    "password": DB2_CONFIG["password"],
    "driver": "com.ibm.db2.jcc.DB2Driver",
}

# ---------------------------------------------------------------------------
# Paths helper (S3A)
# ---------------------------------------------------------------------------
def cos_path(layer: str, table: str) -> str:
    """Build s3a:// path for a given layer and table."""
    return f"s3a://{BUCKETS[layer]}/{table}/"


def build_spark(app_name: str = "IBM-Medallion-Pipeline"):
    """Create a SparkSession pre-configured for COS S3A and Db2 JDBC."""
    from pyspark.sql import SparkSession

    packages = ",".join([
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "com.ibm.db2:jcc:11.5.9.0",
        "io.delta:delta-core_2.12:2.3.0",
    ])

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.hadoop.fs.s3a.access.key", COS_CONFIG["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", COS_CONFIG["secret_key"])
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{COS_CONFIG['endpoint']}")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .master("local[*]")
        .getOrCreate()
    )
