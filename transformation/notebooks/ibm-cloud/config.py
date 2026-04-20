"""
IBM Cloud - Shared configuration for the Medallion Pipeline.
COS endpoints, credentials, bucket names, SparkSession builder,
and Analytics Engine integration.
"""
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Load .env file
# ---------------------------------------------------------------------------
_ENV_FILE = Path(__file__).resolve().parents[3] / "infrastructure" / "ibm-cloud" / ".env"

if _ENV_FILE.exists():
    with open(_ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

# ---------------------------------------------------------------------------
# IBM Cloud Object Storage
# ---------------------------------------------------------------------------
COS_CONFIG = {
    "access_key": os.environ["COS_ACCESS_KEY"],
    "secret_key": os.environ["COS_SECRET_KEY"],
    "endpoint": os.environ.get("COS_ENDPOINT", "s3.us-south.cloud-object-storage.appdomain.cloud"),
    "region": "us-south",
}

BUCKETS = {
    "raw": os.environ.get("COS_BUCKET_RAW", "datalake-raw-us-south"),
    "bronze": os.environ.get("COS_BUCKET_BRONZE", "datalake-bronze-us-south"),
    "silver": os.environ.get("COS_BUCKET_SILVER", "datalake-silver-us-south"),
    "gold": os.environ.get("COS_BUCKET_GOLD", "datalake-gold-us-south"),
}

# ---------------------------------------------------------------------------
# IBM Db2 on Cloud
# ---------------------------------------------------------------------------
DB2_CONFIG = {
    "hostname": os.environ["DB2_HOSTNAME"],
    "port": os.environ.get("DB2_PORT", "30376"),
    "database": os.environ.get("DB2_DATABASE", "bludb"),
    "username": os.environ["DB2_USERNAME"],
    "password": os.environ["DB2_PASSWORD"],
}

# ---------------------------------------------------------------------------
# IBM Analytics Engine Serverless
# ---------------------------------------------------------------------------
_ae_instance = os.environ["AE_INSTANCE_ID"]
_ae_region = os.environ.get("AE_REGION", "us-south")

AE_CONFIG = {
    "instance_id": _ae_instance,
    "api_key": os.environ["AE_API_KEY"],
    "region": _ae_region,
    "api_endpoint": os.environ.get("AE_API_ENDPOINT",
        f"https://api.{_ae_region}.ae.cloud.ibm.com/v3/analytics_engines/{_ae_instance}"),
    "spark_applications_url": os.environ.get("AE_SPARK_APPS_URL",
        f"https://api.{_ae_region}.ae.cloud.ibm.com/v3/analytics_engines/{_ae_instance}/spark_applications"),
    "history_ui": f"https://spark-console.{_ae_region}.ae.cloud.ibm.com/v3/analytics_engines/{_ae_instance}/spark_history_ui",
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

    builder = SparkSession.builder
    builder = (
        builder
        .appName(app_name)  # type: ignore[union-attr]
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

    return builder
