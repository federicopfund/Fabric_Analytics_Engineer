# ═══════════════════════════════════════════════════════════════
# Outputs — Credentials, Endpoints, and Resource IDs
# ═══════════════════════════════════════════════════════════════

# ── Networking ──
output "vpc_id" {
  description = "VPC ID"
  value       = ibm_is_vpc.main.id
}

output "vpc_crn" {
  description = "VPC CRN"
  value       = ibm_is_vpc.main.crn
}

output "subnet_id" {
  description = "Compute subnet ID"
  value       = ibm_is_subnet.compute.id
}

# ── Compute ──
output "cluster_id" {
  description = "IKS Cluster ID"
  value       = ibm_container_vpc_cluster.spark.id
}

output "cluster_name" {
  description = "IKS Cluster name"
  value       = ibm_container_vpc_cluster.spark.name
}

# ── COS Credentials ──
output "cos_access_key_id" {
  description = "COS HMAC access key (Writer)"
  value       = ibm_resource_key.cos_hmac.credentials["cos_hmac_keys.access_key_id"]
  sensitive   = true
}

output "cos_secret_access_key" {
  description = "COS HMAC secret key (Writer)"
  value       = ibm_resource_key.cos_hmac.credentials["cos_hmac_keys.secret_access_key"]
  sensitive   = true
}

output "cos_reader_access_key" {
  description = "COS HMAC access key (Reader)"
  value       = ibm_resource_key.cos_reader.credentials["cos_hmac_keys.access_key_id"]
  sensitive   = true
}

output "cos_endpoint" {
  description = "COS public endpoint"
  value       = local.cos_endpoint
}

output "cos_private_endpoint" {
  description = "COS private endpoint (for VPC-internal traffic)"
  value       = local.cos_private_endpoint
}

# ── COS Bucket Names ──
output "bucket_names" {
  description = "Map of medallion layer → bucket name"
  value       = local.bucket_names
}

# ── Database ──
output "db2_hostname" {
  description = "Db2 connection hostname"
  value       = ibm_resource_key.db2_credentials.credentials["connection.db2.hosts.0.hostname"]
  sensitive   = true
}

output "db2_port" {
  description = "Db2 connection port"
  value       = ibm_resource_key.db2_credentials.credentials["connection.db2.hosts.0.port"]
  sensitive   = true
}

output "db2_database" {
  description = "Db2 database name"
  value       = ibm_resource_key.db2_credentials.credentials["connection.db2.database"]
  sensitive   = true
}

output "db2_crn" {
  description = "Db2 instance CRN"
  value       = ibm_resource_instance.db2.crn
}

# ── Analytics Engine ──
output "ae_instance_id" {
  description = "Analytics Engine instance GUID"
  value       = ibm_resource_instance.analytics_engine.guid
}

output "ae_api_endpoint" {
  description = "Analytics Engine API endpoint"
  value       = local.ae_api_base
}

output "ae_history_ui" {
  description = "Spark History Server UI URL"
  value       = local.ae_history_ui
}

output "ae_api_key" {
  description = "Analytics Engine API Key"
  value       = ibm_resource_key.ae_credentials.credentials["apikey"]
  sensitive   = true
}

# ── CI/CD ──
output "toolchain_id" {
  description = "Continuous Delivery Toolchain ID"
  value       = ibm_cd_toolchain.pipeline.id
}

output "toolchain_url" {
  description = "Toolchain console URL"
  value       = "https://cloud.ibm.com/devops/toolchains/${ibm_cd_toolchain.pipeline.id}?env_id=ibm:yp:${var.region}"
}

# ── Security ──
output "key_protect_id" {
  description = "Key Protect instance ID (if enabled)"
  value       = var.enable_key_protect ? ibm_resource_instance.key_protect[0].guid : null
}

output "secrets_manager_id" {
  description = "Secrets Manager instance ID (if enabled)"
  value       = var.enable_secrets_manager ? ibm_resource_instance.secrets_manager[0].guid : null
}

# ── Observability ──
output "activity_tracker_id" {
  description = "Activity Tracker instance ID (if enabled)"
  value       = var.enable_activity_tracker ? ibm_resource_instance.activity_tracker[0].guid : null
}

output "monitoring_access_key" {
  description = "IBM Cloud Monitoring ingestion key (if enabled)"
  value       = var.enable_monitoring ? ibm_resource_key.monitoring_credentials[0].credentials["Sysdig Access Key"] : null
  sensitive   = true
}

# ── IAM ──
output "access_group_data_engineers" {
  description = "IAM Access Group ID for Data Engineers"
  value       = ibm_iam_access_group.data_engineers.id
}

output "access_group_data_analysts" {
  description = "IAM Access Group ID for Data Analysts"
  value       = ibm_iam_access_group.data_analysts.id
}

# ── Computed Connection Strings ──
output "spark_s3a_config" {
  description = "Spark S3A configuration properties (for spark-defaults.conf)"
  value = {
    "spark.hadoop.fs.s3a.endpoint"                  = "https://${local.cos_endpoint}"
    "spark.hadoop.fs.s3a.impl"                      = "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.hadoop.fs.s3a.path.style.access"         = "true"
    "spark.hadoop.fs.s3a.connection.ssl.enabled"     = "true"
  }
}

output "db2_jdbc_url" {
  description = "JDBC URL for Db2 (requires hostname/port injection)"
  value       = "jdbc:db2://${ibm_resource_key.db2_credentials.credentials["connection.db2.hosts.0.hostname"]}:${ibm_resource_key.db2_credentials.credentials["connection.db2.hosts.0.port"]}/${ibm_resource_key.db2_credentials.credentials["connection.db2.database"]}:sslConnection=true;"
  sensitive   = true
}
