# ─────────────────────────────────────────────────────────────
# Locals — Naming conventions, tags, computed values
# ─────────────────────────────────────────────────────────────
locals {
  # Naming convention: {project}-{component}-{env}
  prefix = "${var.project_name}-${var.environment}"

  # Standard tags applied to all resources
  common_tags = [
    "project:${var.project_name}",
    "environment:${var.environment}",
    "managed-by:terraform",
    "team:data-engineering",
    "cost-center:${var.cost_center}",
  ]

  # COS bucket names with environment isolation
  bucket_names = {
    raw    = "datalake-raw-${var.region}-${var.environment}"
    bronze = "datalake-bronze-${var.region}-${var.environment}"
    silver = "datalake-silver-${var.region}-${var.environment}"
    gold   = "datalake-gold-${var.region}-${var.environment}"
    logs   = "datalake-logs-${var.region}-${var.environment}"
  }

  # COS endpoints
  cos_endpoint          = "s3.${var.region}.cloud-object-storage.appdomain.cloud"
  cos_direct_endpoint   = "s3.direct.${var.region}.cloud-object-storage.appdomain.cloud"
  cos_private_endpoint  = "s3.private.${var.region}.cloud-object-storage.appdomain.cloud"

  # Analytics Engine
  ae_history_ui = "https://spark-console.${var.region}.ae.cloud.ibm.com/v3/analytics_engines/${ibm_resource_instance.analytics_engine.guid}/spark_history_ui"
  ae_api_base   = "https://api.${var.region}.ae.cloud.ibm.com/v3/analytics_engines/${ibm_resource_instance.analytics_engine.guid}"
}
