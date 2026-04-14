# ═══════════════════════════════════════════════════════════════
# IBM Cloud — Medallion Data Pipeline Infrastructure
# ═══════════════════════════════════════════════════════════════
# Provisioning:
#   - Networking (VPC, Subnets, Security Groups, ACLs)
#   - Compute (IKS, Analytics Engine Serverless)
#   - Storage (COS Medallion Buckets + Lifecycle Policies)
#   - Database (Db2 on Cloud)
#   - Security (Key Protect, IAM, Secrets Manager)
#   - Observability (Activity Tracker, Log Analysis, Monitoring)
#   - CI/CD (Continuous Delivery Toolchain + Tekton)
# ═══════════════════════════════════════════════════════════════

provider "ibm" {
  ibmcloud_api_key = var.ibmcloud_api_key
  region           = var.region
}

# ─────────────────────────────────────────────────────────────
# Data Sources
# ─────────────────────────────────────────────────────────────
data "ibm_resource_group" "rg" {
  name = var.resource_group
}

data "ibm_resource_instance" "cos" {
  name              = var.cos_instance_name
  service           = "cloud-object-storage"
  resource_group_id = data.ibm_resource_group.rg.id
}

# Random suffix for globally unique resource names
resource "random_id" "suffix" {
  byte_length = 3
}

# ═══════════════════════════════════════════════════════════════
# 1. NETWORKING — VPC, Subnets, Security Groups, ACLs
# ═══════════════════════════════════════════════════════════════
resource "ibm_is_vpc" "main" {
  name                        = "${local.prefix}-vpc"
  resource_group              = data.ibm_resource_group.rg.id
  address_prefix_management   = "auto"
  default_network_acl_name    = "${local.prefix}-default-acl"
  default_security_group_name = "${local.prefix}-default-sg"
  default_routing_table_name  = "${local.prefix}-default-rt"
  tags                        = local.common_tags
}

resource "ibm_is_subnet" "compute" {
  name                     = "${local.prefix}-compute-subnet"
  vpc                      = ibm_is_vpc.main.id
  zone                     = var.cluster_zone
  total_ipv4_address_count = 256
  resource_group           = data.ibm_resource_group.rg.id
  tags                     = local.common_tags
}

resource "ibm_is_public_gateway" "egress" {
  name           = "${local.prefix}-pgw"
  vpc            = ibm_is_vpc.main.id
  zone           = var.cluster_zone
  resource_group = data.ibm_resource_group.rg.id
  tags           = local.common_tags
}

resource "ibm_is_subnet_public_gateway_attachment" "compute_pgw" {
  subnet         = ibm_is_subnet.compute.id
  public_gateway = ibm_is_public_gateway.egress.id
}

# Security Group — Spark cluster communication
resource "ibm_is_security_group" "spark" {
  name           = "${local.prefix}-spark-sg"
  vpc            = ibm_is_vpc.main.id
  resource_group = data.ibm_resource_group.rg.id
  tags           = local.common_tags
}

resource "ibm_is_security_group_rule" "spark_inbound_cluster" {
  group     = ibm_is_security_group.spark.id
  direction = "inbound"
  remote    = ibm_is_security_group.spark.id
  tcp {
    port_min = 1
    port_max = 65535
  }
}

resource "ibm_is_security_group_rule" "spark_outbound_all" {
  group     = ibm_is_security_group.spark.id
  direction = "outbound"
  remote    = "0.0.0.0/0"
}

resource "ibm_is_security_group_rule" "spark_inbound_ui" {
  group     = ibm_is_security_group.spark.id
  direction = "inbound"
  remote    = var.allowed_cidr
  tcp {
    port_min = 8080
    port_max = 8080
  }
}

resource "ibm_is_security_group_rule" "spark_inbound_api" {
  group     = ibm_is_security_group.spark.id
  direction = "inbound"
  remote    = var.allowed_cidr
  tcp {
    port_min = 443
    port_max = 443
  }
}

# Network ACL — Subnet-level firewall
resource "ibm_is_network_acl" "compute" {
  name           = "${local.prefix}-compute-acl"
  vpc            = ibm_is_vpc.main.id
  resource_group = data.ibm_resource_group.rg.id
  tags           = local.common_tags

  # Allow all inbound from VPC CIDR
  rules {
    name        = "allow-vpc-inbound"
    action      = "allow"
    direction   = "inbound"
    source      = "10.0.0.0/8"
    destination = "0.0.0.0/0"
  }

  # Allow HTTPS inbound
  rules {
    name        = "allow-https-inbound"
    action      = "allow"
    direction   = "inbound"
    source      = "0.0.0.0/0"
    destination = "0.0.0.0/0"
    tcp {
      port_min        = 443
      port_max        = 443
      source_port_min = 1
      source_port_max = 65535
    }
  }

  # Allow all outbound
  rules {
    name        = "allow-all-outbound"
    action      = "allow"
    direction   = "outbound"
    source      = "0.0.0.0/0"
    destination = "0.0.0.0/0"
  }

  # Deny all other inbound
  rules {
    name        = "deny-all-inbound"
    action      = "deny"
    direction   = "inbound"
    source      = "0.0.0.0/0"
    destination = "0.0.0.0/0"
  }
}

# ═══════════════════════════════════════════════════════════════
# 2. SECURITY — Key Protect (Encryption at Rest)
# ═══════════════════════════════════════════════════════════════
resource "ibm_resource_instance" "key_protect" {
  count             = var.enable_key_protect ? 1 : 0
  name              = "${local.prefix}-key-protect"
  service           = "kms"
  plan              = "tiered-pricing"
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags
}

resource "ibm_kms_key" "cos_encryption" {
  count       = var.enable_key_protect ? 1 : 0
  instance_id = ibm_resource_instance.key_protect[0].guid
  key_name    = "${local.prefix}-cos-root-key"
  standard_key = false
  force_delete = true
}

# IAM Authorization: COS → Key Protect (for encrypted buckets)
resource "ibm_iam_authorization_policy" "cos_kms" {
  count                       = var.enable_key_protect ? 1 : 0
  source_service_name         = "cloud-object-storage"
  source_resource_instance_id = data.ibm_resource_instance.cos.guid
  target_service_name         = "kms"
  target_resource_instance_id = ibm_resource_instance.key_protect[0].guid
  roles                       = ["Reader"]
}

# ═══════════════════════════════════════════════════════════════
# 3. SECRETS MANAGER — Centralized credential management
# ═══════════════════════════════════════════════════════════════
resource "ibm_resource_instance" "secrets_manager" {
  count             = var.enable_secrets_manager ? 1 : 0
  name              = "${local.prefix}-secrets-mgr"
  service           = "secrets-manager"
  plan              = "trial"
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags
}

# ═══════════════════════════════════════════════════════════════
# 4. COMPUTE — IKS Cluster (VPC Gen2)
# ═══════════════════════════════════════════════════════════════
resource "ibm_container_vpc_cluster" "spark" {
  name              = "${local.prefix}-iks"
  vpc_id            = ibm_is_vpc.main.id
  kube_version      = var.kube_version
  flavor            = var.worker_flavor
  worker_count      = var.worker_count
  resource_group_id = data.ibm_resource_group.rg.id
  cos_instance_crn  = data.ibm_resource_instance.cos.crn
  tags              = local.common_tags

  zones {
    name      = var.cluster_zone
    subnet_id = ibm_is_subnet.compute.id
  }

  timeouts {
    create = "60m"
    delete = "30m"
  }
}

# ═══════════════════════════════════════════════════════════════
# 5. STORAGE — COS Buckets with Lifecycle Policies
# ═══════════════════════════════════════════════════════════════

# COS HMAC credentials (Spark S3A access + state backend)
resource "ibm_resource_key" "cos_hmac" {
  name                 = "${local.prefix}-cos-hmac"
  resource_instance_id = data.ibm_resource_instance.cos.id
  role                 = "Writer"
  parameters           = { HMAC = true }
}

# Reader credentials (for monitoring/read-only access)
resource "ibm_resource_key" "cos_reader" {
  name                 = "${local.prefix}-cos-reader"
  resource_instance_id = data.ibm_resource_instance.cos.id
  role                 = "Reader"
  parameters           = { HMAC = true }
}

# RAW bucket — source data, archive after 90 days
resource "ibm_cos_bucket" "raw" {
  bucket_name          = local.bucket_names.raw
  resource_instance_id = data.ibm_resource_instance.cos.id
  region_location      = var.region
  storage_class        = "standard"

  dynamic "archive_rule" {
    for_each = var.enable_lifecycle_policies ? [1] : []
    content {
      rule_id = "archive-raw-90d"
      enable  = true
      days    = 90
      type    = "Glacier"
    }
  }

  dynamic "expire_rule" {
    for_each = var.enable_lifecycle_policies ? [1] : []
    content {
      rule_id = "expire-raw-365d"
      enable  = true
      days    = 365
    }
  }

  dynamic "activity_tracking" {
    for_each = var.enable_activity_tracker ? [1] : []
    content {
      read_data_events  = true
      write_data_events = true
    }
  }

  dynamic "metrics_monitoring" {
    for_each = var.enable_monitoring ? [1] : []
    content {
      usage_metrics_enabled   = true
      request_metrics_enabled = true
    }
  }
}

# Bronze bucket — deduped parquet, standard retention
resource "ibm_cos_bucket" "bronze" {
  bucket_name          = local.bucket_names.bronze
  resource_instance_id = data.ibm_resource_instance.cos.id
  region_location      = var.region
  storage_class        = "standard"

  dynamic "activity_tracking" {
    for_each = var.enable_activity_tracker ? [1] : []
    content {
      read_data_events  = true
      write_data_events = true
    }
  }

  dynamic "metrics_monitoring" {
    for_each = var.enable_monitoring ? [1] : []
    content {
      usage_metrics_enabled   = true
      request_metrics_enabled = true
    }
  }
}

# Silver bucket — business logic, standard retention
resource "ibm_cos_bucket" "silver" {
  bucket_name          = local.bucket_names.silver
  resource_instance_id = data.ibm_resource_instance.cos.id
  region_location      = var.region
  storage_class        = "standard"

  dynamic "activity_tracking" {
    for_each = var.enable_activity_tracker ? [1] : []
    content {
      read_data_events  = true
      write_data_events = true
    }
  }

  dynamic "metrics_monitoring" {
    for_each = var.enable_monitoring ? [1] : []
    content {
      usage_metrics_enabled   = true
      request_metrics_enabled = true
    }
  }
}

# Gold bucket — star schema + Delta, highest retention
resource "ibm_cos_bucket" "gold" {
  bucket_name          = local.bucket_names.gold
  resource_instance_id = data.ibm_resource_instance.cos.id
  region_location      = var.region
  storage_class        = "standard"

  dynamic "retention_rule" {
    for_each = var.enable_lifecycle_policies ? [1] : []
    content {
      default   = 30
      maximum   = 365
      minimum   = 7
      permanent = false
    }
  }

  dynamic "activity_tracking" {
    for_each = var.enable_activity_tracker ? [1] : []
    content {
      read_data_events  = true
      write_data_events = true
    }
  }

  dynamic "metrics_monitoring" {
    for_each = var.enable_monitoring ? [1] : []
    content {
      usage_metrics_enabled   = true
      request_metrics_enabled = true
    }
  }
}

# Logs bucket — audit logs, auto-expire after 30 days
resource "ibm_cos_bucket" "logs" {
  count                = var.enable_activity_tracker ? 1 : 0
  bucket_name          = local.bucket_names.logs
  resource_instance_id = data.ibm_resource_instance.cos.id
  region_location      = var.region
  storage_class        = "standard"

  expire_rule {
    rule_id = "expire-logs-30d"
    enable  = true
    days    = 30
  }
}

# ═══════════════════════════════════════════════════════════════
# 6. DATABASE — Db2 on Cloud
# ═══════════════════════════════════════════════════════════════
resource "ibm_resource_instance" "db2" {
  name              = "${local.prefix}-db2"
  service           = "dashdb-for-transactions"
  plan              = var.db2_plan
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags
}

resource "ibm_resource_key" "db2_credentials" {
  name                 = "${local.prefix}-db2-key"
  resource_instance_id = ibm_resource_instance.db2.id
  role                 = "Manager"
}

# ═══════════════════════════════════════════════════════════════
# 7. ETL — DataStage
# ═══════════════════════════════════════════════════════════════
resource "ibm_resource_instance" "datastage" {
  name              = "${local.prefix}-datastage"
  service           = "datastage"
  plan              = "lite"
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags
}

# ═══════════════════════════════════════════════════════════════
# 8. SPARK — Analytics Engine Serverless
# ═══════════════════════════════════════════════════════════════
resource "ibm_resource_instance" "analytics_engine" {
  name              = "${local.prefix}-ae"
  service           = "ibmanalyticsengine"
  plan              = "standard-serverless-spark"
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags

  parameters = {
    default_runtime = jsonencode({
      spark_version = "3.5"
    })
    instance_home = jsonencode({
      region          = var.region
      endpoint        = local.cos_direct_endpoint
      hmac_access_key = ibm_resource_key.cos_hmac.credentials["cos_hmac_keys.access_key_id"]
      hmac_secret_key = ibm_resource_key.cos_hmac.credentials["cos_hmac_keys.secret_access_key"]
      guid            = data.ibm_resource_instance.cos.guid
    })
  }

  lifecycle {
    ignore_changes = [parameters]
  }
}

resource "ibm_resource_key" "ae_credentials" {
  name                 = "${local.prefix}-ae-key"
  resource_instance_id = ibm_resource_instance.analytics_engine.id
  role                 = "Manager"
}

# ═══════════════════════════════════════════════════════════════
# 9. OBSERVABILITY — Activity Tracker, Log Analysis, Monitoring
# ═══════════════════════════════════════════════════════════════

# Activity Tracker — Audit all API calls
resource "ibm_resource_instance" "activity_tracker" {
  count             = var.enable_activity_tracker ? 1 : 0
  name              = "${local.prefix}-activity-tracker"
  service           = "logdnaat"
  plan              = "lite"
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags
}

# Log Analysis — Centralized logging
resource "ibm_resource_instance" "log_analysis" {
  count             = var.enable_log_analysis ? 1 : 0
  name              = "${local.prefix}-log-analysis"
  service           = "logdna"
  plan              = "lite"
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags
}

# IBM Cloud Monitoring — Metrics and alerting
resource "ibm_resource_instance" "monitoring" {
  count             = var.enable_monitoring ? 1 : 0
  name              = "${local.prefix}-monitoring"
  service           = "sysdig-monitor"
  plan              = "lite"
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags

  parameters = {
    default_receiver = true
  }
}

resource "ibm_resource_key" "monitoring_credentials" {
  count                = var.enable_monitoring ? 1 : 0
  name                 = "${local.prefix}-monitoring-key"
  resource_instance_id = ibm_resource_instance.monitoring[0].id
  role                 = "Manager"
}

# ═══════════════════════════════════════════════════════════════
# 10. CI/CD — Continuous Delivery + Toolchain
# ═══════════════════════════════════════════════════════════════
resource "ibm_resource_instance" "continuous_delivery" {
  name              = "${local.prefix}-cd"
  service           = "continuous-delivery"
  plan              = "lite"
  location          = var.region
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags
}

resource "ibm_cd_toolchain" "pipeline" {
  name              = "${local.prefix}-toolchain"
  resource_group_id = data.ibm_resource_group.rg.id
  tags              = local.common_tags
}

resource "ibm_cd_toolchain_tool_githubconsolidated" "repo" {
  toolchain_id = ibm_cd_toolchain.pipeline.id
  name         = "data-engineer-repo"

  initialization {
    type     = "link"
    repo_url = var.github_repo_url
  }

  parameters {
    toolchain_issues_enabled = false
    enable_traceability      = true
  }
}

# ═══════════════════════════════════════════════════════════════
# 11. IAM — Access Groups and Policies
# ═══════════════════════════════════════════════════════════════

# Data Engineers group — full access to data services
resource "ibm_iam_access_group" "data_engineers" {
  name        = "${local.prefix}-data-engineers"
  description = "Data engineering team: COS Writer, Db2 Manager, AE Manager"
  tags        = local.common_tags
}

resource "ibm_iam_access_group_policy" "de_cos" {
  access_group_id = ibm_iam_access_group.data_engineers.id
  roles           = ["Writer", "Object Writer"]

  resources {
    service              = "cloud-object-storage"
    resource_instance_id = data.ibm_resource_instance.cos.guid
  }
}

resource "ibm_iam_access_group_policy" "de_ae" {
  access_group_id = ibm_iam_access_group.data_engineers.id
  roles           = ["Manager"]

  resources {
    service              = "ibmanalyticsengine"
    resource_instance_id = ibm_resource_instance.analytics_engine.guid
  }
}

resource "ibm_iam_access_group_policy" "de_db2" {
  access_group_id = ibm_iam_access_group.data_engineers.id
  roles           = ["Manager"]

  resources {
    service              = "dashdb-for-transactions"
    resource_instance_id = ibm_resource_instance.db2.guid
  }
}

# Data Analysts group — read-only COS, read Db2
resource "ibm_iam_access_group" "data_analysts" {
  name        = "${local.prefix}-data-analysts"
  description = "Data analysts: COS Reader, Db2 Reader"
  tags        = local.common_tags
}

resource "ibm_iam_access_group_policy" "da_cos" {
  access_group_id = ibm_iam_access_group.data_analysts.id
  roles           = ["Reader", "Object Reader"]

  resources {
    service              = "cloud-object-storage"
    resource_instance_id = data.ibm_resource_instance.cos.guid
  }
}

resource "ibm_iam_access_group_policy" "da_db2" {
  access_group_id = ibm_iam_access_group.data_analysts.id
  roles           = ["Viewer"]

  resources {
    service              = "dashdb-for-transactions"
    resource_instance_id = ibm_resource_instance.db2.guid
  }
}
