# ═══════════════════════════════════════════════════════════════
# Variables — IBM Cloud Medallion Pipeline
# ═══════════════════════════════════════════════════════════════

# ── Authentication ──
variable "ibmcloud_api_key" {
  description = "IBM Cloud API key (generate at https://cloud.ibm.com/iam/apikeys)"
  type        = string
  sensitive   = true
}

# ── Project ──
variable "project_name" {
  description = "Project name used as prefix for all resources"
  type        = string
  default     = "medallion"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.project_name))
    error_message = "Project name must be 3-21 lowercase alphanumeric characters or hyphens, starting with a letter."
  }
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "cost_center" {
  description = "Cost center tag for billing"
  type        = string
  default     = "data-engineering"
}

# ── Region & Resource Group ──
variable "region" {
  description = "IBM Cloud region"
  type        = string
  default     = "us-south"

  validation {
    condition     = contains(["us-south", "us-east", "eu-de", "eu-gb", "jp-tok", "au-syd"], var.region)
    error_message = "Region must be a valid IBM Cloud MZR."
  }
}

variable "resource_group" {
  description = "Resource group name"
  type        = string
  default     = "Default"
}

# ── Networking ──
variable "cluster_zone" {
  description = "Availability zone for the compute subnet"
  type        = string
  default     = "us-south-1"
}

variable "allowed_cidr" {
  description = "CIDR block allowed to access Spark UI and APIs"
  type        = string
  default     = "0.0.0.0/0"
}

# ── IKS Cluster ──
variable "worker_count" {
  description = "Number of worker nodes in the IKS cluster"
  type        = number
  default     = 2

  validation {
    condition     = var.worker_count >= 1 && var.worker_count <= 10
    error_message = "Worker count must be between 1 and 10."
  }
}

variable "worker_flavor" {
  description = "Worker node machine type (vCPU x RAM)"
  type        = string
  default     = "bx2.4x16"
}

variable "kube_version" {
  description = "Kubernetes version for IKS"
  type        = string
  default     = "1.30"
}

# ── COS ──
variable "cos_instance_name" {
  description = "Existing Cloud Object Storage instance name"
  type        = string
  default     = "CloudObjectStorage"
}

# ── Database ──
variable "db2_plan" {
  description = "Db2 on Cloud plan (free, standard, enterprise)"
  type        = string
  default     = "free"

  validation {
    condition     = contains(["free", "standard", "enterprise"], var.db2_plan)
    error_message = "Db2 plan must be one of: free, standard, enterprise."
  }
}

# ── GitHub ──
variable "github_repo_url" {
  description = "GitHub repository URL for CI/CD toolchain"
  type        = string
  default     = "https://github.com/federicopfund/Fabric_Analytics_Engineer"
}

# ── Feature Flags (toggle per environment) ──
variable "enable_key_protect" {
  description = "Enable Key Protect for COS encryption at rest"
  type        = bool
  default     = false
}

variable "enable_secrets_manager" {
  description = "Enable IBM Secrets Manager for centralized credentials"
  type        = bool
  default     = false
}

variable "enable_activity_tracker" {
  description = "Enable Activity Tracker for API audit logging"
  type        = bool
  default     = false
}

variable "enable_log_analysis" {
  description = "Enable IBM Log Analysis for centralized logging"
  type        = bool
  default     = false
}

variable "enable_monitoring" {
  description = "Enable IBM Cloud Monitoring (Sysdig) for metrics and alerts"
  type        = bool
  default     = false
}

variable "enable_lifecycle_policies" {
  description = "Enable COS bucket lifecycle policies (archive, expiry, retention)"
  type        = bool
  default     = false
}
