# ─────────────────────────────────────────────────────────────
# Remote State Backend — IBM Cloud Object Storage
# ─────────────────────────────────────────────────────────────
# Terraform state is stored in a dedicated COS bucket for
# team collaboration and state locking.
#
# To initialize:
#   export AWS_ACCESS_KEY_ID="<cos_hmac_access_key>"
#   export AWS_SECRET_ACCESS_KEY="<cos_hmac_secret_key>"
#   terraform init
# ─────────────────────────────────────────────────────────────
terraform {
  backend "s3" {
    bucket                      = "terraform-state-data-engineer"
    key                         = "ibm-cloud/medallion-pipeline/terraform.tfstate"
    region                      = "us-south"
    endpoint                    = "https://s3.us-south.cloud-object-storage.appdomain.cloud"
    skip_region_validation      = true
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_requesting_account_id  = true
    skip_s3_checksum            = true
    force_path_style            = true
  }
}
