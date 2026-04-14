# ═══════════════════════════════════════════════════════════════
# Makefile — IBM Cloud Medallion Data Pipeline
# ═══════════════════════════════════════════════════════════════
# Usage:
#   make help                  Show all available targets
#   make setup                 Install CLI + plugins + verify login
#   make infra-plan ENV=dev    Terraform plan for dev environment
#   make infra-apply ENV=dev   Terraform apply for dev environment
#   make pipeline-submit       Build + upload + submit Spark job to AE
#   make health                Run full infrastructure health check
# ═══════════════════════════════════════════════════════════════

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ── Configuration ──
ENV               ?= dev
REGION            ?= us-south
SCRIPTS_DIR       := scripts
TERRAFORM_DIR     := terraform
TEKTON_DIR        := tekton
NOTEBOOKS_DIR     := ../../transformation/notebooks/ibm-cloud
SCALA_DIR         := ../../transformation/spark-jobs/pipelines/batch-etl-scala

# ── Colors ──
CYAN  := \033[0;36m
GREEN := \033[0;32m
YELLOW:= \033[1;33m
RED   := \033[0;31m
NC    := \033[0m

# ═══════════════════════════════════════════════════════════════
# HELP
# ═══════════════════════════════════════════════════════════════
.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo "$(CYAN)  IBM Cloud — Medallion Data Pipeline$(NC)"
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-24s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "  ENV=$(ENV) (set with ENV=dev|staging|prod)"
	@echo ""

# ═══════════════════════════════════════════════════════════════
# SETUP & PREREQUISITES
# ═══════════════════════════════════════════════════════════════
.PHONY: setup
setup: ## Install IBM Cloud CLI + plugins + verify login
	@bash $(SCRIPTS_DIR)/setup.sh

.PHONY: check-env
check-env: ## Validate .env file exists and has required values
	@echo "$(CYAN)Checking environment configuration...$(NC)"
	@if [ ! -f .env ]; then \
		echo "$(RED)ERROR: .env file not found. Copy from .env.example:$(NC)"; \
		echo "  cp .env.example .env"; \
		exit 1; \
	fi
	@missing=0; \
	for var in IBMCLOUD_API_KEY COS_ACCESS_KEY COS_SECRET_KEY DB2_HOSTNAME DB2_PASSWORD AE_INSTANCE_ID; do \
		val=$$(grep "^$$var=" .env | cut -d= -f2); \
		if [ -z "$$val" ]; then \
			echo "$(RED)  MISSING: $$var$(NC)"; \
			missing=1; \
		else \
			echo "$(GREEN)  ✓ $$var$(NC)"; \
		fi; \
	done; \
	if [ $$missing -eq 1 ]; then exit 1; fi
	@echo "$(GREEN)Environment configuration OK ✔$(NC)"

.PHONY: login
login: ## Login to IBM Cloud (SSO)
	ibmcloud login --sso -r $(REGION)
	ibmcloud target -g Default

# ═══════════════════════════════════════════════════════════════
# INFRASTRUCTURE (Terraform)
# ═══════════════════════════════════════════════════════════════
.PHONY: infra-init
infra-init: ## Initialize Terraform (backend + providers)
	@echo "$(CYAN)Initializing Terraform...$(NC)"
	cd $(TERRAFORM_DIR) && terraform init

.PHONY: infra-plan
infra-plan: ## Plan infrastructure changes (ENV=dev|staging|prod)
	@echo "$(CYAN)Planning infrastructure for $(YELLOW)$(ENV)$(NC)..."
	cd $(TERRAFORM_DIR) && terraform plan \
		-var-file="environments/$(ENV).tfvars" \
		-out="plans/$(ENV).tfplan"

.PHONY: infra-apply
infra-apply: ## Apply infrastructure changes (ENV=dev|staging|prod)
	@echo "$(YELLOW)Applying infrastructure for $(ENV)...$(NC)"
	cd $(TERRAFORM_DIR) && terraform apply \
		-var-file="environments/$(ENV).tfvars" \
		-auto-approve

.PHONY: infra-destroy
infra-destroy: ## Destroy infrastructure (REQUIRES CONFIRMATION)
	@echo "$(RED)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(RED)║  DANGER: Destroying $(ENV) infrastructure    ║$(NC)"
	@echo "$(RED)╚══════════════════════════════════════════════╝$(NC)"
	@read -p "Type '$(ENV)' to confirm: " confirm && \
		[ "$$confirm" = "$(ENV)" ] || (echo "Aborted." && exit 1)
	cd $(TERRAFORM_DIR) && terraform destroy \
		-var-file="environments/$(ENV).tfvars"

.PHONY: infra-output
infra-output: ## Show Terraform outputs (sensitive values hidden)
	cd $(TERRAFORM_DIR) && terraform output

.PHONY: infra-state
infra-state: ## List all resources in Terraform state
	cd $(TERRAFORM_DIR) && terraform state list

# ═══════════════════════════════════════════════════════════════
# PIPELINE — Scala (Analytics Engine)
# ═══════════════════════════════════════════════════════════════
.PHONY: build-jar
build-jar: ## Build Scala fat JAR with sbt assembly
	@echo "$(CYAN)Building Scala fat JAR...$(NC)"
	cd $(SCALA_DIR) && sbt clean assembly

.PHONY: pipeline-submit
pipeline-submit: ## Build + Upload + Submit Spark job to Analytics Engine
	@bash $(SCRIPTS_DIR)/submit-to-ae.sh

.PHONY: pipeline-submit-skip
pipeline-submit-skip: ## Submit pre-built JAR to Analytics Engine (skip build)
	@bash $(SCRIPTS_DIR)/submit-to-ae.sh --skip-build

.PHONY: pipeline-status
pipeline-status: ## Check status of a Spark application (APP_ID=<id>)
	@if [ -z "$(APP_ID)" ]; then echo "$(RED)Usage: make pipeline-status APP_ID=<id>$(NC)"; exit 1; fi
	@bash $(SCRIPTS_DIR)/submit-to-ae.sh --status $(APP_ID)

.PHONY: pipeline-logs
pipeline-logs: ## Get logs of a Spark application (APP_ID=<id>)
	@if [ -z "$(APP_ID)" ]; then echo "$(RED)Usage: make pipeline-logs APP_ID=<id>$(NC)"; exit 1; fi
	@bash $(SCRIPTS_DIR)/submit-to-ae.sh --logs $(APP_ID)

.PHONY: pipeline-list
pipeline-list: ## List all submitted Spark applications
	@bash $(SCRIPTS_DIR)/submit-to-ae.sh --list

# ═══════════════════════════════════════════════════════════════
# PIPELINE — Python Notebooks (PySpark)
# ═══════════════════════════════════════════════════════════════
.PHONY: notebooks-all
notebooks-all: ## Run all PySpark notebooks (Bronze → Silver → Gold)
	cd $(NOTEBOOKS_DIR) && make all

.PHONY: notebooks-bronze
notebooks-bronze: ## Run Bronze layer notebook
	cd $(NOTEBOOKS_DIR) && make bronze

.PHONY: notebooks-silver
notebooks-silver: ## Run Silver layer notebook
	cd $(NOTEBOOKS_DIR) && make silver

.PHONY: notebooks-gold
notebooks-gold: ## Run Gold layer notebook
	cd $(NOTEBOOKS_DIR) && make gold

.PHONY: notebooks-status
notebooks-status: ## Show last notebook pipeline execution status
	cd $(NOTEBOOKS_DIR) && make status

# ═══════════════════════════════════════════════════════════════
# DEPLOY — IKS + CI/CD
# ═══════════════════════════════════════════════════════════════
.PHONY: deploy-spark
deploy-spark: ## Deploy Spark master/worker pods to IKS
	@bash $(SCRIPTS_DIR)/deploy-spark.sh

.PHONY: deploy-cicd
deploy-cicd: ## Configure Tekton CI/CD toolchain
	@bash $(SCRIPTS_DIR)/setup-cicd.sh

.PHONY: deploy-tekton
deploy-tekton: ## Apply Tekton pipeline definitions to IKS
	kubectl apply -f $(TEKTON_DIR)/pipeline.yaml
	kubectl apply -f $(TEKTON_DIR)/tasks.yaml
	kubectl apply -f $(TEKTON_DIR)/triggers.yaml
	@echo "$(GREEN)Tekton pipeline deployed ✔$(NC)"

# ═══════════════════════════════════════════════════════════════
# MONITORING & HEALTH
# ═══════════════════════════════════════════════════════════════
.PHONY: health
health: ## Run full infrastructure health check
	@bash $(SCRIPTS_DIR)/health-check.sh

.PHONY: health-ae
health-ae: ## Check Analytics Engine status only
	@bash $(SCRIPTS_DIR)/health-check.sh --ae

.PHONY: health-cos
health-cos: ## Check COS buckets status only
	@bash $(SCRIPTS_DIR)/health-check.sh --cos

.PHONY: health-db2
health-db2: ## Check Db2 connectivity only
	@bash $(SCRIPTS_DIR)/health-check.sh --db2

# ═══════════════════════════════════════════════════════════════
# DATA OPERATIONS
# ═══════════════════════════════════════════════════════════════
.PHONY: cos-list
cos-list: ## List contents of all COS medallion buckets
	@for bucket in datalake-raw-$(REGION) datalake-bronze-$(REGION) datalake-silver-$(REGION) datalake-gold-$(REGION); do \
		echo "$(CYAN)── $$bucket ──$(NC)"; \
		ibmcloud cos list-objects --bucket "$$bucket" --region $(REGION) 2>/dev/null | head -20 || echo "  (empty or inaccessible)"; \
		echo ""; \
	done

.PHONY: cos-usage
cos-usage: ## Show storage usage per bucket
	@for bucket in datalake-raw-$(REGION) datalake-bronze-$(REGION) datalake-silver-$(REGION) datalake-gold-$(REGION); do \
		count=$$(ibmcloud cos list-objects --bucket "$$bucket" --region $(REGION) --output json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('Contents',[])))" 2>/dev/null || echo "0"); \
		printf "  $(GREEN)%-35s$(NC) %s objects\n" "$$bucket" "$$count"; \
	done

# ═══════════════════════════════════════════════════════════════
# SECURITY
# ═══════════════════════════════════════════════════════════════
.PHONY: rotate-credentials
rotate-credentials: ## Rotate COS HMAC and Db2 credentials
	@bash $(SCRIPTS_DIR)/rotate-credentials.sh

.PHONY: audit-secrets
audit-secrets: ## Scan codebase for hardcoded secrets
	@echo "$(CYAN)Scanning for potential hardcoded secrets...$(NC)"
	@grep -rn --include="*.py" --include="*.sh" --include="*.scala" --include="*.tf" \
		-E "(password|secret|api.?key|access.?key)\s*[:=]\s*['\"][^'\"]{8,}" \
		../../ 2>/dev/null | grep -v ".env.example" | grep -v "tfvars.example" | grep -v "PLACEHOLDER" || \
		echo "$(GREEN)No hardcoded secrets found ✔$(NC)"

# ═══════════════════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════════════════
.PHONY: clean
clean: ## Remove local build artifacts and temp files
	@echo "Cleaning build artifacts..."
	rm -rf $(TERRAFORM_DIR)/.terraform $(TERRAFORM_DIR)/plans/*.tfplan
	cd $(SCALA_DIR) && sbt clean 2>/dev/null || true
	find ../../ -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find ../../ -name ".ipynb_checkpoints" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)Clean ✔$(NC)"
