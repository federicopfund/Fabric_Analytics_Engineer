# ═══════════════════════════════════════════════════════════════
# Makefile — Spark Medallion K8s Operations
# ═══════════════════════════════════════════════════════════════
# Uso: make deploy | make delete | make logs | make status
# ═══════════════════════════════════════════════════════════════

NAMESPACE      := spark-medallion
CRONJOB_NAME   := spark-medallion-pipeline
KUBECTL        := kubectl
DOCKER_REPO    ?= us.icr.io/spark-medallion
IMAGE_TAG      ?= latest
IMAGE_NAME     := $(DOCKER_REPO)/spark-driver:$(IMAGE_TAG)
K8S_DIR        := kubernetes
DOCKER_DIR     := docker

.PHONY: help build push deploy delete status logs port-forward trigger secrets lint test clean

# ── Default ──
help: ## Muestra esta ayuda
	@echo "═══════════════════════════════════════════════════"
	@echo "  Spark Medallion — K8s Operations"
	@echo "═══════════════════════════════════════════════════"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ═══════════════════════════════════════════════════════════════
# Docker
# ═══════════════════════════════════════════════════════════════
build: ## Construye la imagen Docker
	docker build -t $(IMAGE_NAME) -f $(DOCKER_DIR)/Dockerfile.k8s $(DOCKER_DIR)/

push: build ## Push de la imagen a IBM Container Registry
	ibmcloud cr login
	docker push $(IMAGE_NAME)

# ═══════════════════════════════════════════════════════════════
# Kubernetes Deployment
# ═══════════════════════════════════════════════════════════════
secrets: ## Genera secrets desde variables de entorno (requiere envsubst)
	@echo "→ Generando secrets con envsubst..."
	@envsubst < $(K8S_DIR)/secrets.yaml | $(KUBECTL) apply -f -
	@echo "✓ Secrets aplicados en namespace $(NAMESPACE)"

deploy: lint ## Despliega la infraestructura completa
	@echo "═══ Deploying Spark Medallion Pipeline ═══"
	$(KUBECTL) apply -f $(K8S_DIR)/namespace.yaml
	$(KUBECTL) apply -f $(K8S_DIR)/rbac.yaml
	@echo "→ Aplicando secrets..."
	@envsubst < $(K8S_DIR)/secrets.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) apply -f $(K8S_DIR)/configmaps.yaml
	$(KUBECTL) apply -f $(K8S_DIR)/network-policy.yaml
	$(KUBECTL) apply -f $(K8S_DIR)/service.yaml
	$(KUBECTL) apply -f $(K8S_DIR)/monitoring.yaml || echo "⚠ monitoring.yaml requiere prometheus-operator (skipped)"
	$(KUBECTL) apply -f $(K8S_DIR)/cronjob.yaml
	@echo ""
	@echo "╔══════════════════════════════════════════════╗"
	@echo "║  ✓ Deploy completo — namespace: $(NAMESPACE) ║"
	@echo "╚══════════════════════════════════════════════╝"
	@$(MAKE) --no-print-directory status

delete: ## Elimina todos los recursos del namespace
	@echo "⚠  Eliminando namespace $(NAMESPACE)..."
	$(KUBECTL) delete namespace $(NAMESPACE) --ignore-not-found
	@echo "✓ Namespace eliminado"

# ═══════════════════════════════════════════════════════════════
# Operations
# ═══════════════════════════════════════════════════════════════
status: ## Estado del CronJob y pods
	@echo ""
	@echo "── CronJob ──"
	@$(KUBECTL) get cronjob $(CRONJOB_NAME) -n $(NAMESPACE) -o wide 2>/dev/null || echo "  (no encontrado)"
	@echo ""
	@echo "── Jobs recientes ──"
	@$(KUBECTL) get jobs -n $(NAMESPACE) --sort-by=.metadata.creationTimestamp -o wide 2>/dev/null | tail -5
	@echo ""
	@echo "── Pods ──"
	@$(KUBECTL) get pods -n $(NAMESPACE) --sort-by=.metadata.creationTimestamp -o wide 2>/dev/null | tail -10
	@echo ""
	@echo "── Resource Quota ──"
	@$(KUBECTL) describe resourcequota -n $(NAMESPACE) 2>/dev/null | grep -A5 "Used"

logs: ## Logs del último job ejecutado
	@POD=$$($(KUBECTL) get pods -n $(NAMESPACE) -l app=spark-medallion,component=driver \
		--sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null); \
	if [ -n "$$POD" ]; then \
		echo "── Logs de $$POD ──"; \
		$(KUBECTL) logs -n $(NAMESPACE) "$$POD" -c spark-driver --tail=200; \
	else \
		echo "No hay pods de driver disponibles"; \
	fi

logs-init: ## Logs del init container (preflight) del último pod
	@POD=$$($(KUBECTL) get pods -n $(NAMESPACE) -l app=spark-medallion,component=driver \
		--sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null); \
	if [ -n "$$POD" ]; then \
		echo "── Init logs de $$POD ──"; \
		$(KUBECTL) logs -n $(NAMESPACE) "$$POD" -c preflight-check --tail=100; \
	else \
		echo "No hay pods disponibles"; \
	fi

trigger: ## Ejecuta el CronJob manualmente (ad-hoc)
	@echo "→ Triggering manual execution..."
	$(KUBECTL) create job -n $(NAMESPACE) \
		"$(CRONJOB_NAME)-manual-$$(date +%s)" \
		--from=cronjob/$(CRONJOB_NAME)
	@echo "✓ Job manual creado. Usa 'make logs' para seguir la ejecución."

port-forward: ## Port-forward al Spark UI (localhost:4040)
	@POD=$$($(KUBECTL) get pods -n $(NAMESPACE) -l app=spark-medallion,component=driver \
		--field-selector=status.phase=Running \
		-o jsonpath='{.items[0].metadata.name}' 2>/dev/null); \
	if [ -n "$$POD" ]; then \
		echo "→ Spark UI: http://localhost:4040"; \
		$(KUBECTL) port-forward -n $(NAMESPACE) "$$POD" 4040:4040; \
	else \
		echo "No hay driver pods running. El pipeline se ejecuta cada hora."; \
	fi

# ═══════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════
lint: ## Valida los manifests con kubectl dry-run
	@echo "→ Validando manifests..."
	@for f in $(K8S_DIR)/namespace.yaml $(K8S_DIR)/rbac.yaml $(K8S_DIR)/configmaps.yaml \
		$(K8S_DIR)/network-policy.yaml $(K8S_DIR)/service.yaml $(K8S_DIR)/cronjob.yaml; do \
		echo "  ✓ $$f"; \
		$(KUBECTL) apply -f "$$f" --dry-run=client -o yaml > /dev/null 2>&1 || \
		echo "  ✗ $$f (error en validación)"; \
	done
	@echo "→ Validación completa"

test: build ## Ejecuta el contenedor localmente para pruebas
	docker run --rm -it \
		-e COS_ENDPOINT=https://s3.us-south.cloud-object-storage.appdomain.cloud \
		-e COS_ACCESS_KEY=test \
		-e COS_SECRET_KEY=test \
		$(IMAGE_NAME) \
		spark-submit --version

clean: ## Limpia jobs completados y pods terminados
	@echo "→ Limpiando jobs completados..."
	$(KUBECTL) delete jobs -n $(NAMESPACE) --field-selector status.successful=1
	@echo "→ Limpiando pods terminados..."
	$(KUBECTL) delete pods -n $(NAMESPACE) --field-selector status.phase=Succeeded
	$(KUBECTL) delete pods -n $(NAMESPACE) --field-selector status.phase=Failed
	@echo "✓ Limpieza completa"
