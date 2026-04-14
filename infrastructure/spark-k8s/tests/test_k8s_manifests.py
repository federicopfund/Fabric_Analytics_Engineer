#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════
# Test Suite — Kubernetes Deployment Validation
# Spark Medallion Pipeline on IBM IKS
#
# Ejecutar:
#   pytest tests/ -v --tb=short
#   pytest tests/ -v -k "security"     # solo tests de seguridad
#   pytest tests/ -v -k "cronjob"      # solo tests del CronJob
# ═══════════════════════════════════════════════════════════════
"""
Valida los manifests de Kubernetes para el pipeline Spark Medallion:
  - Estructura YAML correcta
  - Security hardening (non-root, capabilities, read-only)
  - RBAC mínimo privilegio
  - ResourceQuota y LimitRange
  - CronJob scheduling y concurrency
  - NetworkPolicy zero-trust
  - Probes y health checks
  - Labels y selectors consistentes
  - Secrets no contienen valores en texto plano
"""

import os
import re
from pathlib import Path

import pytest
import yaml

# ═══════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════

K8S_DIR = Path(__file__).parent.parent / "kubernetes"
DOCKER_DIR = Path(__file__).parent.parent / "docker"


def load_manifests(filepath: str) -> list[dict]:
    """Carga un archivo YAML multi-documento y retorna lista de resources."""
    full_path = K8S_DIR / filepath
    with open(full_path) as f:
        docs = list(yaml.safe_load_all(f))
    return [d for d in docs if d is not None]


@pytest.fixture(scope="module")
def namespace():
    return load_manifests("namespace.yaml")[0]


@pytest.fixture(scope="module")
def rbac_resources():
    return load_manifests("rbac.yaml")


@pytest.fixture(scope="module")
def secrets():
    return load_manifests("secrets.yaml")


@pytest.fixture(scope="module")
def configmaps():
    return load_manifests("configmaps.yaml")


@pytest.fixture(scope="module")
def cronjob():
    return load_manifests("cronjob.yaml")[0]


@pytest.fixture(scope="module")
def network_policies():
    return load_manifests("network-policy.yaml")


@pytest.fixture(scope="module")
def service():
    return load_manifests("service.yaml")[0]


@pytest.fixture(scope="module")
def monitoring():
    return load_manifests("monitoring.yaml")


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

def find_resource(resources: list[dict], kind: str, name: str = None) -> dict:
    """Busca un resource por kind y opcionalmente por name."""
    for r in resources:
        if r["kind"] == kind:
            if name is None or r["metadata"]["name"] == name:
                return r
    return None


def get_pod_spec(cronjob_doc: dict) -> dict:
    """Extrae el pod spec del CronJob."""
    return cronjob_doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]


def get_driver_container(cronjob_doc: dict) -> dict:
    """Extrae el container principal (spark-driver) del CronJob."""
    pod_spec = get_pod_spec(cronjob_doc)
    for c in pod_spec["containers"]:
        if c["name"] == "spark-driver":
            return c
    return pod_spec["containers"][0]


# ═══════════════════════════════════════════════════════════════
# 1. MANIFEST STRUCTURE — Archivos y YAML válido
# ═══════════════════════════════════════════════════════════════

class TestManifestStructure:
    """Valida que todos los archivos existen y son YAML válido."""

    REQUIRED_MANIFESTS = [
        "namespace.yaml",
        "rbac.yaml",
        "secrets.yaml",
        "configmaps.yaml",
        "cronjob.yaml",
        "network-policy.yaml",
        "service.yaml",
        "monitoring.yaml",
    ]

    @pytest.mark.parametrize("manifest", REQUIRED_MANIFESTS)
    def test_manifest_exists(self, manifest):
        """Verifica que cada manifest requerido existe."""
        path = K8S_DIR / manifest
        assert path.exists(), f"Missing manifest: {manifest}"

    @pytest.mark.parametrize("manifest", REQUIRED_MANIFESTS)
    def test_manifest_valid_yaml(self, manifest):
        """Verifica que cada manifest es YAML válido."""
        docs = load_manifests(manifest)
        assert len(docs) > 0, f"{manifest} is empty"
        for doc in docs:
            assert "apiVersion" in doc, f"{manifest}: missing apiVersion"
            assert "kind" in doc, f"{manifest}: missing kind"
            assert "metadata" in doc, f"{manifest}: missing metadata"

    def test_dockerfile_exists(self):
        """Verifica que el Dockerfile.k8s existe."""
        assert (DOCKER_DIR / "Dockerfile.k8s").exists()

    def test_entrypoint_exists(self):
        """Verifica que el entrypoint.sh existe."""
        assert (DOCKER_DIR / "entrypoint.sh").exists()

    def test_entrypoint_has_shebang(self):
        """Verifica que entrypoint.sh tiene shebang correcto."""
        content = (DOCKER_DIR / "entrypoint.sh").read_text()
        assert content.startswith("#!/bin/bash"), "entrypoint.sh missing #!/bin/bash"

    def test_all_resources_have_namespace(self):
        """Verifica que todos los namespaced resources declaran namespace."""
        CLUSTER_SCOPED = {"Namespace", "ClusterRole", "ClusterRoleBinding"}
        for manifest in self.REQUIRED_MANIFESTS:
            for doc in load_manifests(manifest):
                if doc["kind"] not in CLUSTER_SCOPED:
                    ns = doc.get("metadata", {}).get("namespace")
                    assert ns == "spark-medallion", (
                        f"{manifest}/{doc['kind']}/{doc['metadata']['name']}: "
                        f"namespace should be 'spark-medallion', got '{ns}'"
                    )


# ═══════════════════════════════════════════════════════════════
# 2. NAMESPACE
# ═══════════════════════════════════════════════════════════════

class TestNamespace:

    def test_name(self, namespace):
        assert namespace["metadata"]["name"] == "spark-medallion"

    def test_labels_present(self, namespace):
        labels = namespace["metadata"]["labels"]
        assert "app.kubernetes.io/part-of" in labels
        assert labels["environment"] == "production"


# ═══════════════════════════════════════════════════════════════
# 3. RBAC — Mínimo privilegio
# ═══════════════════════════════════════════════════════════════

class TestRBAC:

    def test_service_account_exists(self, rbac_resources):
        sa = find_resource(rbac_resources, "ServiceAccount", "spark-driver")
        assert sa is not None, "ServiceAccount spark-driver not found"

    def test_role_exists(self, rbac_resources):
        role = find_resource(rbac_resources, "Role", "spark-driver-role")
        assert role is not None, "Role spark-driver-role not found"

    def test_role_no_wildcard_verbs(self, rbac_resources):
        """RBAC no debe tener verbs: ['*'] — mínimo privilegio."""
        role = find_resource(rbac_resources, "Role", "spark-driver-role")
        for rule in role["rules"]:
            assert "*" not in rule["verbs"], (
                f"Role has wildcard verbs on {rule['resources']}"
            )

    def test_role_no_wildcard_resources(self, rbac_resources):
        """RBAC no debe tener resources: ['*']."""
        role = find_resource(rbac_resources, "Role", "spark-driver-role")
        for rule in role["rules"]:
            assert "*" not in rule["resources"], "Role has wildcard resources"

    def test_role_no_cluster_admin(self, rbac_resources):
        """No debe haber ClusterRoleBinding a cluster-admin."""
        for r in rbac_resources:
            if r["kind"] == "ClusterRoleBinding":
                ref = r.get("roleRef", {})
                assert ref.get("name") != "cluster-admin", (
                    "CRITICAL: ClusterRoleBinding to cluster-admin detected"
                )

    def test_role_scoped_to_namespace(self, rbac_resources):
        """Role (no ClusterRole) para scope a namespace."""
        role = find_resource(rbac_resources, "Role", "spark-driver-role")
        assert role is not None, "Should use Role (not ClusterRole)"

    def test_rolebinding_references_correct_sa(self, rbac_resources):
        rb = find_resource(rbac_resources, "RoleBinding", "spark-driver-binding")
        assert rb is not None
        subjects = rb["subjects"]
        assert any(
            s["kind"] == "ServiceAccount" and s["name"] == "spark-driver"
            for s in subjects
        )

    def test_resource_quota_exists(self, rbac_resources):
        quota = find_resource(rbac_resources, "ResourceQuota")
        assert quota is not None, "ResourceQuota not found"

    def test_resource_quota_has_limits(self, rbac_resources):
        quota = find_resource(rbac_resources, "ResourceQuota")
        hard = quota["spec"]["hard"]
        assert "requests.cpu" in hard
        assert "requests.memory" in hard
        assert "pods" in hard

    def test_limit_range_exists(self, rbac_resources):
        lr = find_resource(rbac_resources, "LimitRange")
        assert lr is not None, "LimitRange not found"

    def test_limit_range_has_defaults(self, rbac_resources):
        lr = find_resource(rbac_resources, "LimitRange")
        container_limit = next(
            (l for l in lr["spec"]["limits"] if l["type"] == "Container"), None
        )
        assert container_limit is not None
        assert "default" in container_limit
        assert "defaultRequest" in container_limit


# ═══════════════════════════════════════════════════════════════
# 4. SECRETS — No plaintext, template pattern
# ═══════════════════════════════════════════════════════════════

class TestSecrets:

    def test_cos_credentials_exist(self, secrets):
        s = find_resource(secrets, "Secret", "cos-credentials")
        assert s is not None

    def test_db2_credentials_exist(self, secrets):
        s = find_resource(secrets, "Secret", "db2-credentials")
        assert s is not None

    def test_secrets_use_string_data(self, secrets):
        """Secrets deben usar stringData (template) no data (base64)."""
        for s in secrets:
            if s["kind"] == "Secret":
                assert "stringData" in s, (
                    f"Secret {s['metadata']['name']} should use stringData"
                )

    def test_secrets_are_templates(self, secrets):
        """Secrets deben contener ${} placeholders, no valores reales."""
        for s in secrets:
            if s["kind"] != "Secret":
                continue
            for key, value in s.get("stringData", {}).items():
                # Endpoints hardcodeados son válidos
                if "appdomain.cloud" in str(value):
                    continue
                # Los templates deben tener ${...} o ser valores estáticos conocidos
                if "${" in str(value):
                    continue
                # Valores estáticos cortos (puertos, nombres db, flags) son OK
                if str(value) in ("true", "false") or str(value).isdigit():
                    continue
                # Verificar que no sea un valor real (> 20 chars sin template)
                assert len(str(value)) < 30 or "${" in str(value), (
                    f"Secret {s['metadata']['name']}.{key} may contain "
                    f"a hardcoded credential: '{value[:20]}...'"
                )

    def test_secrets_type_opaque(self, secrets):
        for s in secrets:
            if s["kind"] == "Secret":
                assert s["type"] == "Opaque"

    def test_db2_has_ssl(self, secrets):
        """Db2 credentials deben incluir SSL en JDBC URL."""
        s = find_resource(secrets, "Secret", "db2-credentials")
        jdbc = s["stringData"].get("jdbc-url", "")
        assert "sslConnection=true" in jdbc, "Db2 JDBC URL missing SSL"


# ═══════════════════════════════════════════════════════════════
# 5. CRONJOB — Scheduling, concurrency, containers
# ═══════════════════════════════════════════════════════════════

class TestCronJob:

    def test_kind(self, cronjob):
        assert cronjob["kind"] == "CronJob"

    def test_schedule_is_valid_cron(self, cronjob):
        schedule = cronjob["spec"]["schedule"]
        parts = schedule.split()
        assert len(parts) == 5, f"Invalid cron expression: {schedule}"

    def test_concurrency_policy_forbid(self, cronjob):
        """No debe haber ejecuciones superpuestas."""
        assert cronjob["spec"]["concurrencyPolicy"] == "Forbid"

    def test_starting_deadline(self, cronjob):
        """Debe tener deadline para evitar storm de jobs atrasados."""
        assert "startingDeadlineSeconds" in cronjob["spec"]
        assert cronjob["spec"]["startingDeadlineSeconds"] <= 600

    def test_history_limits(self, cronjob):
        assert cronjob["spec"]["successfulJobsHistoryLimit"] >= 1
        assert cronjob["spec"]["failedJobsHistoryLimit"] >= 1

    def test_ttl_cleanup(self, cronjob):
        job_spec = cronjob["spec"]["jobTemplate"]["spec"]
        assert "ttlSecondsAfterFinished" in job_spec
        assert job_spec["ttlSecondsAfterFinished"] > 0

    def test_backoff_limit(self, cronjob):
        job_spec = cronjob["spec"]["jobTemplate"]["spec"]
        assert job_spec["backoffLimit"] <= 3, "Too many retries"

    def test_active_deadline(self, cronjob):
        """Timeout para evitar jobs stuck eternamente."""
        job_spec = cronjob["spec"]["jobTemplate"]["spec"]
        assert "activeDeadlineSeconds" in job_spec
        assert job_spec["activeDeadlineSeconds"] <= 3600

    def test_restart_policy_never(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        assert pod_spec["restartPolicy"] == "Never"

    def test_service_account(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        assert pod_spec["serviceAccountName"] == "spark-driver"

    def test_init_container_exists(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        assert "initContainers" in pod_spec
        assert len(pod_spec["initContainers"]) >= 1

    def test_init_container_runs_preflight(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        init = pod_spec["initContainers"][0]
        cmd = " ".join(init.get("command", []) + init.get("args", []))
        assert "preflight" in cmd.lower()

    def test_driver_container_exists(self, cronjob):
        driver = get_driver_container(cronjob)
        assert driver is not None

    def test_driver_uses_spark_submit(self, cronjob):
        driver = get_driver_container(cronjob)
        cmd = " ".join(driver.get("command", []) + driver.get("args", []))
        assert "spark-submit" in cmd

    def test_driver_class_is_pipeline(self, cronjob):
        driver = get_driver_container(cronjob)
        args = driver.get("args", [])
        assert "medallion.Pipeline" in args

    def test_driver_deploy_mode_client(self, cronjob):
        """En K8s, el pod ES el driver → deploy-mode=client."""
        driver = get_driver_container(cronjob)
        args = driver.get("args", [])
        idx = args.index("--deploy-mode") if "--deploy-mode" in args else -1
        assert idx >= 0 and args[idx + 1] == "client"

    def test_dynamic_allocation_enabled(self, cronjob):
        driver = get_driver_container(cronjob)
        args = " ".join(driver.get("args", []))
        assert "dynamicAllocation.enabled=true" in args

    def test_driver_has_resource_limits(self, cronjob):
        driver = get_driver_container(cronjob)
        res = driver.get("resources", {})
        assert "requests" in res, "Driver missing resource requests"
        assert "limits" in res, "Driver missing resource limits"

    def test_init_container_has_resource_limits(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        init = pod_spec["initContainers"][0]
        res = init.get("resources", {})
        assert "limits" in res, "Init container missing resource limits"

    def test_driver_exposes_ports(self, cronjob):
        driver = get_driver_container(cronjob)
        ports = {p["containerPort"] for p in driver.get("ports", [])}
        assert 4040 in ports, "Spark UI port 4040 not exposed"
        assert 7078 in ports, "Driver RPC port 7078 not exposed"

    def test_volumes_defined(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        volume_names = {v["name"] for v in pod_spec.get("volumes", [])}
        assert "spark-work" in volume_names
        assert "spark-conf" in volume_names
        assert "scripts" in volume_names

    def test_image_pull_secret(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        secrets = pod_spec.get("imagePullSecrets", [])
        assert len(secrets) >= 1, "Missing imagePullSecrets for ICR"

    def test_termination_grace_period(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        assert pod_spec.get("terminationGracePeriodSeconds", 30) >= 30


# ═══════════════════════════════════════════════════════════════
# 6. SECURITY — Container hardening
# ═══════════════════════════════════════════════════════════════

class TestSecurity:

    def test_driver_runs_as_non_root(self, cronjob):
        driver = get_driver_container(cronjob)
        sc = driver.get("securityContext", {})
        assert sc.get("runAsNonRoot") is True, "Driver must run as non-root"

    def test_driver_specific_uid(self, cronjob):
        driver = get_driver_container(cronjob)
        sc = driver.get("securityContext", {})
        assert sc.get("runAsUser") == 185, "Driver should run as UID 185 (spark)"

    def test_driver_no_privilege_escalation(self, cronjob):
        driver = get_driver_container(cronjob)
        sc = driver.get("securityContext", {})
        assert sc.get("allowPrivilegeEscalation") is False

    def test_driver_drops_all_capabilities(self, cronjob):
        driver = get_driver_container(cronjob)
        sc = driver.get("securityContext", {})
        caps = sc.get("capabilities", {})
        assert "ALL" in caps.get("drop", []), "Should drop ALL capabilities"

    def test_no_privileged_containers(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        all_containers = (
            pod_spec.get("containers", []) +
            pod_spec.get("initContainers", [])
        )
        for c in all_containers:
            sc = c.get("securityContext", {})
            assert sc.get("privileged") is not True, (
                f"Container {c['name']} should not be privileged"
            )

    def test_no_host_network(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        assert pod_spec.get("hostNetwork") is not True

    def test_no_host_pid(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        assert pod_spec.get("hostPID") is not True

    def test_jvm_opens_for_spark(self, cronjob):
        """JDK 11+ requiere --add-opens para Spark."""
        driver = get_driver_container(cronjob)
        env_vars = {e["name"]: e.get("value", "") for e in driver.get("env", [])}
        java_opts = env_vars.get("JAVA_TOOL_OPTIONS", "")
        assert "--add-opens" in java_opts, "Missing JDK 11+ --add-opens"

    def test_db2_ssl_enabled(self, cronjob):
        driver = get_driver_container(cronjob)
        env_vars = {e["name"]: e.get("value", "") for e in driver.get("env", [])}
        assert env_vars.get("DB2_SSL") == "true", "Db2 SSL should be enabled"

    def test_secrets_not_in_env_value(self, cronjob):
        """Credentials deben venir de secretKeyRef, no hardcodeadas."""
        driver = get_driver_container(cronjob)
        SENSITIVE_KEYS = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                          "DB2_PASSWORD", "DB2_USERNAME"}
        for env in driver.get("env", []):
            if env["name"] in SENSITIVE_KEYS:
                assert "valueFrom" in env, (
                    f"Sensitive env {env['name']} must use valueFrom/secretKeyRef"
                )
                assert "secretKeyRef" in env["valueFrom"], (
                    f"Sensitive env {env['name']} must use secretKeyRef"
                )


# ═══════════════════════════════════════════════════════════════
# 7. HEALTH CHECKS — Probes
# ═══════════════════════════════════════════════════════════════

class TestHealthChecks:

    def test_startup_probe_exists(self, cronjob):
        driver = get_driver_container(cronjob)
        assert "startupProbe" in driver, "Missing startupProbe"

    def test_liveness_probe_exists(self, cronjob):
        driver = get_driver_container(cronjob)
        assert "livenessProbe" in driver, "Missing livenessProbe"

    def test_startup_probe_has_initial_delay(self, cronjob):
        driver = get_driver_container(cronjob)
        probe = driver["startupProbe"]
        assert probe.get("initialDelaySeconds", 0) >= 10, (
            "Startup probe needs initial delay for Spark init"
        )

    def test_liveness_uses_healthcheck_script(self, cronjob):
        driver = get_driver_container(cronjob)
        probe = driver["livenessProbe"]
        cmd = " ".join(probe.get("exec", {}).get("command", []))
        assert "healthcheck" in cmd.lower()


# ═══════════════════════════════════════════════════════════════
# 8. NETWORK POLICY — Zero-trust
# ═══════════════════════════════════════════════════════════════

class TestNetworkPolicy:

    def test_driver_policy_exists(self, network_policies):
        np = find_resource(network_policies, "NetworkPolicy", "spark-driver-policy")
        assert np is not None

    def test_executor_policy_exists(self, network_policies):
        np = find_resource(network_policies, "NetworkPolicy", "spark-executor-policy")
        assert np is not None

    def test_driver_has_ingress_and_egress(self, network_policies):
        np = find_resource(network_policies, "NetworkPolicy", "spark-driver-policy")
        types = np["spec"]["policyTypes"]
        assert "Ingress" in types
        assert "Egress" in types

    def test_executor_has_ingress_and_egress(self, network_policies):
        np = find_resource(network_policies, "NetworkPolicy", "spark-executor-policy")
        types = np["spec"]["policyTypes"]
        assert "Ingress" in types
        assert "Egress" in types

    def test_driver_allows_dns(self, network_policies):
        """Driver debe poder resolver DNS."""
        np = find_resource(network_policies, "NetworkPolicy", "spark-driver-policy")
        egress_ports = []
        for rule in np["spec"].get("egress", []):
            for p in rule.get("ports", []):
                egress_ports.append(p.get("port"))
        assert 53 in egress_ports, "Driver needs DNS egress (port 53)"

    def test_driver_allows_db2(self, network_policies):
        """Driver debe poder conectar a Db2."""
        np = find_resource(network_policies, "NetworkPolicy", "spark-driver-policy")
        egress_ports = []
        for rule in np["spec"].get("egress", []):
            for p in rule.get("ports", []):
                egress_ports.append(p.get("port"))
        assert 30376 in egress_ports, "Driver needs Db2 egress (port 30376)"

    def test_executor_cannot_reach_db2(self, network_policies):
        """Executors NO deben tener acceso directo a Db2."""
        np = find_resource(network_policies, "NetworkPolicy", "spark-executor-policy")
        egress_ports = []
        for rule in np["spec"].get("egress", []):
            for p in rule.get("ports", []):
                egress_ports.append(p.get("port"))
        assert 30376 not in egress_ports, (
            "Executors should NOT have direct Db2 access"
        )

    def test_executor_ingress_only_from_driver(self, network_policies):
        """Executor ingress solo desde driver pods."""
        np = find_resource(network_policies, "NetworkPolicy", "spark-executor-policy")
        for rule in np["spec"].get("ingress", []):
            for source in rule.get("from", []):
                if "podSelector" in source:
                    labels = source["podSelector"].get("matchLabels", {})
                    assert labels.get("spark-role") == "driver"


# ═══════════════════════════════════════════════════════════════
# 9. SERVICE
# ═══════════════════════════════════════════════════════════════

class TestService:

    def test_type_clusterip(self, service):
        """Service debe ser ClusterIP (no LoadBalancer/NodePort)."""
        assert service["spec"]["type"] == "ClusterIP"

    def test_exposes_spark_ui(self, service):
        ports = {p["name"]: p["port"] for p in service["spec"]["ports"]}
        assert ports.get("spark-ui") == 4040

    def test_selector_matches_driver(self, service):
        selector = service["spec"]["selector"]
        assert selector.get("spark-role") == "driver"

    def test_has_prometheus_annotations(self, service):
        annotations = service["metadata"].get("annotations", {})
        assert annotations.get("prometheus.io/scrape") == "true"


# ═══════════════════════════════════════════════════════════════
# 10. MONITORING — Prometheus resources
# ═══════════════════════════════════════════════════════════════

class TestMonitoring:

    def test_service_monitor_exists(self, monitoring):
        sm = find_resource(monitoring, "ServiceMonitor")
        assert sm is not None, "ServiceMonitor not found"

    def test_pod_monitor_exists(self, monitoring):
        pm = find_resource(monitoring, "PodMonitor")
        assert pm is not None, "PodMonitor not found"

    def test_prometheus_rule_exists(self, monitoring):
        pr = find_resource(monitoring, "PrometheusRule")
        assert pr is not None, "PrometheusRule not found"

    def test_alerts_have_severity(self, monitoring):
        pr = find_resource(monitoring, "PrometheusRule")
        for group in pr["spec"]["groups"]:
            for rule in group["rules"]:
                if "alert" in rule:
                    assert "severity" in rule.get("labels", {}), (
                        f"Alert {rule['alert']} missing severity label"
                    )

    def test_critical_alerts_exist(self, monitoring):
        """Deben existir alertas para job failed y crash-loop."""
        pr = find_resource(monitoring, "PrometheusRule")
        alert_names = set()
        for group in pr["spec"]["groups"]:
            for rule in group["rules"]:
                if "alert" in rule:
                    alert_names.add(rule["alert"])
        assert "SparkJobFailed" in alert_names
        assert "SparkPodCrashLooping" in alert_names

    def test_scrape_interval_reasonable(self, monitoring):
        sm = find_resource(monitoring, "ServiceMonitor")
        for endpoint in sm["spec"]["endpoints"]:
            interval = endpoint.get("interval", "30s")
            seconds = int(interval.replace("s", ""))
            assert 10 <= seconds <= 120, f"Scrape interval {interval} unreasonable"


# ═══════════════════════════════════════════════════════════════
# 11. LABELS & SELECTORS CONSISTENCY
# ═══════════════════════════════════════════════════════════════

class TestLabelsConsistency:

    PART_OF_LABEL = "app.kubernetes.io/part-of"
    EXPECTED_PART_OF = "medallion-pipeline"

    def test_all_resources_have_part_of_label(self):
        """Todos los resources deben tener app.kubernetes.io/part-of."""
        for manifest in TestManifestStructure.REQUIRED_MANIFESTS:
            for doc in load_manifests(manifest):
                labels = doc.get("metadata", {}).get("labels", {})
                assert self.PART_OF_LABEL in labels, (
                    f"{manifest}/{doc['kind']}/{doc['metadata']['name']} "
                    f"missing {self.PART_OF_LABEL} label"
                )

    def test_service_selector_matches_cronjob_labels(self, service, cronjob):
        """Service selector debe coincidir con labels del pod template del CronJob."""
        svc_selector = service["spec"]["selector"]
        pod_labels = (
            cronjob["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["labels"]
        )
        for key, value in svc_selector.items():
            assert pod_labels.get(key) == value, (
                f"Service selector {key}={value} doesn't match CronJob pod labels"
            )


# ═══════════════════════════════════════════════════════════════
# 12. CONFIGMAPS
# ═══════════════════════════════════════════════════════════════

class TestConfigMaps:

    def test_spark_config_exists(self, configmaps):
        cm = find_resource(configmaps, "ConfigMap", "spark-config")
        assert cm is not None

    def test_spark_scripts_exists(self, configmaps):
        cm = find_resource(configmaps, "ConfigMap", "spark-scripts")
        assert cm is not None

    def test_spark_defaults_has_s3a(self, configmaps):
        cm = find_resource(configmaps, "ConfigMap", "spark-config")
        defaults = cm["data"].get("spark-defaults.conf", "")
        assert "fs.s3a" in defaults, "spark-defaults.conf missing S3A config"

    def test_spark_defaults_has_delta(self, configmaps):
        cm = find_resource(configmaps, "ConfigMap", "spark-config")
        defaults = cm["data"].get("spark-defaults.conf", "")
        assert "delta" in defaults.lower(), "spark-defaults.conf missing Delta config"

    def test_preflight_script_exists(self, configmaps):
        cm = find_resource(configmaps, "ConfigMap", "spark-scripts")
        assert "preflight.sh" in cm["data"]

    def test_healthcheck_script_exists(self, configmaps):
        cm = find_resource(configmaps, "ConfigMap", "spark-scripts")
        assert "healthcheck.sh" in cm["data"]

    def test_preflight_validates_jar(self, configmaps):
        cm = find_resource(configmaps, "ConfigMap", "spark-scripts")
        preflight = cm["data"]["preflight.sh"]
        assert "jar" in preflight.lower() or "JAR" in preflight


# ═══════════════════════════════════════════════════════════════
# 13. DOCKERFILE VALIDATION
# ═══════════════════════════════════════════════════════════════

class TestDockerfile:

    @pytest.fixture(scope="class")
    def dockerfile_content(self):
        return (DOCKER_DIR / "Dockerfile.k8s").read_text()

    def test_multi_stage_build(self, dockerfile_content):
        """Debe ser multi-stage para optimizar imagen."""
        assert dockerfile_content.count("FROM ") >= 2, "Should be multi-stage"

    def test_uses_jdk_11(self, dockerfile_content):
        """Spark 3.3.1 requiere JDK 11."""
        assert "11" in dockerfile_content
        assert "temurin" in dockerfile_content.lower() or "openjdk" in dockerfile_content.lower()

    def test_non_root_user(self, dockerfile_content):
        """Debe ejecutar como usuario non-root."""
        assert "USER" in dockerfile_content
        assert "root" not in dockerfile_content.split("USER")[-1].split("\n")[0].lower()

    def test_tini_init(self, dockerfile_content):
        """Debe usar tini como PID 1 para signal handling."""
        assert "tini" in dockerfile_content.lower()

    def test_spark_version(self, dockerfile_content):
        """Debe usar Spark 3.3.1."""
        assert "3.3.1" in dockerfile_content

    def test_exposes_required_ports(self, dockerfile_content):
        assert "4040" in dockerfile_content
        assert "7078" in dockerfile_content

    def test_has_labels(self, dockerfile_content):
        assert "LABEL" in dockerfile_content


# ═══════════════════════════════════════════════════════════════
# 14. CROSS-CUTTING CONCERNS
# ═══════════════════════════════════════════════════════════════

class TestCrossCutting:

    def test_image_consistent_across_manifests(self, cronjob):
        """Init container y main container deben usar la misma imagen."""
        pod_spec = get_pod_spec(cronjob)
        driver = get_driver_container(cronjob)
        init = pod_spec["initContainers"][0]
        assert driver["image"] == init["image"], (
            "Driver and init container should use the same image"
        )

    def test_no_latest_tag_warning(self, cronjob):
        """latest tag es peligroso en producción — advertencia."""
        driver = get_driver_container(cronjob)
        if ":latest" in driver.get("image", ""):
            import warnings
            warnings.warn(
                "Image uses :latest tag. Use specific version tags in production.",
                UserWarning,
            )

    def test_pod_anti_affinity(self, cronjob):
        """Drivers no deben colocarse en el mismo nodo."""
        pod_spec = get_pod_spec(cronjob)
        affinity = pod_spec.get("affinity", {})
        anti = affinity.get("podAntiAffinity", {})
        assert (
            "requiredDuringSchedulingIgnoredDuringExecution" in anti or
            "preferredDuringSchedulingIgnoredDuringExecution" in anti
        ), "Missing pod anti-affinity rule"

    def test_tolerations_defined(self, cronjob):
        pod_spec = get_pod_spec(cronjob)
        assert len(pod_spec.get("tolerations", [])) >= 1

    def test_configmap_referenced_in_cronjob(self, cronjob):
        """CronJob debe montar el ConfigMap de spark-config."""
        pod_spec = get_pod_spec(cronjob)
        volume_cms = set()
        for v in pod_spec.get("volumes", []):
            if "configMap" in v:
                volume_cms.add(v["configMap"]["name"])
        assert "spark-config" in volume_cms
        assert "spark-scripts" in volume_cms
