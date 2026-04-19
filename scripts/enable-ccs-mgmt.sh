#!/usr/bin/env bash
# enable-ccs-mgmt.sh
# Bring up the ONT management cluster on a fresh Talos Linux install.
# Usage: ./enable-ccs-mgmt.sh [options]
# Options:
#   --kubeconfig PATH     Path to kubeconfig (default: ~/.kube/config)
#   --compiled-dir PATH   Path to compiled enable directory
#                         (default: ../lab/configs/ccs-mgmt/compiled)
#   --registry HOST:PORT  Local OCI registry (default: 10.20.0.1:5000)
#   --dsns-ip IP          DSNS LoadBalancer IP (default: 10.20.0.240)
#   --dry-run             Print kubectl commands without executing
#   -h, --help            Show this help

set -euo pipefail

SCRIPT_START=$(date +%s)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# SECTION 2: Defaults and argument parsing
# ---------------------------------------------------------------------------

KUBECONFIG="${HOME}/.kube/config"
COMPILED_DIR="${SCRIPT_DIR}/../lab/configs/ccs-mgmt/compiled"
REGISTRY="10.20.0.1:5000"
DSNS_IP="10.20.0.240"
DRY_RUN=false

usage() {
  sed -n '/^# enable-ccs-mgmt/,/^#   -h/p' "$0" | sed 's/^# \{0,2\}//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kubeconfig)    KUBECONFIG="$2";   shift 2 ;;
    --compiled-dir)  COMPILED_DIR="$2"; shift 2 ;;
    --registry)      REGISTRY="$2";     shift 2 ;;
    --dsns-ip)       DSNS_IP="$2";      shift 2 ;;
    --dry-run)       DRY_RUN=true;      shift   ;;
    -h|--help)       usage              ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
done

COMPILED_DIR="$(realpath "${COMPILED_DIR}")"
ENABLE_DIR="${COMPILED_DIR}/enable"

export KUBECONFIG

KUBECTL="kubectl"
if [[ "${DRY_RUN}" == "true" ]]; then
  KUBECTL="echo kubectl"
fi

# ---------------------------------------------------------------------------
# SECTION 3: Helper functions
# ---------------------------------------------------------------------------

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_step() {
  echo ""
  echo -e "${BLUE}==> [$(date '+%H:%M:%S')] $*${NC}"
}

log_ok() {
  echo -e "${GREEN}    [OK] $*${NC}"
}

log_err() {
  echo -e "${RED}    [ERR] $*${NC}" >&2
  exit 1
}

wait_ready() {
  local namespace="$1"
  local deployment="$2"
  local timeout="${3:-120}"
  local elapsed=0
  local interval=5
  local desired available
  log_step "Waiting for deployment ${deployment} in ${namespace} (timeout: ${timeout}s)"
  while true; do
    desired=$(kubectl get deployment "${deployment}" -n "${namespace}" \
      -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
    available=$(kubectl get deployment "${deployment}" -n "${namespace}" \
      -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo "0")
    desired="${desired:-0}"
    available="${available:-0}"
    if [[ "${desired}" -gt 0 && "${available}" -ge "${desired}" ]]; then
      log_ok "${deployment} ready (${available}/${desired})"
      return 0
    fi
    if [[ "${elapsed}" -ge "${timeout}" ]]; then
      log_err "Timeout waiting for deployment ${deployment} in ${namespace} after ${timeout}s"
    fi
    sleep "${interval}"
    elapsed=$((elapsed + interval))
    echo "    ... ${deployment}: ${available:-0}/${desired:-0} available (${elapsed}s)"
  done
}

wait_crd() {
  local crd_name="$1"
  local timeout="${2:-120}"
  local elapsed=0
  local interval=5
  local status
  log_step "Waiting for CRD ${crd_name} (timeout: ${timeout}s)"
  while true; do
    status=$(kubectl get crd "${crd_name}" \
      -o jsonpath='{.status.conditions[?(@.type=="Established")].status}' \
      2>/dev/null || echo "")
    if [[ "${status}" == "True" ]]; then
      log_ok "CRD ${crd_name} established"
      return 0
    fi
    if [[ "${elapsed}" -ge "${timeout}" ]]; then
      log_err "Timeout waiting for CRD ${crd_name} after ${timeout}s"
    fi
    sleep "${interval}"
    elapsed=$((elapsed + interval))
    echo "    ... ${crd_name}: not yet established (${elapsed}s)"
  done
}

# ---------------------------------------------------------------------------
# SECTION 4: Phase apply function
# ---------------------------------------------------------------------------

apply_phase() {
  local phase_dir="$1"
  local phase_name
  phase_name="$(basename "${phase_dir}")"
  log_step "Applying phase: ${phase_name}"
  if [[ ! -d "${phase_dir}" ]]; then
    log_err "Phase directory not found: ${phase_dir}"
  fi
  local found=0
  for yaml_file in "${phase_dir}"/*.yaml; do
    [[ -e "${yaml_file}" ]] || continue
    local filename
    filename="$(basename "${yaml_file}")"
    if [[ "${filename}" == "phase-meta.yaml" ]]; then
      continue
    fi
    echo "    Applying ${filename}"
    ${KUBECTL} apply -f "${yaml_file}" --server-side=false \
      || log_err "Failed to apply ${yaml_file}"
    found=$((found + 1))
  done
  if [[ "${found}" -eq 0 ]]; then
    echo "    (no manifests in phase)"
  fi
  log_ok "Phase ${phase_name} applied (${found} files)"
}

# ---------------------------------------------------------------------------
# SECTION 5: Pre-flight checks
# ---------------------------------------------------------------------------

log_step "Pre-flight checks"

if [[ ! -d "${COMPILED_DIR}" ]]; then
  log_err "Compiled directory not found: ${COMPILED_DIR}"
fi

if [[ ! -d "${ENABLE_DIR}" ]]; then
  log_err "Enable directory not found: ${ENABLE_DIR}"
fi

for phase in 00a-namespaces 00-infrastructure-dependencies 01-guardian-bootstrap \
             02-guardian-deploy 03-platform-wrapper 04-conductor 05-post-bootstrap; do
  if [[ ! -d "${ENABLE_DIR}/${phase}" ]]; then
    log_err "Expected phase directory missing: ${ENABLE_DIR}/${phase}"
  fi
done

for infra_file in cert-manager.yaml cnpg-operator.yaml kueue-controller.yaml kueue-quota.yaml; do
  if [[ ! -f "${COMPILED_DIR}/${infra_file}" ]]; then
    log_err "Expected infrastructure file missing: ${COMPILED_DIR}/${infra_file}"
  fi
done

if ! command -v kubectl &>/dev/null; then
  log_err "kubectl not found in PATH"
fi

if [[ ! -f "${KUBECONFIG}" ]]; then
  log_err "kubeconfig not found: ${KUBECONFIG}"
fi

if [[ "${DRY_RUN}" == "false" ]]; then
  kubectl cluster-info || log_err "Cannot reach cluster -- check kubeconfig and cluster availability"
else
  echo "kubectl cluster-info  # skipped in dry-run"
fi

log_ok "Pre-flight checks passed"
log_ok "  Compiled dir:  ${COMPILED_DIR}"
log_ok "  Registry:      ${REGISTRY}"
log_ok "  DSNS IP:       ${DSNS_IP}"
log_ok "  Dry-run:       ${DRY_RUN}"

# ---------------------------------------------------------------------------
# SECTION 6: Phase sequence
# ---------------------------------------------------------------------------

declare -A PHASE_STATUS

# Phase 00a: namespaces
apply_phase "${ENABLE_DIR}/00a-namespaces"
PHASE_STATUS["00a-namespaces"]="applied"

# Phase 00: infrastructure dependencies
# cert-manager, CNPG, and Kueue are in the compiled root, not in the phase
# directory. The phase directory contains only a prerequisites documentation
# ConfigMap. Install the operators first, then apply the phase.
log_step "Phase 00: infrastructure dependencies (cert-manager, CNPG, Kueue)"
${KUBECTL} apply -f "${COMPILED_DIR}/cert-manager.yaml" --server-side=false \
  || log_err "Failed to apply cert-manager.yaml"
${KUBECTL} apply -f "${COMPILED_DIR}/cnpg-operator.yaml" --server-side=false \
  || log_err "Failed to apply cnpg-operator.yaml"
${KUBECTL} apply -f "${COMPILED_DIR}/kueue-controller.yaml" --server-side=false \
  || log_err "Failed to apply kueue-controller.yaml"
${KUBECTL} apply -f "${COMPILED_DIR}/kueue-quota.yaml" --server-side=false \
  || log_err "Failed to apply kueue-quota.yaml"
apply_phase "${ENABLE_DIR}/00-infrastructure-dependencies"
PHASE_STATUS["00-infrastructure-dependencies"]="applied"

# Wait for cert-manager before continuing -- guardian CRDs use cert-manager Issuers
if [[ "${DRY_RUN}" == "false" ]]; then
  wait_crd "certificates.cert-manager.io" 180
  wait_ready "cert-manager" "cert-manager-webhook" 180
fi
log_ok "cert-manager ready"

# Phase 01: guardian bootstrap (CRDs, RBAC, policy)
apply_phase "${ENABLE_DIR}/01-guardian-bootstrap"
PHASE_STATUS["01-guardian-bootstrap"]="applied"

# Phase 02: guardian deploy
apply_phase "${ENABLE_DIR}/02-guardian-deploy"
PHASE_STATUS["02-guardian-deploy"]="applied"

if [[ "${DRY_RUN}" == "false" ]]; then
  wait_ready "seam-system" "guardian" 180
fi
log_ok "guardian ready"

# Phase 03: platform, wrapper, seam-core
apply_phase "${ENABLE_DIR}/03-platform-wrapper"
PHASE_STATUS["03-platform-wrapper"]="applied"

if [[ "${DRY_RUN}" == "false" ]]; then
  wait_ready "seam-system" "platform" 180
  wait_ready "seam-system" "wrapper" 180
  wait_ready "seam-system" "seam-core" 180
fi
log_ok "platform, wrapper, seam-core ready"

# Phase 04: conductor
apply_phase "${ENABLE_DIR}/04-conductor"
PHASE_STATUS["04-conductor"]="applied"

if [[ "${DRY_RUN}" == "false" ]]; then
  wait_ready "ont-system" "conductor" 180
fi
log_ok "conductor ready"

# Phase 05: post-bootstrap
apply_phase "${ENABLE_DIR}/05-post-bootstrap"
PHASE_STATUS["05-post-bootstrap"]="applied"

# ---------------------------------------------------------------------------
# SECTION 7: Post-bootstrap steps
# ---------------------------------------------------------------------------

POST_STATUS=()

# Step 7a: CoreDNS DSNS patch
# The Corefile is an existing ConfigMap value that must be appended to.
# A static manifest would replace the whole ConfigMap and lose the default zones.
# coredns.sh captures this same logic; here it is inlined for portability.
log_step "7a: CoreDNS DSNS patch"
if [[ "${DRY_RUN}" == "false" ]]; then
  CURRENT_COREFILE=$(kubectl get cm coredns -n kube-system -o jsonpath='{.data.Corefile}')
  if echo "${CURRENT_COREFILE}" | grep -q "seam.ontave.dev"; then
    log_ok "CoreDNS DSNS stanza already present, skipping append"
  else
    NEW_COREFILE="${CURRENT_COREFILE}
seam.ontave.dev. {
    file /etc/coredns/dsns/zone.db
    reload 10s
}"
    jq -n --arg cfg "${NEW_COREFILE}" '{"data": {"Corefile": $cfg}}' | \
      kubectl patch cm coredns -n kube-system --patch-file /dev/stdin \
      || log_err "CoreDNS Corefile patch failed"
  fi
  kubectl rollout restart deployment/coredns -n kube-system
  wait_ready "kube-system" "coredns" 120
else
  echo kubectl patch cm coredns -n kube-system --patch-file /dev/stdin
  echo kubectl rollout restart deployment/coredns -n kube-system
fi
POST_STATUS+=("7a-coredns: applied")
log_ok "CoreDNS DSNS patch complete"

# Step 7b: CNPG readiness gate
# CNPG Cluster creation requires the CNPG operator webhook to be live.
# Wait for the operator pod before applying the Cluster resource.
log_step "7b: CNPG readiness gate"
if [[ "${DRY_RUN}" == "false" ]]; then
  CNPG_ELAPSED=0
  CNPG_TIMEOUT=120
  CNPG_INTERVAL=5
  while true; do
    CNPG_READY=$(kubectl get pods -n cnpg-system \
      -l app.kubernetes.io/name=cloudnative-pg \
      -o jsonpath='{.items[0].status.conditions[?(@.type=="Ready")].status}' \
      2>/dev/null || echo "")
    if [[ "${CNPG_READY}" == "True" ]]; then
      log_ok "CNPG operator pod ready"
      break
    fi
    if [[ "${CNPG_ELAPSED}" -ge "${CNPG_TIMEOUT}" ]]; then
      log_err "Timeout waiting for CNPG operator pod after ${CNPG_TIMEOUT}s"
    fi
    sleep "${CNPG_INTERVAL}"
    CNPG_ELAPSED=$((CNPG_ELAPSED + CNPG_INTERVAL))
    echo "    ... CNPG operator not yet ready (${CNPG_ELAPSED}s)"
  done
fi

${KUBECTL} apply -f "${COMPILED_DIR}/guardian-cnpg.yaml" --server-side=false \
  || log_err "Failed to apply guardian-cnpg.yaml"

if [[ "${DRY_RUN}" == "false" ]]; then
  log_step "Waiting for CNPG cluster guardian-cnpg (timeout: 300s)"
  CL_ELAPSED=0
  CL_TIMEOUT=300
  CL_INTERVAL=10
  while true; do
    CL_READY=$(kubectl get cluster guardian-cnpg -n seam-system \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' \
      2>/dev/null || echo "")
    if [[ "${CL_READY}" == "True" ]]; then
      log_ok "CNPG cluster guardian-cnpg is Ready"
      break
    fi
    if [[ "${CL_ELAPSED}" -ge "${CL_TIMEOUT}" ]]; then
      log_err "Timeout waiting for CNPG cluster guardian-cnpg after ${CL_TIMEOUT}s"
    fi
    sleep "${CL_INTERVAL}"
    CL_ELAPSED=$((CL_ELAPSED + CL_INTERVAL))
    echo "    ... guardian-cnpg: not yet ready (${CL_ELAPSED}s)"
  done
fi

${KUBECTL} apply -f "${COMPILED_DIR}/guardian-db-app.yaml" --server-side=false \
  || log_err "Failed to apply guardian-db-app.yaml"
POST_STATUS+=("7b-cnpg: applied")
log_ok "CNPG readiness gate complete"

# Step 7c: Guardian CNPG connection patch
# guardian-patch.yaml creates the db-app Secret (CNPG connection URI) and the
# webhook Certificate. The deployment already references both by name; this step
# makes them exist so Guardian can start cleanly after restart.
# guardian-deployment-patch.yaml is a shell script (kubectl patch commands) that
# adds webhook cert volume mounts and expands guardian RBAC rules. It uses the
# .yaml extension but is not a kubectl apply target.
log_step "7c: Guardian CNPG connection patch"
${KUBECTL} apply -f "${COMPILED_DIR}/guardian-patch.yaml" --server-side=false \
  || log_err "Failed to apply guardian-patch.yaml"
if [[ "${DRY_RUN}" == "false" ]]; then
  bash "${COMPILED_DIR}/guardian-deployment-patch.yaml" \
    || log_err "guardian-deployment-patch.yaml failed"
  kubectl rollout restart deployment/guardian -n seam-system
  wait_ready "seam-system" "guardian" 180
else
  echo "bash ${COMPILED_DIR}/guardian-deployment-patch.yaml"
  echo kubectl rollout restart deployment/guardian -n seam-system
fi
POST_STATUS+=("7c-guardian-patch: applied")
log_ok "Guardian CNPG connection patch complete"

# Step 7d: Kueue webhook scoping
# Scope Kueue's mutating webhook to ont-managed namespaces only so it does not
# intercept workloads in system namespaces. A static manifest replacement would
# conflict with Kueue's own controller on subsequent reconciles; kubectl patch
# is the correct path for a targeted field addition.
log_step "7d: Kueue webhook scoping"
${KUBECTL} patch mutatingwebhookconfiguration \
  kueue-validating-webhook-configuration \
  --type=json \
  -p='[{"op":"add","path":"/webhooks/0/namespaceSelector","value":{"matchLabels":{"ont-managed":"true"}}}]' \
  || log_err "Kueue webhook scoping patch failed"
POST_STATUS+=("7d-kueue-webhook: patched")
log_ok "Kueue webhook scoping complete"

# Step 7e: Webhook certificates
# platform-wrapper-seam-core-webhook-cert.sh applies cert-manager Certificate
# objects for platform, wrapper, and seam-core. If absent, cert-manager has
# handled webhook TLS provisioning automatically via annotations.
log_step "7e: Webhook certificates"
if [[ -f "${COMPILED_DIR}/platform-wrapper-seam-core-webhook-cert.sh" ]]; then
  if [[ "${DRY_RUN}" == "false" ]]; then
    bash "${COMPILED_DIR}/platform-wrapper-seam-core-webhook-cert.sh" \
      || log_err "Webhook cert script failed"
  else
    echo "bash ${COMPILED_DIR}/platform-wrapper-seam-core-webhook-cert.sh"
  fi
  POST_STATUS+=("7e-webhook-certs: applied from script")
  log_ok "Webhook certificates provisioned from script"
else
  POST_STATUS+=("7e-webhook-certs: cert-manager automatic")
  log_ok "Webhook cert provisioning handled by cert-manager automatically"
fi

# ---------------------------------------------------------------------------
# SECTION 8: Completion report
# ---------------------------------------------------------------------------

SCRIPT_END=$(date +%s)
ELAPSED=$((SCRIPT_END - SCRIPT_START))

CLUSTER_ENDPOINT=$(kubectl cluster-info 2>/dev/null \
  | grep "Kubernetes control plane" | grep -o 'https://[^ ]*' 2>/dev/null || echo "unknown")

echo ""
echo "============================================================"
echo " ONT Management Cluster Enable: COMPLETE"
echo "============================================================"
echo ""
echo "Phases applied:"
for phase in 00a-namespaces 00-infrastructure-dependencies 01-guardian-bootstrap \
             02-guardian-deploy 03-platform-wrapper 04-conductor 05-post-bootstrap; do
  printf "  [OK] %-38s %s\n" "${phase}" "${PHASE_STATUS[${phase}]:-unknown}"
done
echo ""
echo "Post-bootstrap steps:"
for step in "${POST_STATUS[@]}"; do
  echo "  [OK] ${step}"
done
echo ""
printf "  Cluster endpoint:  %s\n" "${CLUSTER_ENDPOINT}"
printf "  Elapsed:           %ss\n" "${ELAPSED}"
echo ""
echo "Manual step remaining:"
echo "  Apply your TalosCluster CR to import the cluster:"
echo "  kubectl apply -f lab/configs/ccs-mgmt/ccs-mgmt.yaml"
echo "============================================================"
