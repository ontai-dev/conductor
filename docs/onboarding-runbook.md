# Seam Platform Onboarding Runbook

**Audience:** Kubernetes administrators and platform engineers onboarding clusters to ONT (Operator Native Thinking) governance.

**Scope:** Alpha release. Covers the import path for both management and tenant clusters. CAPI-managed lifecycle and mode=bootstrap are noted where they appear but are deferred.

---

## What is Covered in This Release

| Capability | Status |
|---|---|
| Import existing Talos cluster as management cluster | Covered |
| Import existing Talos cluster as tenant cluster | Covered |
| Pack delivery (Helm, raw YAML, Kustomize) | Covered |
| Day2 operations: PKI rotation, Talos upgrade, K8s upgrade, hardening | Covered |
| Drift detection and corrective reconciliation | Covered |
| Security enforcement via Guardian | Covered |
| RBAC audit and enforcement (audit mode then enforce) | Covered |
| Self-hosted OCI registry workflow | Covered |
| CAPI-managed cluster lifecycle | Deferred |
| mode=bootstrap (Talos machine provisioning from scratch) | Deferred |
| IdentityProvider and IdentityBinding | Future scope |
| LineageSink | Future scope |
| Screen (virtualization operator) | Future scope |
| Vortex (UI with NLP / AI integration) | Future scope |
| ONTAR (application runtime) | Future scope |

---

## Architecture in One Paragraph

Seam is a schema-first Kubernetes governance platform built on ONT philosophy. Every cluster, every pack, every RBAC policy is a CRD instance. Operators reconcile declared state -- they do not run imperative scripts. The **management cluster** runs five operators: Guardian (RBAC plane), Platform (cluster lifecycle), Wrapper (pack delivery), Conductor agent (governance intelligence), and Compiler (offline compile tool, not deployed). **Tenant clusters** run only the Conductor agent in tenant role. The Compiler produces CRs; humans apply them; operators and the Conductor agent close the gap between declared and actual state forever.

---

## 1. Pre-requisites

### 1.1 Talos cluster

The cluster must be running Talos Linux v1.9 or later. ONT does not support other Kubernetes distributions for the management cluster in the alpha release. The import path assumes:

- The cluster is already bootstrapped with etcd running
- A stable control plane endpoint (VIP or load balancer) exists
- Nodes are reachable via Talos API (talosctl)
- A working kubeconfig is available

### 1.2 Self-hosted OCI registry

All pack artifacts and operator images are served from a self-hosted OCI registry. ONT does not require any external registry at runtime. A Zot or Docker Registry v2 instance reachable from both the management cluster nodes and the workstation running the Compiler is sufficient.

Set up the registry before running any compiler command:

```
# Example: run Zot as a Kubernetes Deployment on the management cluster.
# A sample manifest is in lab/configs/registry/zot-deployment.yaml.
kubectl apply -f lab/configs/registry/zot-deployment.yaml
```

Export the registry address once and reuse it across all compile and push commands:

```
export IMAGE_REGISTRY=10.0.0.1:5000/ontai-dev
```

**Authentication for secured registries:**

The Compiler reads `~/.docker/config.json` to resolve credentials when pushing OCI pack artifacts. On Ubuntu systems with snap-installed Docker the config lives at `~/snap/docker/current/.docker/config.json` instead -- the Compiler checks both paths automatically.

To populate credentials for a registry that requires authentication:

```
docker login your-registry.example.com
# Prompts for username and password.
# Writes the encoded credential to ~/.docker/config.json.
```

The config format the Compiler reads is:

```
{
  "auths": {
    "your-registry.example.com": {
      "auth": "<base64-encoded user:password>"
    }
  }
}
```

If the Compiler returns HTTP 401 or 403 when pushing a pack layer, the error message will include `check registry auth if 401/403`. Run `docker login` for the registry and retry. Unauthenticated registries on `10.x` and `localhost` addresses use plain HTTP without credentials -- no login required for lab setups.

### 1.3 Build the Compiler binary

The Compiler is a stateless offline tool. It reads YAML input files and emits Kubernetes CR YAML. It never connects to any cluster.

```
cd conductor
go build -o compiler ./cmd/compiler
```

For a reproducible lab build that produces all three images (compiler, conductor-execute, conductor agent), run from the conductor directory:

```
make docker-build TAG=dev IMAGE_REGISTRY=10.0.0.1:5000/ontai-dev
make docker-push  TAG=dev IMAGE_REGISTRY=10.0.0.1:5000/ontai-dev
```

This pushes:
- `10.0.0.1:5000/ontai-dev/compiler:dev` -- the offline compile tool
- `10.0.0.1:5000/ontai-dev/conductor-execute:dev` -- executor for Kueue Jobs on the management cluster
- `10.0.0.1:5000/ontai-dev/conductor:dev` -- the governance agent deployed to every cluster

For stable releases the tag convention is `v{talosVersion}-r{revision}` (e.g. `v1.9.3-r1`). Lab and development builds always use the `:dev` tag. Custom per-build tags are never committed to Deployment YAML or enable bundles.

### 1.4 seam-core CRDs

seam-core owns every cross-operator CRD definition. The CRDs must be installed on the management cluster before any operator reaches Running state:

```
kubectl --kubeconfig ./management-kubeconfig apply -k github.com/ontai-dev/seam-core/config/crd
```

Or from a local checkout:

```
kubectl --kubeconfig ./management-kubeconfig apply -k seam-core/config/crd
```

---

## 2. Onboarding the Management Cluster

The management cluster runs all five ONT operators and holds the authoritative governance state for every tenant cluster it owns.

### 2.1 Compile the cluster manifests

Edit `docs/configs/cluster-input-management.yaml` to match your cluster. The cluster name must be short, DNS-label-safe, and unique across your ONT deployment.

**Management clusters must always use `mode: import`.** The Compiler rejects `mode: bootstrap` combined with `role: management` or an absent role (management is the default). If you pass a management cluster input file with `mode: bootstrap`, the Compiler returns:

```
input file "cluster-input-management.yaml": management cluster cannot be bootstrapped via compiler (mode=bootstrap is not supported for role=management); use mode=import
```

Compile:

```
./compiler bootstrap \
  --input  docs/configs/cluster-input-management.yaml \
  --output ./out/management
```

Inspect the output before applying:

```
ls ./out/management/
# management.yaml                        InfrastructureTalosCluster CR
# seam-mc-management-{node}.yaml x N     Talos machine config Secrets (one per node)
# seam-mc-management-talosconfig.yaml    talosconfig Secret
# bootstrap-sequence.yaml                Ordered apply sequence
```

### 2.2 Apply the output to the management cluster

```
kubectl --kubeconfig ./management-kubeconfig apply -f ./out/management/
```

The `InfrastructureTalosCluster` CR is created in `seam-system`. The Platform operator reconciles it and creates `seam-tenant-{name}` namespace for the cluster's operational resources.

### 2.3 Enable the ONT operator stack

The Compiler generates an enable bundle that installs all operators in the correct order. Generate it once per management cluster:

```
./compiler enable \
  --cluster-role management \
  --output ./out/management-enable \
  --registry 10.0.0.1:5000/ontai-dev \
  --signing-private-key ./keys/signing-private.pem \
  --output-public-key ./keys/signing-public.pem
```

The enable bundle applies five phases in order:

| Phase | Content | Gate |
|---|---|---|
| 00 | seam-core CRDs, namespaces | CRDs registered |
| 01 | Guardian bootstrap RBAC | Namespace labels applied |
| 02 | Guardian Deployment | Guardian admission webhook operational |
| 03 | Platform and Wrapper | Guardian RBACProfile provisioned=true |
| 04 | Conductor agent Deployment | RunnerConfig published |
| 05 | Post-bootstrap integration checks | All operators Running |

Apply each phase in sequence:

```
kubectl --kubeconfig ./management-kubeconfig apply -f ./out/management-enable/00-seam-core/
kubectl --kubeconfig ./management-kubeconfig apply -f ./out/management-enable/01-guardian-bootstrap/
# wait for Guardian webhook pod to be Running before continuing
kubectl --kubeconfig ./management-kubeconfig apply -f ./out/management-enable/02-guardian-deploy/
kubectl --kubeconfig ./management-kubeconfig apply -f ./out/management-enable/03-platform-wrapper/
kubectl --kubeconfig ./management-kubeconfig apply -f ./out/management-enable/04-conductor/
```

**Why the strict ordering?** Guardian deploys first because its admission webhook gates every RBAC resource write on every subsequent phase. No operator provisions its own RBAC -- Guardian owns all RBAC on all clusters. If a phase fails because the webhook rejects a resource, check the Guardian pod logs and confirm the webhook is reachable.

### 2.4 Verify management cluster readiness

```
kubectl --kubeconfig ./management-kubeconfig -n seam-system get pods
# All operator pods should be Running.

kubectl --kubeconfig ./management-kubeconfig -n ont-system get infrastructurerunnerconfig
# RunnerConfig should exist with 17 published capabilities in status.

kubectl --kubeconfig ./management-kubeconfig -n seam-system \
  get infrastructuretaloscluster management -o jsonpath='{.status.conditions}'
# Ready=True expected.
```

---

## 3. Onboarding a Tenant Cluster

A tenant cluster is governed by the management cluster. The Conductor agent runs in tenant role on the tenant cluster and synchronizes governance state back to the management cluster.

### 3.1 Modes and roles explained

ONT uses two independent fields on `InfrastructureTalosCluster`:

**`spec.mode`**: How the cluster was created.
- `import` -- the cluster already exists; ONT assumes governance of a running cluster.
- `bootstrap` -- ONT provisions the cluster from bare metal (deferred for alpha tenant clusters).

When `mode=import` and the cluster is deleted from seam-system, the management relationship is severed and governance stops. The cluster continues to run -- no nodes are destroyed. This is the "divorce" path, not a destruction.

**`spec.role`**: The cluster's function within ONT.
- `management` -- runs the full operator stack; holds authoritative governance state.
- `tenant` -- governed by a management cluster; runs only the Conductor agent in tenant role.

These two fields are independent. A cluster with `mode=import, role=tenant` is an existing cluster being brought under ONT governance. A cluster with `mode=bootstrap, role=tenant` would be a new cluster provisioned by the Platform operator (deferred for alpha).

**Hard constraint enforced by the Compiler:** `mode=bootstrap` is never valid for `role=management`. Management clusters are always imported from an existing, running cluster. The Compiler returns an error immediately if you attempt to bootstrap a management cluster. Management cluster lifecycle (`mode=bootstrap`) is permanently deferred -- management clusters are by definition pre-existing.

**Deletion semantics:**
- `mode=import`: deleting the `InfrastructureTalosCluster` CR severs governance only. The cluster continues to run.
- `mode=bootstrap`: deleting the CR triggers full decommission. The cluster is destroyed.

### 3.2 Compile the tenant cluster manifests

Edit `docs/configs/cluster-input-tenant.yaml` for your tenant cluster.

**Machine config files are required for tenant imports.** When importing an existing cluster that was bootstrapped independently (without ONT), the Compiler needs the raw Talos machine config for each node so it can extract the CA and generate the per-node `seam-mc-*` Secrets. Set `machineConfigPaths` in the input file:

```
machineConfigPaths:
  tenant-cp1: /path/to/tenant-cp1-machineconfig.yaml
  tenant-cp2: /path/to/tenant-cp2-machineconfig.yaml
  tenant-cp3: /path/to/tenant-cp3-machineconfig.yaml
```

The keys are node hostnames and must match the `nodes[].hostname` entries exactly. The `init` node entry is required (CA extraction is performed from its machine config). If the cluster was originally bootstrapped by ONT, the Compiler reads machine configs from the `seam-mc-*` Secrets already in the management cluster -- `machineConfigPaths` is not needed in that case.

Compile:

```
./compiler bootstrap \
  --input  docs/configs/cluster-input-tenant.yaml \
  --output ./out/tenant
```

Output:
```
ls ./out/tenant/
# tenant.yaml                              InfrastructureTalosCluster CR
# seam-tenant-tenant.yaml                  Namespace manifest
# seam-mc-tenant-{hostname}.yaml x N       Machine config Secrets (one per node)
# seam-mc-tenant-talosconfig.yaml          talosconfig Secret
# bootstrap-sequence.yaml                  Ordered apply sequence
```

The machine config Secrets and talosconfig Secret are applied to the **management cluster**. The Platform operator reads them to communicate with the tenant cluster via the Talos API for day2 operations.

### 3.3 Apply to the management cluster

All tenant cluster CRs live on the management cluster, not on the tenant cluster itself:

```
kubectl --kubeconfig ./management-kubeconfig apply -f ./out/tenant/
```

### 3.4 Enable the tenant Conductor agent

The Compiler generates a tenant-specific enable bundle. It installs only Phase 04 (Conductor agent in tenant role). Phases 01-03 are skipped for tenant clusters -- the management cluster already governs the RBAC plane:

```
./compiler enable \
  --cluster-role tenant \
  --output ./out/tenant-enable \
  --registry 10.0.0.1:5000/ontai-dev \
  --kubeconfig ./tenant-kubeconfig
```

Apply to the tenant cluster:

```
kubectl --kubeconfig ./tenant-kubeconfig apply -f ./out/tenant-enable/04-conductor/
```

The tenant Conductor agent:
- Watches local pack state and emits DriftSignals to the management cluster when divergence is detected
- Runs the TalosVersionDriftLoop and KubernetesVersionDriftLoop
- Receives RunnerConfig and PackReceipt updates from the management cluster via pull loops
- Verifies Ed25519 signatures on all received artifacts before writing to local state

### 3.5 Verify tenant cluster readiness

```
# On management cluster: check TalosCluster status
kubectl --kubeconfig ./management-kubeconfig -n seam-system \
  get infrastructuretaloscluster tenant -o jsonpath='{.status.conditions}'

# On management cluster: check RunnerConfig was provisioned for the tenant
kubectl --kubeconfig ./management-kubeconfig -n seam-tenant-tenant \
  get infrastructurerunnerconfig

# On tenant cluster: check conductor agent pod
kubectl --kubeconfig ./tenant-kubeconfig -n ont-system get pods
```

---

## 4. Delivering Packs to Clusters

A **pack** is a versioned, signed software artifact -- a Helm chart, a raw YAML bundle, or a Kustomize overlay -- compiled into three OCI layers and tracked through its full lifecycle on every target cluster.

### 4.1 Compile a pack

Three source types are supported. Example config files are in `docs/configs/`:

```
# Helm chart from a self-hosted chart repository
./compiler packbuild \
  --input docs/configs/packbuild-helm.yaml \
  --output ./out/cert-manager-pack

# Raw YAML manifest directory
./compiler packbuild \
  --input docs/configs/packbuild-raw.yaml \
  --output ./out/minio-pack

# Kustomize overlay
./compiler packbuild \
  --input docs/configs/packbuild-kustomize.yaml \
  --output ./out/local-path-pack
```

Each command pushes three OCI layers to the `registryUrl` in the input file:
- `-rbac` layer: ClusterRole, Role, ClusterRoleBinding, RoleBinding
- `-cluster` layer: CRDs and other cluster-scoped resources
- `-workload` layer: Deployments, Services, ConfigMaps, and all namespace-scoped workloads

### 4.2 Apply the ClusterPack CR

```
kubectl --kubeconfig ./management-kubeconfig apply -f ./out/cert-manager-pack/
```

The `InfrastructureClusterPack` CR triggers a `PackExecution`. The Wrapper operator runs a Conductor execute-mode Job that applies each OCI layer in sequence and writes a `PackOperationResult`. Once delivery succeeds, a `PackReceipt` is written on the tenant cluster's Conductor agent confirming what was deployed.

### 4.3 Verify pack delivery

```
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-tenant-management \
  get infrastructurepackexecution,infrastructurepackinstance,packoperationresult
```

A `PackReceipt` on the tenant cluster confirms delivery:

```
kubectl --kubeconfig ./tenant-kubeconfig \
  -n ont-system \
  get infrastructurepackreceipts
```

---

## 5. Security: RBAC Audit and Enforcement

Guardian owns all RBAC on all clusters. No ONT component creates its own Roles or ClusterRoles -- every RBAC resource is provisioned and owned by Guardian.

### 5.1 How Guardian enforces security

Guardian deploys an admission webhook that intercepts every write to Role, ClusterRole, RoleBinding, and ClusterRoleBinding. Resources that carry the annotation `ontai.dev/rbac-owner=guardian` are owned by Guardian and cannot be modified by any other principal.

Guardian operates in two modes, controlled by the annotation `ontai.dev/rbac-enforcement-mode`:
- `audit` -- violations are logged and reported but not blocked. Use this initially to understand pre-existing RBAC state.
- `enforce` -- unauthorized RBAC writes are rejected at admission. Transition to this mode after the audit sweep is clean.

On first install, Guardian runs a Phase 1 annotation sweep that stamps `ontai.dev/rbac-owner=guardian` and `ontai.dev/rbac-enforcement-mode=audit` on all pre-existing Role and ClusterRole resources. This makes existing RBAC visible to Guardian without breaking anything.

### 5.2 Auditing current RBAC state

```
# List all RBACPolicies -- these declare the intended RBAC state
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-system \
  get rbacpolicies

# List PermissionSets -- three-layer hierarchy: management-maximum, cluster-maximum, per-component
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-system \
  get permissionsets

# List PermissionSnapshots -- signed point-in-time snapshots of granted permissions
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-system \
  get permissionsnapshots
```

A `PermissionSnapshot` is signed by the management cluster Conductor using Ed25519. Tenant cluster Conductors verify the signature before applying the snapshot locally. This chain means: if a snapshot does not match what the management cluster signed, it is rejected outright.

### 5.3 CNPG audit data (future capability)

The Guardian CNPG (CloudNative PG) integration writes every RBAC governance event -- grant, revoke, policy change, enforcement violation -- to a PostgreSQL database. In the alpha release, this data is captured and queryable per-cluster. In a future release, the CNPG sink will be exposed as a structured API that enables:
- Cross-cluster policy diff reports
- Time-travel queries ("what was the permission state of cluster X at timestamp T?")
- Alerting on unexpected permission escalations across the fleet

For now, query the `audit_events` table in the CNPG instance deployed by Guardian to review governance history.

### 5.4 Transitioning from audit to enforce

After confirming no unexpected RBAC violations appear in audit mode:

```
# Patch the Guardian RBACProfile enforcement mode
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-system \
  patch rbacprofile guardian-conductor-tenant \
  --type=merge \
  -p '{"spec":{"enforcementMode":"enforce"}}'
```

The Guardian admission webhook immediately begins rejecting unauthorized RBAC writes on all clusters under management.

---

## 6. Day2 Operations

Day2 operations are triggered by creating the appropriate CR on the management cluster. The Platform operator detects the CR, generates a RunnerConfig, and submits a Conductor execute-mode Job to perform the operation. No manual node access is required.

### 6.1 Talos version upgrade

Create a `TalosHardeningApply` or `UpgradePolicy` CR targeting the cluster. The Platform operator submits a `talos-upgrade` executor Job:

```
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-tenant-management \
  apply -f - <<EOF
apiVersion: platform.ontai.dev/v1alpha1
kind: UpgradePolicy
metadata:
  name: talos-upgrade-v1-9-4
  namespace: seam-tenant-management
spec:
  upgradeType: talos
  targetTalosVersion: "v1.9.4"
  clusterRef: management
EOF
```

The `talosUpgradeHandler` performs a rolling per-node sequential upgrade. Each node is upgraded and the handler waits for it to reboot and come back online before proceeding to the next. No manual intervention is needed unless a node fails to rejoin within the 10-minute window.

### 6.2 Kubernetes version upgrade

```
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-tenant-management \
  apply -f - <<EOF
apiVersion: platform.ontai.dev/v1alpha1
kind: UpgradePolicy
metadata:
  name: kube-upgrade-1-32-4
  namespace: seam-tenant-management
spec:
  upgradeType: kubernetes
  targetKubernetesVersion: "1.32.4"
  clusterRef: management
EOF
```

The `kubeUpgradeHandler` reads the full machine config from each node via the Talos API, merges the new kubelet image reference (`ghcr.io/siderolabs/kubelet:v{version}`), and applies the merged config in `no-reboot` mode. The kubelet service restarts automatically -- no node reboot required.

**Important:** `targetKubernetesVersion` is stored without the `v` prefix (e.g. `1.32.4`, not `v1.32.4`). The executor adds the `v` prefix when building the kubelet OCI image reference.

### 6.3 PKI rotation

Two trigger paths are available:

**Annotation-triggered (on-demand):**

```
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-system \
  annotate infrastructuretaloscluster management \
  platform.ontai.dev/rotate-pki=true
```

The Platform reconciler detects the annotation, creates a `PKIRotation` CR with label `pki-trigger=manual` in `seam-tenant-{cluster}`, then clears the annotation. The executor Job applies a staged machine config to initiate the Talos rotation cycle and refreshes the kubeconfig Secrets.

**Automatic (threshold-based):**

Set `spec.pkiRotationThresholdDays` on the `InfrastructureTalosCluster` CR. The Platform reconciler reads the `status.pkiExpiryDate` field (earliest cert expiry across all cluster Secrets) and creates a `PKIRotation` CR with label `pki-trigger=auto` when expiry is within the threshold.

```
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-system \
  patch infrastructuretaloscluster management \
  --type=merge \
  -p '{"spec":{"pkiRotationThresholdDays":30}}'
```

### 6.4 Hardening profile application

A `HardeningProfile` CR declares a set of Talos `machineConfigPatches` (partial machine config overlays). Creating a `NodeMaintenance` CR that references the profile triggers the `hardening-apply` executor Job. The Job applies each patch in order across all nodes in `no-reboot` mode.

```
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-tenant-management \
  apply -f - <<EOF
apiVersion: platform.ontai.dev/v1alpha1
kind: NodeMaintenance
metadata:
  name: apply-baseline-hardening
  namespace: seam-tenant-management
spec:
  operation: hardening-apply
  hardeningProfileRef: baseline
  clusterRef: management
EOF
```

### 6.5 Etcd backup

```
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-tenant-management \
  apply -f - <<EOF
apiVersion: platform.ontai.dev/v1alpha1
kind: EtcdMaintenance
metadata:
  name: etcd-backup-2026-05-03
  namespace: seam-tenant-management
spec:
  operation: etcd-backup
  clusterRef: management
  s3Destination:
    bucket: ont-etcd-backups
    key: management/2026-05-03.snapshot
EOF
```

The `etcdBackupHandler` streams the snapshot via the Talos API and uploads it to the S3-compatible endpoint configured in the executor Job environment (`S3_ENDPOINT`, `S3_REGION`). The upload uses a seekable byte reader, which is required for MinIO and Scality over plain HTTP.

---

## 7. Drift Detection and Corrective Reconciliation

The Conductor agent on each cluster continuously monitors actual state against declared state. When divergence is detected, it does not remediate directly. Instead, it emits a `DriftSignal` to the management cluster. The management cluster operators respond by scheduling the appropriate corrective Job.

### 7.1 Drift signal lifecycle

```
pending -> queued -> confirmed
```

- **pending**: emitted by the Conductor agent; the management cluster has not yet acted.
- **queued**: the DriftSignalReconciler has processed the signal and created a corrective UpgradePolicy or PackExecution. The corrective Job is scheduled or running.
- **confirmed**: the Conductor agent detected that declared state matches actual state; drift is resolved.

### 7.2 Types of drift detected

| Loop | What it monitors | DriftSignal trigger |
|---|---|---|
| PackReceiptDriftLoop | Deployed resources from PackReceipt | Resource missing from cluster |
| TalosVersionDriftLoop | Node `osImage` vs `spec.talosVersion` | Out-of-band Talos upgrade detected |
| KubernetesVersionDriftLoop | Node `kubeletVersion` vs `spec.kubernetesVersion` | Out-of-band K8s upgrade detected |
| CapabilityPublisher | RunnerConfig existence | RunnerConfig missing from ont-system |

### 7.3 Changes made outside ONT awareness

If a Kubernetes admin makes a change outside ONT -- deletes a Deployment managed by a pack, upgrades Talos or Kubernetes directly via `talosctl`, or modifies an RBAC resource -- the relevant drift loop detects it within one reconciliation cycle (typically 1-2 minutes) and emits a DriftSignal.

**The corrective sequence for a deleted pack resource:**

1. PackReceiptDriftLoop notices the resource is absent
2. DriftSignal `drift-{pack}-{cluster}` emitted to management cluster in `pending` state
3. DriftSignalHandler deletes the PackExecution for that pack
4. Wrapper operator creates a new PackExecution
5. Conductor execute-mode Job redeploys the pack
6. PackReceiptDriftLoop confirms resource is present; DriftSignal moves to `confirmed`

**The corrective sequence for an out-of-band Kubernetes upgrade:**

1. KubernetesVersionDriftLoop detects nodes at `v1.32.4` while spec says `1.32.3`
2. DriftSignal `drift-k8s-version-{cluster}` emitted
3. DriftSignalReconciler creates a corrective `UpgradePolicy` targeting `1.32.3`
4. UpgradePolicyReconciler submits a `kube-upgrade` executor Job
5. Job applies the correct kubelet image config to all nodes via Talos API
6. KubernetesVersionDriftLoop confirms nodes returned to spec version; DriftSignal confirmed

### 7.4 Manually nudging a stuck operation

Corrective Jobs use annotations and TCOR records to track state. If a Job fails, the UpgradePolicy is set to `Degraded=True` and no automatic retry occurs. To retry:

```
# Delete the failed Job
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-tenant-{cluster} delete job {job-name}

# Remove the TCOR operation record for the failed job
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-tenant-{cluster} patch infrastructuretalosclusteroperationresult {cluster} \
  --type=json \
  -p '[{"op":"remove","path":"/spec/operations/{job-name}"}]'

# Touch the UpgradePolicy to trigger re-reconcile
kubectl --kubeconfig ./management-kubeconfig \
  -n seam-tenant-{cluster} annotate upgradepolicy {policy-name} \
  ontai.dev/retry-ts="$(date -u +%s)" --overwrite
```

---

## 8. Annotations Reference

ONT uses annotations on CRs to communicate intent and trigger operations. This table covers all annotations active in the alpha release.

| Annotation | Object | Effect |
|---|---|---|
| `platform.ontai.dev/rotate-pki=true` | `InfrastructureTalosCluster` | Triggers on-demand PKI rotation. Cleared by reconciler after PKIRotation CR is created. |
| `ontai.dev/reset-approved=true` | `TalosClusterReset` | Required gate before any cluster reset proceeds. Prevents accidental destruction. |
| `ontai.dev/rbac-owner=guardian` | Role, ClusterRole, RoleBinding | Marks the resource as Guardian-owned. Unauthorized writes rejected in enforce mode. |
| `ontai.dev/rbac-enforcement-mode=audit\|enforce` | Role, ClusterRole | Controls whether Guardian logs (audit) or blocks (enforce) unauthorized writes. |
| `seam.ontai.dev/webhook-mode=exempt` | Namespace | Exempts the namespace from Guardian admission webhook checks (used for seam-system). |
| `ontai.dev/retry-ts={unix-ts}` | UpgradePolicy | Manual retry nudge after a Degraded Job failure. Forces reconciler to re-evaluate. |
| `ontai.dev/superseded=true` | PackOperationResult | Set by the persistence layer on the previous POR revision when a new revision is written. |

**Annotation types by purpose:**

- **Trigger annotations** (`rotate-pki`, `reset-approved`): one-shot writes that cause the reconciler to act once and clear the annotation.
- **Ownership annotations** (`rbac-owner`): durable labels that declare a resource's controlling principal.
- **Mode annotations** (`rbac-enforcement-mode`, `webhook-mode`): control how a controller or webhook behaves against a given resource or namespace.
- **Retry annotations** (`retry-ts`): manual nudges for stuck reconciliation cycles.
- **Lineage annotations**: the `InfrastructureLineageController` writes governance annotations on root declaration CRs once the `LineageSynced` condition transitions to `True`. The controller runs within seam-core and covers all 9 root-declaration GVKs across `infrastructure.ontai.dev` and `security.ontai.dev`. `LineageSynced=True` is the expected steady-state once seam-core is deployed. Operators function normally while a condition is pending its first reconcile.

---

## 9. Operational Records: TCOR and POR

Every corrective Job writes its outcome to two records:

**`TalosClusterOperationResult` (TCOR):** One per cluster. Stores a map of `{jobName: operationRecord}`. Each record contains the Job outcome (succeeded or failed), a human-readable message, and timestamps. The Platform operator reads this map to determine Job success -- not the Kubernetes Job `.status.succeeded` field.

**`PackOperationResult` (POR):** One per PackExecution. Stores the full delivery record for each pack deploy Job: which OCI digests were applied, which resources were deployed, which revision this is. PORs follow a single-active-revision pattern: when a new revision is written, the previous one is labeled `ontai.dev/superseded=true`. Up to 10 superseded PORs are retained per PackExecution.

Together, TCOR and POR form the operational memory of the ONT platform. In a future release, these records will feed a **GraphQuery database** (backed by the CNPG integration) that enables:
- "What was deployed on cluster X at time T?" (POR time-travel)
- "Which corrective Jobs have run on cluster Y in the last 30 days?" (TCOR audit)
- Cross-cluster lineage queries linking a running workload back to its originating PackBuild, through its ClusterPack, PackExecution, and PackReceipt, to the InfrastructureLineageIndex of the root declaration

This convergence of operational records into a queryable memory layer is the foundation for governance at fleet scale.

---

## 10. Future Scope

### IdentityProvider and IdentityBinding

IdentityProvider and IdentityBinding CRDs are planned as the authentication layer for ONT. They will allow platform operators to declare which identity providers (OIDC, LDAP, SAML) are trusted on which clusters and what bindings exist between external identities and ONT PermissionSets. Implementation is deferred until after alpha.

### LineageSink

LineageSink is the planned mechanism for exporting `InfrastructureLineageIndex` records to external audit and compliance systems. A LineageSink CR will declare a target (webhook, S3, message queue) and the controller will stream every lineage event as an immutable, signed record. Deferred after alpha.

### Screen

Screen is a planned ONT operator for virtualization workloads (`virt.ontai.dev`). Screen will manage KubeVirt-backed VMs under the same governance model as container packs: declared as CRs, delivered via the pack mechanism, audited by Guardian, and tracked by TCOR. No implementation exists. Screen will not appear in any release until a Governor-approved ADR is submitted.

### Vortex

Vortex is the planned management UI for the ONT platform with NLP and AI-assisted governance. Vortex will surface DriftSignals, TCOR/POR history, and lineage queries in a conversational interface backed by the GraphQuery layer. Future scope only.

### ONTAR

ONTAR (ONT Application Runtime) is the planned application runtime layer that brings ONT governance principles to the application tier. ONTAR will allow application teams to declare application topology, dependency graphs, and SLO targets as CRDs governed by the same Guardian and Conductor machinery that manages infrastructure. Future scope, post-alpha.

---

## 11. Troubleshooting Quick Reference

| Symptom | Where to look | Common cause |
|---|---|---|
| Conductor agent in CrashLoopBackOff | `kubectl logs -n ont-system conductor-xxx` | Kubeconfig Secret missing or malformed |
| PackExecution stuck in pending | `kubectl get pe -n seam-tenant-X` | RunnerConfig not yet published; check CapabilityPublisher |
| DriftSignal stays in `queued` | Check UpgradePolicy status for Degraded=True | Corrective Job failed; retry with nudge annotation |
| Node `NotReady` after kube upgrade | Check node machine config kubelet image | v-prefix missing in targetKubernetesVersion (should not have `v`) |
| Guardian webhook rejects resource | `kubectl get events -n seam-system` | Resource lacks `ontai.dev/rbac-owner=guardian` annotation |
| LineageSynced=False on a CR | Check seam-core pod logs in seam-system | LineageController running; likely a transient state on first reconcile. All 9 root-declaration GVKs are covered. |
| UpgradePolicy reaches Degraded | Check TCOR operation record | Talos API rejected partial machine config; check executor Job logs |

---

*conductor/docs/onboarding-runbook.md*
*For schema details see conductor-schema.md and seam-core-schema.md.*
*For compiler command reference see compiler-usage.md.*
