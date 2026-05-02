# Compiler Usage

The Compiler is a short-lived tool that converts human-authored spec files into
Kubernetes CR YAML. It is a read-transform-write pipeline: it never connects to any
cluster, never applies resources, and never mutates cluster state.

Output files are reviewed by the human and then applied with `kubectl apply`.

---

## Prerequisites

Build the Compiler binary from the conductor repo root:

```
go build -o compiler ./cmd/compiler
```

The binary is stateless and requires no cluster access.

---

## 1. Generating an InfrastructureTalosCluster CR

### What it does

`compiler bootstrap` reads a `ClusterInput` YAML file and emits:

- `{cluster-name}.yaml` -- the `InfrastructureTalosCluster` CR for the management
  cluster, ready to apply to the management cluster under `seam-system`.
- `seam-mc-{cluster}-{hostname}.yaml` -- one Talos machine config Secret per node
  (mode=bootstrap only).
- `bootstrap-sequence.yaml` -- ordered bootstrap step manifest.
- `seam-tenant-{cluster-name}.yaml` -- tenant namespace manifest (mode=import only).

### Command

```
compiler bootstrap \
  --input  lab/configs/ccs-mgmt/cluster-input.yaml \
  --output /tmp/ccs-mgmt-out
```

### cluster-input.yaml fields

```yaml
name: ccs-mgmt
namespace: seam-system
mode: import          # bootstrap | import
role: management      # management | tenant
capi:
  enabled: false      # false for management cluster
importExistingCluster: true
bootstrap:
  controlPlaneEndpoint: "https://10.20.0.10:6443"
  talosVersion: "v1.9.3"
  kubernetesVersion: "1.32.3"
  installDisk: "/dev/vda"
  nodes:
    - hostname: ccs-mgmt-cp1
      ip: "10.20.0.2"
      role: init
    - hostname: ccs-mgmt-cp2
      ip: "10.20.0.3"
      role: controlplane
    - hostname: ccs-mgmt-w1
      ip: "10.20.0.5"
      role: worker
ciliumPrerequisites: true
registryMirrors:
  - registry: ghcr.io
    endpoints: [http://10.20.0.1:5000]
  - registry: docker.io
    endpoints: [http://10.20.0.1:5000]
machineConfigPaths:           # import path: read existing configs from disk
  ccs-mgmt-cp1: /path/to/ccs-mgmt-cp1.yaml
```

For `mode=bootstrap`, `machineConfigPaths` is absent and the Compiler generates fresh
Talos machine configs from `bootstrap.nodes`. For `mode=import`, machineconfig Secrets
are derived from existing files listed in `machineConfigPaths`.

### Applying the output

```
kubectl --kubeconfig lab/configs/ccs-mgmt/kubeconfig apply -f /tmp/ccs-mgmt-out/
```

If machine config Secrets already exist (re-import), patch rather than apply to avoid
immutability conflicts on the InfrastructureTalosCluster spec.

---

## 2. Generating an InfrastructureClusterPack CR

### What it does

`compiler packbuild` reads a `PackBuildInput` YAML file and emits:

- `{pack-name}.yaml` -- the `InfrastructureClusterPack` CR ready to apply to the
  management cluster under the namespace declared in the input file.

For **Helm packs** (`helmSource` present): the Compiler fetches the chart tarball,
renders it, splits RBAC and workload manifests, pushes three OCI layers to the
declared registry, and records the digests in the CR. The registry must be reachable
at compile time.

For **pre-built packs** (no `helmSource`): the Compiler maps pre-computed OCI digests
declared in the input file directly into the CR. No network calls.

### Command

```
compiler packbuild \
  --input  lab/configs/packbuilds/cert-manager-helm-v1.yaml \
  --output /tmp/cert-manager-pack-out
```

### PackBuildInput fields

```yaml
name: cert-manager-helm-v1.14.0-r1     # CR metadata.name
namespace: seam-tenant-ccs-mgmt        # target namespace on the management cluster
version: v1.14.0-r1                    # semantic version recorded in spec.version
registryUrl: 10.20.0.1:5000/ontai-dev/packs/cert-manager-helm
basePackName: cert-manager-helm        # logical name shared across versions
targetClusters:
  - ccs-mgmt

# --- Helm path ---
helmSource:
  url: http://10.20.0.1:8888/cert-manager-v1.14.0.tgz
  chart: cert-manager
  version: v1.14.0
  valuesFile: values.yaml              # optional; path relative to this file

# --- Pre-built path (no helmSource) ---
# digest: sha256:abc...
# checksum: deadbeef...
# rbacDigest: sha256:...
# workloadDigest: sha256:...
# valuesFile: custom-values.yaml      # informational; records what overlay was used
```

`valuesFile` in `HelmSource` controls which values file is merged with chart defaults
at render time. The resolved filename is recorded in the output
`InfrastructureClusterPack` spec as `spec.valuesFile` so admins can trace which
customization produced the deployed artifact.

### Applying the output

```
kubectl --kubeconfig lab/configs/ccs-mgmt/kubeconfig apply \
  -f /tmp/cert-manager-pack-out/cert-manager-helm-v1.14.0-r1.yaml
```

The `InfrastructureClusterPack` CR must be applied to the management cluster in the
namespace declared in the input file (`seam-tenant-{clusterName}`). The Wrapper
operator watches this namespace and creates a `PackExecution` once the CR is admitted.

---

## 3. Full Bootstrap Sequence (ccs-mgmt)

This is the ordered sequence for the management cluster using the lab configs.

### Step 1: Generate cluster manifests

```
compiler bootstrap \
  --input  lab/configs/ccs-mgmt/cluster-input.yaml \
  --output /tmp/ccs-mgmt-out
```

Inspect output:

```
ls /tmp/ccs-mgmt-out/
```

Expected files: `ccs-mgmt.yaml`, `seam-mc-ccs-mgmt-{node}.yaml` x5 (or import-mode
equivalents), `bootstrap-sequence.yaml`.

### Step 2: Apply machine config Secrets and TalosCluster CR

```
kubectl --kubeconfig lab/configs/ccs-mgmt/kubeconfig apply -f /tmp/ccs-mgmt-out/
```

### Step 3: Compile packs

For each pack in `lab/configs/packbuilds/`:

```
compiler packbuild \
  --input  lab/configs/packbuilds/cert-manager-helm-v1.yaml \
  --output /tmp/cert-manager-pack-out

compiler packbuild \
  --input  lab/configs/packbuilds/nginx-v2.yaml \
  --output /tmp/nginx-pack-out
```

### Step 4: Apply ClusterPack CRs

```
kubectl --kubeconfig lab/configs/ccs-mgmt/kubeconfig \
  apply -f /tmp/cert-manager-pack-out/

kubectl --kubeconfig lab/configs/ccs-mgmt/kubeconfig \
  apply -f /tmp/nginx-pack-out/
```

Each applied `InfrastructureClusterPack` triggers a Wrapper `PackExecution`. Wrapper
creates the corresponding `InfrastructurePackInstance` and submits a Conductor
execute-mode Job. When the Job completes, Conductor writes a `PackOperationResult`
to `seam-tenant-ccs-mgmt`. Wrapper advances `PackExecution.status` to `Succeeded`.

### Verify pack delivery

```
kubectl --kubeconfig lab/configs/ccs-mgmt/kubeconfig \
  get infrastructurepackexecution,infrastructurepackinstance,packoperationresult \
  -n seam-tenant-ccs-mgmt
```

---

## ValuesFile Traceability

When a Helm pack is compiled with a `valuesFile`, the filename is recorded in
`spec.valuesFile` of the output `InfrastructureClusterPack`. To check what values
were used for a deployed pack:

```
kubectl --kubeconfig lab/configs/ccs-mgmt/kubeconfig \
  get infrastructureclusterpack cert-manager-helm-v1.14.0-r1 \
  -n seam-tenant-ccs-mgmt \
  -o jsonpath='{.spec.valuesFile}'
```

---

*Compiler docs -- conductor/docs/compiler-usage.md*
*conductor-schema.md §9 for full compile/apply sequence and RunnerConfig protocol.*
