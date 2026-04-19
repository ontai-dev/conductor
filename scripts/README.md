# conductor/scripts

## enable-ccs-mgmt.sh

Brings up the ONT management cluster on a fresh Talos Linux install.
Applies the six enable phases in order, waits for each component
to reach ready state before proceeding, and completes the
post-bootstrap steps that cannot be expressed as static manifests.

### Prerequisites

- kubectl configured against the target cluster
- Compiled enable directory from the Compiler bootstrap output
- CNPG operator installed (included in phase 00 infrastructure
  dependencies)
- cert-manager installed (included in phase 00)

### Usage

  ./enable-ccs-mgmt.sh --help

  ./enable-ccs-mgmt.sh \
    --compiled-dir path/to/compiled \
    --kubeconfig path/to/kubeconfig

### Options

| Flag | Default | Description |
|------|---------|-------------|
| --kubeconfig | ~/.kube/config | Path to kubeconfig |
| --compiled-dir | ../lab/configs/ccs-mgmt/compiled | Path to compiled enable directory |
| --registry | 10.20.0.1:5000 | Local OCI registry host:port |
| --dsns-ip | 10.20.0.240 | DSNS LoadBalancer IP address |
| --dry-run | false | Print commands without executing |

### What the script does not do

The following steps are manual because they require either
physical cluster access or secrets not present in the repo:

1. Generating the initial talosconfig (talosctl config generate)
2. Applying machine configs to individual nodes
3. Bootstrapping etcd (talosctl bootstrap)
4. Creating the talosconfig secret in the cluster
5. Applying the TalosCluster CR to import the cluster

The script prints a reminder for step 5 at completion.

### Idempotency

The script is safe to run on a cluster that has already been
partially or fully enabled. Every kubectl apply is idempotent.
Wait conditions are skipped if the target is already ready.
