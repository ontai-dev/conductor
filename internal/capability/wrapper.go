// Package capability — Wrapper domain capability implementations.
// pack-deploy: fetches a ClusterPack OCI artifact and applies it to the target
// cluster via server-side apply, honouring spec.executionOrder stage ordering.
// conductor-schema.md §6, wrapper-schema.md §4.
package capability

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// namespaceGVR is the GroupVersionResource for Kubernetes Namespace resources.
// Used by ensureNamespaces to pre-create missing namespaces before manifest apply.
var namespaceGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}

// packExecutionGVR is the GroupVersionResource for PackExecution.
// infra.ontai.dev/v1alpha1/packexecutions — wrapper-schema.md §4.
var packExecutionGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "packexecutions",
}

// clusterPackGVR is the GroupVersionResource for ClusterPack.
// infra.ontai.dev/v1alpha1/clusterpacks — wrapper-schema.md §4.
var clusterPackGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "clusterpacks",
}

// Readiness-check GVRs. Used by isResourceReady to poll resource status.
var (
	deploymentGVR  = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	statefulSetGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	daemonSetGVR   = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}
	pvcGVR         = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
)

// stageReadinessPollInterval is the polling frequency when waiting for stage
// resources to reach their ready states. wrapper-schema.md §2.2.
const stageReadinessPollInterval = 5 * time.Second

// packDeployHandler implements the pack-deploy named capability.
// Fetches the ClusterPack OCI artifact, verifies the content checksum, and
// applies manifests to the target cluster honouring spec.executionOrder stage
// ordering. When executionOrder is absent, falls back to single-pass apply.
// conductor-schema.md §6, wrapper-schema.md §4.
type packDeployHandler struct{}

func (h *packDeployHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.OCIClient == nil || params.KubeClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
			"pack-deploy requires OCIClient, KubeClient, and DynamicClient"), nil
	}

	// Read the PackExecution CR to get the ClusterPack reference.
	// PackExecutions live in seam-tenant-{clusterRef}. wrapper-schema.md §4.
	peTenantNS := "seam-tenant-" + params.ClusterRef
	peList, err := params.DynamicClient.Resource(packExecutionGVR).Namespace(peTenantNS).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list PackExecution in %s: %v", peTenantNS, err)), nil
	}

	var clusterPackName, clusterPackVersion string
	for _, item := range peList.Items {
		targetCluster, _, _ := unstructuredString(item.Object, "spec", "targetClusterRef")
		if targetCluster != params.ClusterRef {
			continue
		}
		clusterPackName, _, _ = unstructuredString(item.Object, "spec", "clusterPackRef", "name")
		clusterPackVersion, _, _ = unstructuredString(item.Object, "spec", "clusterPackRef", "version")
		break
	}

	if clusterPackName == "" {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no PackExecution CR targeting cluster %q found in %s", params.ClusterRef, peTenantNS)), nil
	}

	// Read the ClusterPack to get OCI registry reference, checksum, and
	// spec.executionOrder for staged deployment. wrapper-schema.md §3.
	cpList, err := params.DynamicClient.Resource(clusterPackGVR).Namespace(peTenantNS).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list ClusterPack in %s: %v", peTenantNS, err)), nil
	}

	var ociRef, expectedChecksum, rbacDigest, workloadDigest string
	var executionStages []string // stage names in declared order; empty → single-pass fallback
	for _, item := range cpList.Items {
		name, _, _ := unstructuredString(item.Object, "metadata", "name")
		version, _, _ := unstructuredString(item.Object, "spec", "version")
		if name != clusterPackName || version != clusterPackVersion {
			continue
		}
		url, _, _ := unstructuredString(item.Object, "spec", "registryRef", "url")
		digest, _, _ := unstructuredString(item.Object, "spec", "registryRef", "digest")
		if url == "" {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("ClusterPack %s/%s has no registryRef.url", clusterPackName, clusterPackVersion)), nil
		}
		if digest != "" {
			ociRef = url + "@" + digest
		} else {
			ociRef = url
		}
		expectedChecksum, _, _ = unstructuredString(item.Object, "spec", "checksum")
		rbacDigest, _, _ = unstructuredString(item.Object, "spec", "rbacDigest")
		workloadDigest, _, _ = unstructuredString(item.Object, "spec", "workloadDigest")

		// Read spec.executionOrder — ordered list of stage objects with a "name" field.
		// When present and non-empty, staged execution is used. wrapper-schema.md §2.2.
		order, ok, _ := unstructuredList(item.Object, "spec", "executionOrder")
		if ok {
			for _, s := range order {
				stageMap, ok := s.(map[string]interface{})
				if !ok {
					continue
				}
				stageName, _ := stageMap["name"].(string)
				if stageName != "" {
					executionStages = append(executionStages, stageName)
				}
			}
		}
		break
	}

	if ociRef == "" {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
			fmt.Sprintf("ClusterPack %s/%s has no registryRef", clusterPackName, clusterPackVersion)), nil
	}

	// When rbacDigest is present, the ClusterPack uses the two-layer OCI artifact
	// contract. RBAC manifests are submitted to guardian /rbac-intake/pack and the
	// workload layer is applied separately after guardian acknowledges. INV-004,
	// wrapper-schema.md §4. Legacy packs (no rbacDigest) fall through to single-layer path.
	if rbacDigest != "" {
		return h.executeSplitPath(ctx, params, now, clusterPackName, ociRef, workloadDigest, rbacDigest, expectedChecksum, executionStages)
	}

	// Step 1 — Fetch manifests from OCI registry.
	step1Start := time.Now().UTC()
	blobs, err := params.OCIClient.PullManifests(ctx, ociRef)
	if err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExternalDependencyFailure,
			fmt.Sprintf("PullManifests(%s): %v", ociRef, err)), nil
	}
	step1End := time.Now().UTC()

	// Verify content checksum if provided by the ClusterPack CR.
	if expectedChecksum != "" {
		actualChecksum := strings.TrimPrefix(computeManifestChecksum(blobs), "sha256:")
		if actualChecksum != expectedChecksum {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
				fmt.Sprintf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)), nil
		}
	}

	// Step 2 — Decompress tar.gz artifact blobs and parse YAML manifests.
	// Each blob is an OCI layer — a tar.gz containing pre-rendered YAML files.
	step2Start := time.Now().UTC()
	var allManifests []parsedManifest
	for blobIdx, blob := range blobs {
		yamlFiles, err := extractYAMLsFromTarGz(blob)
		if err != nil {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("extract tar.gz artifact blob[%d]: %v", blobIdx, err)), nil
		}
		for fileIdx, yamlData := range yamlFiles {
			for docIdx, doc := range splitYAMLDocuments(yamlData) {
				pm, err := parseManifestYAML(doc)
				if err != nil {
					return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
						fmt.Sprintf("parse manifest blob[%d][%d][%d]: %v", blobIdx, fileIdx, docIdx, err)), nil
				}
				if pm == nil {
					continue
				}
				allManifests = append(allManifests, *pm)
			}
		}
	}

	pullStep := runnerlib.StepResult{
		Name:        "pull-manifests",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   step1Start,
		CompletedAt: step1End,
		Message:     fmt.Sprintf("%d manifests fetched", len(allManifests)),
	}

	// Pre-flight step: ensure all namespaces referenced by manifests exist.
	// Many Helm charts (e.g., ingress-nginx) render resources into a namespace that
	// is not itself declared as a Namespace manifest. wrapper-schema.md §4.
	preflightStart := time.Now().UTC()
	nsCreated, preflightErr := ensureNamespaces(ctx, params.DynamicClient, allManifests)
	preflightEnd := time.Now().UTC()
	if preflightErr != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("pre-flight namespace creation failed: %v", preflightErr)), nil
	}
	preflightStep := runnerlib.StepResult{
		Name:        "ensure-namespaces",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   preflightStart,
		CompletedAt: preflightEnd,
		Message:     fmt.Sprintf("%d namespace(s) pre-created", nsCreated),
	}

	artifacts := []runnerlib.ArtifactRef{{
		Name:      "cluster-pack",
		Kind:      "OCIImage",
		Reference: ociRef,
		Checksum:  expectedChecksum,
	}}

	// Single-pass fallback: when spec.executionOrder is absent or empty, apply all
	// manifests in declaration order. Preserves backward compatibility for packs
	// that do not declare an execution order. wrapper-schema.md §2.2.
	if len(executionStages) == 0 {
		applied := 0
		for _, m := range allManifests {
			if err := applyParsedManifest(ctx, params.DynamicClient, m); err != nil {
				return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
					fmt.Sprintf("apply %s %s/%s: %v", m.kind, m.namespace, m.name, err)), nil
			}
			applied++
		}
		step2End := time.Now().UTC()
		applyStep := runnerlib.StepResult{
			Name:        "apply-manifests",
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   step2Start,
			CompletedAt: step2End,
			Message:     fmt.Sprintf("%d manifests applied", applied),
		}
		readyStart := time.Now().UTC()
		if waitErr := waitForStageReady(ctx, params.DynamicClient, allManifests); waitErr != nil {
			return runnerlib.OperationResultSpec{
				Capability:  runnerlib.CapabilityPackDeploy,
				Status:      runnerlib.ResultFailed,
				StartedAt:   now,
				CompletedAt: time.Now().UTC(),
				Artifacts:   artifacts,
				Steps: []runnerlib.StepResult{
					pullStep,
					preflightStep,
					applyStep,
					{
						Name:        "wait-ready",
						Status:      runnerlib.ResultFailed,
						StartedAt:   readyStart,
						CompletedAt: time.Now().UTC(),
						Message:     waitErr.Error(),
					},
				},
				FailureReason: &runnerlib.FailureReason{
					Category:   runnerlib.ExecutionFailure,
					Reason:     fmt.Sprintf("readiness wait failed: %v", waitErr),
					FailedStep: "wait-ready",
				},
			}, nil
		}
		return runnerlib.OperationResultSpec{
			Capability:  runnerlib.CapabilityPackDeploy,
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   now,
			CompletedAt: time.Now().UTC(),
			Artifacts:   artifacts,
			Steps: []runnerlib.StepResult{
				pullStep,
				preflightStep,
				applyStep,
				{
					Name:        "wait-ready",
					Status:      runnerlib.ResultSucceeded,
					StartedAt:   readyStart,
					CompletedAt: time.Now().UTC(),
					Message:     fmt.Sprintf("%d manifests applied and ready", applied),
				},
			},
		}, nil
	}

	// Staged execution: classify manifests by Kind into their canonical stages,
	// then apply each declared stage in order, waiting for readiness before
	// proceeding to the next stage. wrapper-schema.md §2.2.
	//
	// Stage → Kind mapping:
	//   rbac:      ClusterRole, ClusterRoleBinding, Role, RoleBinding, ServiceAccount
	//   storage:   PersistentVolumeClaim, StorageClass
	//   stateful:  StatefulSet, DaemonSet, Job, CronJob
	//   stateless: Deployment, Service, ConfigMap, Secret, Ingress, everything else
	byStage := make(map[string][]parsedManifest)
	for _, m := range allManifests {
		stage := stageForKind(m.kind)
		byStage[stage] = append(byStage[stage], m)
	}

	stageSteps := []runnerlib.StepResult{pullStep, preflightStep}

	for _, stageName := range executionStages {
		stageStart := time.Now().UTC()
		stageMfsts := byStage[stageName] // empty slice when no manifests for this stage

		// Apply this stage's manifests concurrently via server-side apply.
		applied, applyErr := applyStageManifests(ctx, params.DynamicClient, stageMfsts)
		if applyErr != nil {
			stageSteps = append(stageSteps, runnerlib.StepResult{
				Name:        stageName,
				Status:      runnerlib.ResultFailed,
				StartedAt:   stageStart,
				CompletedAt: time.Now().UTC(),
				Message:     applyErr.Error(),
			})
			return runnerlib.OperationResultSpec{
				Capability:  runnerlib.CapabilityPackDeploy,
				Status:      runnerlib.ResultFailed,
				StartedAt:   now,
				CompletedAt: time.Now().UTC(),
				Artifacts:   artifacts,
				Steps:       stageSteps,
				FailureReason: &runnerlib.FailureReason{
					Category:   runnerlib.ExecutionFailure,
					Reason:     fmt.Sprintf("stage %q apply failed: %v", stageName, applyErr),
					FailedStep: stageName,
				},
			}, nil
		}

		// Wait for all readiness-sensitive resources in this stage before proceeding.
		if waitErr := waitForStageReady(ctx, params.DynamicClient, stageMfsts); waitErr != nil {
			stageSteps = append(stageSteps, runnerlib.StepResult{
				Name:        stageName,
				Status:      runnerlib.ResultFailed,
				StartedAt:   stageStart,
				CompletedAt: time.Now().UTC(),
				Message:     fmt.Sprintf("readiness wait: %v", waitErr),
			})
			return runnerlib.OperationResultSpec{
				Capability:  runnerlib.CapabilityPackDeploy,
				Status:      runnerlib.ResultFailed,
				StartedAt:   now,
				CompletedAt: time.Now().UTC(),
				Artifacts:   artifacts,
				Steps:       stageSteps,
				FailureReason: &runnerlib.FailureReason{
					Category:   runnerlib.ExecutionFailure,
					Reason:     fmt.Sprintf("stage %q readiness wait failed: %v", stageName, waitErr),
					FailedStep: stageName,
				},
			}, nil
		}

		stageSteps = append(stageSteps, runnerlib.StepResult{
			Name:        stageName,
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   stageStart,
			CompletedAt: time.Now().UTC(),
			Message:     fmt.Sprintf("%d manifests applied and ready", applied),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityPackDeploy,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   artifacts,
		Steps:       stageSteps,
	}, nil
}

// ---------------------------------------------------------------------------
// Split path (two-layer OCI artifact) -- INV-004, wrapper-schema.md §4
// ---------------------------------------------------------------------------

// executeSplitPath implements the pack-deploy execution path for ClusterPack
// artifacts that carry separate RBAC and workload OCI layers (rbacDigest set).
// Steps: (1) pull RBAC layer, (2) submit to guardian intake, (3) wait for
// RBACProfile provisioned, (4) pull workload layer, (5) ensureNamespaces,
// (6) apply workload. INV-004, wrapper-schema.md §4.
func (h *packDeployHandler) executeSplitPath(
	ctx context.Context,
	params ExecuteParams,
	now time.Time,
	componentName, registryURL, workloadDigest, rbacDigest, expectedChecksum string,
	executionStages []string,
) (runnerlib.OperationResultSpec, error) {
	if params.GuardianClient == nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
			"pack-deploy split path requires GuardianClient; rbacDigest is set but no client provided (INV-004)"), nil
	}

	rbacRef := registryURL + "@" + rbacDigest
	artifacts := []runnerlib.ArtifactRef{
		{Name: "rbac-layer", Kind: "OCIImage", Reference: rbacRef},
	}
	if workloadDigest != "" {
		artifacts = append(artifacts, runnerlib.ArtifactRef{
			Name:      "workload-layer",
			Kind:      "OCIImage",
			Reference: registryURL + "@" + workloadDigest,
			Checksum:  expectedChecksum,
		})
	}

	// Step 1 — Pull RBAC layer.
	step1Start := time.Now().UTC()
	rbacBlobs, err := params.OCIClient.PullManifests(ctx, rbacRef)
	if err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExternalDependencyFailure,
			fmt.Sprintf("pull RBAC layer %s: %v", rbacRef, err)), nil
	}
	var rbacYAMLs []string
	for blobIdx, blob := range rbacBlobs {
		yamlFiles, err := extractYAMLsFromTarGz(blob)
		if err != nil {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("extract RBAC tar.gz blob[%d]: %v", blobIdx, err)), nil
		}
		for _, data := range yamlFiles {
			for _, doc := range splitYAMLDocuments(data) {
				if len(doc) > 0 {
					rbacYAMLs = append(rbacYAMLs, string(doc))
				}
			}
		}
	}
	pullRBACStep := runnerlib.StepResult{
		Name:        "pull-rbac-layer",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   step1Start,
		CompletedAt: time.Now().UTC(),
		Message:     fmt.Sprintf("%d RBAC manifests fetched", len(rbacYAMLs)),
	}

	// Step 2 — Submit RBAC manifests to guardian intake. INV-004.
	intakeStart := time.Now().UTC()
	wrapped, intakeErr := params.GuardianClient.SubmitPackRBACLayer(ctx, componentName, rbacYAMLs, params.ClusterRef)
	if intakeErr != nil {
		steps := []runnerlib.StepResult{pullRBACStep, {
			Name:        "rbac-intake",
			Status:      runnerlib.ResultFailed,
			StartedAt:   intakeStart,
			CompletedAt: time.Now().UTC(),
			Message:     intakeErr.Error(),
		}}
		return runnerlib.OperationResultSpec{
			Capability:  runnerlib.CapabilityPackDeploy,
			Status:      runnerlib.ResultFailed,
			StartedAt:   now,
			CompletedAt: time.Now().UTC(),
			Artifacts:   artifacts,
			Steps:       steps,
			FailureReason: &runnerlib.FailureReason{
				Category:   runnerlib.ExecutionFailure,
				Reason:     fmt.Sprintf("guardian rbac-intake failed: %v", intakeErr),
				FailedStep: "rbac-intake",
			},
		}, nil
	}
	intakeStep := runnerlib.StepResult{
		Name:        "rbac-intake",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   intakeStart,
		CompletedAt: time.Now().UTC(),
		Message:     fmt.Sprintf("%d RBAC resources wrapped by guardian", wrapped),
	}

	// Step 3 — Wait for guardian RBACProfile provisioned=true.
	waitStart := time.Now().UTC()
	if waitErr := params.GuardianClient.WaitForRBACProfileProvisioned(ctx, params.ClusterRef, componentName); waitErr != nil {
		steps := []runnerlib.StepResult{pullRBACStep, intakeStep, {
			Name:        "wait-rbac-profile",
			Status:      runnerlib.ResultFailed,
			StartedAt:   waitStart,
			CompletedAt: time.Now().UTC(),
			Message:     waitErr.Error(),
		}}
		return runnerlib.OperationResultSpec{
			Capability:  runnerlib.CapabilityPackDeploy,
			Status:      runnerlib.ResultFailed,
			StartedAt:   now,
			CompletedAt: time.Now().UTC(),
			Artifacts:   artifacts,
			Steps:       steps,
			FailureReason: &runnerlib.FailureReason{
				Category:   runnerlib.ExecutionFailure,
				Reason:     fmt.Sprintf("wait RBACProfile provisioned: %v", waitErr),
				FailedStep: "wait-rbac-profile",
			},
		}, nil
	}
	waitStep := runnerlib.StepResult{
		Name:        "wait-rbac-profile",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   waitStart,
		CompletedAt: time.Now().UTC(),
		Message:     "RBACProfile provisioned=true",
	}

	// Step 4 — Pull workload layer (absent on RBAC-only packs).
	var workloadManifests []parsedManifest
	var pullWorkloadStep runnerlib.StepResult
	if workloadDigest != "" {
		wRef := registryURL + "@" + workloadDigest
		wStart := time.Now().UTC()
		wBlobs, err := params.OCIClient.PullManifests(ctx, wRef)
		if err != nil {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExternalDependencyFailure,
				fmt.Sprintf("pull workload layer %s: %v", wRef, err)), nil
		}
		for blobIdx, blob := range wBlobs {
			yamlFiles, err := extractYAMLsFromTarGz(blob)
			if err != nil {
				return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
					fmt.Sprintf("extract workload tar.gz blob[%d]: %v", blobIdx, err)), nil
			}
			for fileIdx, data := range yamlFiles {
				for docIdx, doc := range splitYAMLDocuments(data) {
					pm, err := parseManifestYAML(doc)
					if err != nil {
						return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
							fmt.Sprintf("parse workload manifest blob[%d][%d][%d]: %v", blobIdx, fileIdx, docIdx, err)), nil
					}
					if pm != nil {
						workloadManifests = append(workloadManifests, *pm)
					}
				}
			}
		}
		pullWorkloadStep = runnerlib.StepResult{
			Name:        "pull-workload-layer",
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   wStart,
			CompletedAt: time.Now().UTC(),
			Message:     fmt.Sprintf("%d workload manifests fetched", len(workloadManifests)),
		}
	}

	// Step 5 — Pre-create namespaces referenced by workload manifests.
	nsStart := time.Now().UTC()
	nsCreated, nsErr := ensureNamespaces(ctx, params.DynamicClient, workloadManifests)
	nsStep := runnerlib.StepResult{
		Name:        "ensure-namespaces",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   nsStart,
		CompletedAt: time.Now().UTC(),
		Message:     fmt.Sprintf("%d namespace(s) pre-created", nsCreated),
	}
	if nsErr != nil {
		nsStep.Status = runnerlib.ResultFailed
		nsStep.Message = nsErr.Error()
		return runnerlib.OperationResultSpec{
			Capability:  runnerlib.CapabilityPackDeploy,
			Status:      runnerlib.ResultFailed,
			StartedAt:   now,
			CompletedAt: time.Now().UTC(),
			Artifacts:   artifacts,
			Steps:       []runnerlib.StepResult{pullRBACStep, intakeStep, waitStep, pullWorkloadStep, nsStep},
			FailureReason: &runnerlib.FailureReason{
				Category:   runnerlib.ExecutionFailure,
				Reason:     fmt.Sprintf("pre-flight namespace creation failed: %v", nsErr),
				FailedStep: "ensure-namespaces",
			},
		}, nil
	}

	// Step 6 — Apply workload manifests (single-pass; staged apply deferred to future).
	applyStart := time.Now().UTC()
	applied := 0
	for _, m := range workloadManifests {
		if err := applyParsedManifest(ctx, params.DynamicClient, m); err != nil {
			return runnerlib.OperationResultSpec{
				Capability:  runnerlib.CapabilityPackDeploy,
				Status:      runnerlib.ResultFailed,
				StartedAt:   now,
				CompletedAt: time.Now().UTC(),
				Artifacts:   artifacts,
				Steps:       []runnerlib.StepResult{pullRBACStep, intakeStep, waitStep, pullWorkloadStep, nsStep},
				FailureReason: &runnerlib.FailureReason{
					Category:   runnerlib.ExecutionFailure,
					Reason:     fmt.Sprintf("apply workload %s %s/%s: %v", m.kind, m.namespace, m.name, err),
					FailedStep: "apply-workload",
				},
			}, nil
		}
		applied++
	}
	applyStep := runnerlib.StepResult{
		Name:        "apply-workload",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   applyStart,
		CompletedAt: time.Now().UTC(),
		Message:     fmt.Sprintf("%d workload manifests applied", applied),
	}

	// Step 7 — Wait for workload Deployments, StatefulSets, and DaemonSets to become ready.
	readyStart := time.Now().UTC()
	if waitErr := waitForStageReady(ctx, params.DynamicClient, workloadManifests); waitErr != nil {
		return runnerlib.OperationResultSpec{
			Capability:  runnerlib.CapabilityPackDeploy,
			Status:      runnerlib.ResultFailed,
			StartedAt:   now,
			CompletedAt: time.Now().UTC(),
			Artifacts:   artifacts,
			Steps: []runnerlib.StepResult{
				pullRBACStep, intakeStep, waitStep, pullWorkloadStep, nsStep, applyStep,
				{
					Name:        "wait-ready",
					Status:      runnerlib.ResultFailed,
					StartedAt:   readyStart,
					CompletedAt: time.Now().UTC(),
					Message:     waitErr.Error(),
				},
			},
			FailureReason: &runnerlib.FailureReason{
				Category:   runnerlib.ExecutionFailure,
				Reason:     fmt.Sprintf("workload readiness wait failed: %v", waitErr),
				FailedStep: "wait-ready",
			},
		}, nil
	}
	readyStep := runnerlib.StepResult{
		Name:        "wait-ready",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   readyStart,
		CompletedAt: time.Now().UTC(),
		Message:     fmt.Sprintf("%d workload manifests applied and ready", applied),
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityPackDeploy,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   artifacts,
		Steps:       []runnerlib.StepResult{pullRBACStep, intakeStep, waitStep, pullWorkloadStep, nsStep, applyStep, readyStep},
	}, nil
}

// ---------------------------------------------------------------------------
// Namespace pre-creation
// ---------------------------------------------------------------------------

// ensureNamespaces scans manifests for namespace-scoped resources and
// pre-creates any referenced namespace that is not already represented as
// an explicit Namespace manifest in the set. This prevents the first SSA patch
// from failing with "namespace not found" when the Helm chart does not include
// a Namespace manifest for its own namespace. wrapper-schema.md §4.
func ensureNamespaces(ctx context.Context, dynClient dynamic.Interface, manifests []parsedManifest) (int, error) {
	explicit := make(map[string]struct{})
	for _, m := range manifests {
		if strings.EqualFold(m.kind, "Namespace") && m.namespace == "" {
			explicit[m.name] = struct{}{}
		}
	}
	needed := make(map[string]struct{})
	for _, m := range manifests {
		if m.namespace == "" {
			continue
		}
		if _, ok := explicit[m.namespace]; ok {
			continue
		}
		needed[m.namespace] = struct{}{}
	}
	created := 0
	for ns := range needed {
		nsJSON := []byte(fmt.Sprintf(
			`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":%q}}`, ns))
		_, err := dynClient.Resource(namespaceGVR).Patch(
			ctx, ns, types.ApplyPatchType, nsJSON,
			metav1.PatchOptions{FieldManager: "conductor-pack-deploy"},
		)
		if err != nil {
			return created, fmt.Errorf("pre-create namespace %q: %w", ns, err)
		}
		created++
	}
	return created, nil
}

// ---------------------------------------------------------------------------
// Manifest parsing
// ---------------------------------------------------------------------------

// parsedManifest is a decoded Kubernetes manifest ready for server-side apply.
type parsedManifest struct {
	apiVersion string
	kind       string
	name       string
	namespace  string
	jsonData   []byte
}

// parseManifestYAML converts a raw YAML manifest into a parsedManifest.
// Returns (nil, nil) for blank or incomplete inputs. Returns an error on
// parse failure. conductor-schema.md §9.
func parseManifestYAML(data []byte) (*parsedManifest, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	jsonData, err := sigsyaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("yaml-to-json: %w", err)
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(jsonData, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	apiVersion, _, _ := unstructuredStringFromMap(obj, "apiVersion")
	kind, _, _ := unstructuredStringFromMap(obj, "kind")
	meta, _ := obj["metadata"].(map[string]interface{})
	name, _ := meta["name"].(string)
	namespace, _ := meta["namespace"].(string)
	if apiVersion == "" || kind == "" || name == "" {
		return nil, nil
	}
	return &parsedManifest{
		apiVersion: apiVersion,
		kind:       kind,
		name:       name,
		namespace:  namespace,
		jsonData:   jsonData,
	}, nil
}

// ---------------------------------------------------------------------------
// Stage classification
// ---------------------------------------------------------------------------

// stageForKind returns the canonical execution stage name for a Kubernetes Kind.
// The four stages are: rbac → storage → stateful → stateless.
// wrapper-schema.md §2.2, §3 ClusterPack spec.executionOrder.
func stageForKind(kind string) string {
	switch kind {
	case "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ServiceAccount":
		return "rbac"
	case "PersistentVolumeClaim", "StorageClass":
		return "storage"
	case "StatefulSet", "DaemonSet", "Job", "CronJob":
		return "stateful"
	default:
		// Deployment, Service, ConfigMap, Secret, Ingress, and all other kinds
		// fall into the stateless stage.
		return "stateless"
	}
}

// ---------------------------------------------------------------------------
// Apply helpers
// ---------------------------------------------------------------------------

// applyParsedManifest applies m to the cluster via server-side apply.
func applyParsedManifest(ctx context.Context, dynClient dynamic.Interface, m parsedManifest) error {
	gvr := gvrFromAPIVersionKind(m.apiVersion, m.kind)
	if m.namespace != "" {
		_, err := dynClient.Resource(gvr).Namespace(m.namespace).
			Patch(ctx, m.name, types.ApplyPatchType, m.jsonData, metav1.PatchOptions{
				FieldManager: "conductor-pack-deploy",
			})
		return err
	}
	_, err := dynClient.Resource(gvr).
		Patch(ctx, m.name, types.ApplyPatchType, m.jsonData, metav1.PatchOptions{
			FieldManager: "conductor-pack-deploy",
		})
	return err
}

// applyStageManifests applies all manifests in the slice concurrently via SSA.
// Returns the count of successfully applied manifests and the first error
// encountered (if any). All goroutines run to completion before returning.
func applyStageManifests(ctx context.Context, dynClient dynamic.Interface, manifests []parsedManifest) (int, error) {
	if len(manifests) == 0 {
		return 0, nil
	}

	type result struct{ err error }
	results := make([]result, len(manifests))

	var wg sync.WaitGroup
	for i, m := range manifests {
		wg.Add(1)
		go func(idx int, mfst parsedManifest) {
			defer wg.Done()
			results[idx].err = applyParsedManifest(ctx, dynClient, mfst)
		}(i, m)
	}
	wg.Wait()

	applied := 0
	var firstErr error
	for i, r := range results {
		if r.err == nil {
			applied++
		} else if firstErr == nil {
			firstErr = fmt.Errorf("apply %s %s/%s: %w",
				manifests[i].kind, manifests[i].namespace, manifests[i].name, r.err)
		}
	}
	return applied, firstErr
}

// ---------------------------------------------------------------------------
// Readiness waiting
// ---------------------------------------------------------------------------

// needsReadinessWait returns true for Kinds that require a readiness poll
// after server-side apply before the next stage may proceed.
// wrapper-schema.md §2.2.
func needsReadinessWait(kind string) bool {
	switch kind {
	case "Deployment", "StatefulSet", "DaemonSet", "PersistentVolumeClaim":
		return true
	}
	return false
}

// isResourceReady checks whether a single resource has reached its ready state.
// Returns (false, nil) when the resource exists but is not yet ready.
// Returns (true, nil) when ready. Returns (false, err) on API errors.
// Returns (true, nil) for Kinds with no readiness requirement.
func isResourceReady(ctx context.Context, dynClient dynamic.Interface, m parsedManifest) (bool, error) {
	switch m.kind {
	case "Deployment":
		obj, err := dynClient.Resource(deploymentGVR).Namespace(m.namespace).
			Get(ctx, m.name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get Deployment %s/%s: %w", m.namespace, m.name, err)
		}
		conditions, ok, _ := unstructuredList(obj.Object, "status", "conditions")
		if !ok {
			return false, nil
		}
		for _, c := range conditions {
			cmap, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _ := cmap["type"].(string)
			condStatus, _ := cmap["status"].(string)
			if condType == "Available" && condStatus == "True" {
				return true, nil
			}
		}
		return false, nil

	case "StatefulSet":
		obj, err := dynClient.Resource(statefulSetGVR).Namespace(m.namespace).
			Get(ctx, m.name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get StatefulSet %s/%s: %w", m.namespace, m.name, err)
		}
		replicas, ok, _ := unstructuredInt64(obj.Object, "spec", "replicas")
		if !ok {
			replicas = 1 // Kubernetes default when replicas is unset
		}
		readyReplicas, _, _ := unstructuredInt64(obj.Object, "status", "readyReplicas")
		return replicas > 0 && readyReplicas >= replicas, nil

	case "DaemonSet":
		obj, err := dynClient.Resource(daemonSetGVR).Namespace(m.namespace).
			Get(ctx, m.name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get DaemonSet %s/%s: %w", m.namespace, m.name, err)
		}
		desired, _, _ := unstructuredInt64(obj.Object, "status", "desiredNumberScheduled")
		ready, _, _ := unstructuredInt64(obj.Object, "status", "numberReady")
		return desired > 0 && ready >= desired, nil

	case "PersistentVolumeClaim":
		obj, err := dynClient.Resource(pvcGVR).Namespace(m.namespace).
			Get(ctx, m.name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get PVC %s/%s: %w", m.namespace, m.name, err)
		}
		phase, _, _ := unstructuredString(obj.Object, "status", "phase")
		return phase == "Bound", nil
	}

	// No readiness check defined for this Kind — treat as immediately ready.
	return true, nil
}

// waitForStageReady polls until all readiness-sensitive resources in manifests
// have reached their ready states, or until ctx is cancelled.
// Resources that do not require a readiness check are skipped.
// wrapper-schema.md §2.2.
func waitForStageReady(ctx context.Context, dynClient dynamic.Interface, manifests []parsedManifest) error {
	// Collect only the resources that need readiness polling.
	var toCheck []parsedManifest
	for _, m := range manifests {
		if needsReadinessWait(m.kind) {
			toCheck = append(toCheck, m)
		}
	}
	if len(toCheck) == 0 {
		return nil // rbac/configmap/secret/service: applied is sufficient
	}

	ticker := time.NewTicker(stageReadinessPollInterval)
	defer ticker.Stop()

	for {
		allReady := true
		for _, m := range toCheck {
			ready, err := isResourceReady(ctx, dynClient, m)
			if err != nil {
				return fmt.Errorf("readiness check %s %s/%s: %w", m.kind, m.namespace, m.name, err)
			}
			if !ready {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("stage readiness wait exceeded context deadline: %w", ctx.Err())
		case <-ticker.C:
			// continue polling
		}
	}
}

// ---------------------------------------------------------------------------
// OCI artifact helpers
// ---------------------------------------------------------------------------

// extractYAMLsFromTarGz decompresses a gzip-compressed tar archive and returns
// the contents of all entries with a .yaml or .yml extension.
// This is the standard format for ClusterPack OCI artifacts: a tar.gz whose
// entries are pre-rendered Kubernetes YAML manifests. conductor-schema.md §9.
func extractYAMLsFromTarGz(data []byte) ([][]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close() //nolint:errcheck

	tr := tar.NewReader(gr)
	var result [][]byte
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar next: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		ext := strings.ToLower(filepath.Ext(hdr.Name))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", hdr.Name, err)
		}
		result = append(result, content)
	}
	return result, nil
}

// splitYAMLDocuments splits a multi-document YAML byte slice on "---" separators.
// OCI pack artifacts contain a single file with all manifests separated by "---".
// sigsyaml.YAMLToJSON only converts the first document, so each file must be
// split before calling parseManifestYAML. Returns individual non-empty documents.
func splitYAMLDocuments(data []byte) [][]byte {
	var docs [][]byte
	for _, part := range bytes.Split(data, []byte("\n---")) {
		trimmed := bytes.TrimPrefix(bytes.TrimSpace(part), []byte("---"))
		trimmed = bytes.TrimSpace(trimmed)
		if len(trimmed) > 0 {
			docs = append(docs, trimmed)
		}
	}
	return docs
}

// computeManifestChecksum computes SHA256 of the concatenated raw artifact bytes
// returned by PullManifests. For a single-layer OCI image this equals SHA256 of
// the artifact blob — the same value produced by "sha256sum <artifact>.tar.gz".
// Returns the checksum in "sha256:{hex}" format to match ClusterPack.spec.checksum.
func computeManifestChecksum(manifests [][]byte) string {
	h := sha256.New()
	for _, m := range manifests {
		h.Write(m)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// gvrFromAPIVersionKind derives a schema.GroupVersionResource from the
// apiVersion and kind strings in a Kubernetes manifest. This is a best-effort
// derivation: it lowercases the kind and appends 's' for the resource name.
// For resources where this convention doesn't hold (e.g., "endpoints"),
// the server-side apply call will fail with a 404 and be reported as an error.
func gvrFromAPIVersionKind(apiVersion, kind string) schema.GroupVersionResource {
	group, version := splitAPIVersion(apiVersion)
	resource := lowercasePlural(kind)
	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
}

// splitAPIVersion splits "group/version" into (group, version).
// Core API resources (e.g., "v1") return ("", "v1").
func splitAPIVersion(apiVersion string) (string, string) {
	for i, c := range apiVersion {
		if c == '/' {
			return apiVersion[:i], apiVersion[i+1:]
		}
	}
	return "", apiVersion
}

// lowercasePlural converts a Kind (e.g., "Deployment") to its lowercase plural
// resource name (e.g., "deployments") using the conventional 's' suffix.
func lowercasePlural(kind string) string {
	if len(kind) == 0 {
		return kind
	}
	lower := make([]byte, len(kind))
	for i := 0; i < len(kind); i++ {
		c := kind[i]
		if c >= 'A' && c <= 'Z' {
			lower[i] = c + 32
		} else {
			lower[i] = c
		}
	}
	return string(lower) + "s"
}
