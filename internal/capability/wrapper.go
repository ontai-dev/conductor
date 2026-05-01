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
// infrastructure.ontai.dev/v1alpha1/infrastructurepackexecutions — wrapper-schema.md §4.
var packExecutionGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructurepackexecutions",
}

// clusterPackGVR is the GroupVersionResource for ClusterPack.
// infrastructure.ontai.dev/v1alpha1/infrastructureclusterpacks — wrapper-schema.md §4.
var clusterPackGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructureclusterpacks",
}

// packReceiptGVR is the GroupVersionResource for PackReceipt.
// Written to ont-system on the tenant cluster after successful pack apply.
// Sole local desired-state reference on tenant clusters. conductor-schema.md.
var packReceiptGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructurepackreceipts",
}

// Readiness-check GVRs. Used by isResourceReady to poll resource status.
var (
	deploymentGVR  = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	statefulSetGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	daemonSetGVR   = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}
	jobGVR         = schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
	cronJobGVR     = schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "cronjobs"}
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

	var ociRef, registryBaseURL, expectedChecksum, rbacDigest, workloadDigest, clusterScopedDigest, packSignature string
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
		registryBaseURL = url
		if digest != "" {
			ociRef = url + "@" + digest
		} else {
			ociRef = url
		}
		expectedChecksum, _, _ = unstructuredString(item.Object, "spec", "checksum")
		rbacDigest, _, _ = unstructuredString(item.Object, "spec", "rbacDigest")
		workloadDigest, _, _ = unstructuredString(item.Object, "spec", "workloadDigest")
		clusterScopedDigest, _, _ = unstructuredString(item.Object, "spec", "clusterScopedDigest")
		packSignature, _, _ = unstructuredString(item.Object, "status", "packSignature")

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

	// When rbacDigest is present, the ClusterPack uses the three-bucket OCI artifact
	// contract. RBAC manifests go through guardian /rbac-intake, cluster-scoped
	// manifests (webhooks, CRDs, etc.) are applied directly after intake, and workload
	// manifests are applied last. INV-004, wrapper-schema.md §4.
	// Legacy packs (no rbacDigest) fall through to single-layer path.
	if rbacDigest != "" {
		// Pass the base registry URL (without digest suffix) so executeSplitPath
		// can construct correct layer refs: baseURL@{rbac,clusterScoped,workload}Digest.
		return h.executeSplitPath(ctx, params, now, clusterPackName, clusterPackVersion, registryBaseURL, workloadDigest, rbacDigest, clusterScopedDigest, expectedChecksum, packSignature, executionStages)
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
	nsCreated, preflightErr := ensureNamespaces(ctx, tenantDynClient(params), allManifests)
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
			if err := applyParsedManifest(ctx, tenantDynClient(params), m); err != nil {
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
		if waitErr := waitForStageReady(ctx, tenantDynClient(params), allManifests); waitErr != nil {
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
		if err := writePackReceipt(ctx, tenantDynClient(params), clusterPackName, params.ClusterRef, packSignature, rbacDigest, workloadDigest, manifestsToDeployedResources(allManifests)); err != nil {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("write PackReceipt for %s: %v", clusterPackName, err)), nil
		}
		return runnerlib.OperationResultSpec{
			Capability:         runnerlib.CapabilityPackDeploy,
			Status:             runnerlib.ResultSucceeded,
			StartedAt:          now,
			CompletedAt:        time.Now().UTC(),
			Artifacts:          artifacts,
			ClusterPackRef:     clusterPackName,
			ClusterPackVersion: clusterPackVersion,
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
		applied, applyErr := applyStageManifests(ctx, tenantDynClient(params), stageMfsts)
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
		if waitErr := waitForStageReady(ctx, tenantDynClient(params), stageMfsts); waitErr != nil {
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

	if err := writePackReceipt(ctx, tenantDynClient(params), clusterPackName, params.ClusterRef, packSignature, rbacDigest, workloadDigest, manifestsToDeployedResources(allManifests)); err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("write PackReceipt for %s: %v", clusterPackName, err)), nil
	}
	return runnerlib.OperationResultSpec{
		Capability:         runnerlib.CapabilityPackDeploy,
		Status:             runnerlib.ResultSucceeded,
		StartedAt:          now,
		CompletedAt:        time.Now().UTC(),
		Artifacts:          artifacts,
		Steps:              stageSteps,
		ClusterPackRef:     clusterPackName,
		ClusterPackVersion: clusterPackVersion,
	}, nil
}

// ---------------------------------------------------------------------------
// Split path (two-layer OCI artifact) -- INV-004, wrapper-schema.md §4
// ---------------------------------------------------------------------------

// executeSplitPath implements the pack-deploy execution path for ClusterPack
// artifacts that carry separate OCI layers for RBAC, cluster-scoped, and workload
// resources (three-bucket split; Governor ruling 2026-04-22).
//
// Steps:
//  1. Pull RBAC layer.
//  2. Pull workload layer early (needed for Namespace pre-apply before rbac-intake).
//  3. Apply Namespace manifests from the workload layer to the target cluster.
//  4. Submit RBAC manifests to guardian intake.
//  5. Wait for RBACProfile provisioned.
//  6. Pull and apply cluster-scoped layer (if clusterScopedDigest present).
//     Webhooks, CRDs, and other cluster-scoped non-RBAC resources. Applied
//     directly by the Job using the wrapper-runner-cluster-scoped ClusterRole.
//  7. Apply all workload manifests (SSA idempotent -- Namespace already exists).
//  8. Wait for workload readiness.
//
// INV-004, wrapper-schema.md §4.
func (h *packDeployHandler) executeSplitPath(
	ctx context.Context,
	params ExecuteParams,
	now time.Time,
	componentName, clusterPackVersion, registryURL, workloadDigest, rbacDigest, clusterScopedDigest, expectedChecksum, packSignature string,
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
	if clusterScopedDigest != "" {
		artifacts = append(artifacts, runnerlib.ArtifactRef{
			Name:      "cluster-scoped-layer",
			Kind:      "OCIImage",
			Reference: registryURL + "@" + clusterScopedDigest,
		})
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

	// Step 2 — Pull workload layer early so Namespace manifests can be applied
	// before guardian rbac-intake. Absent on RBAC-only packs.
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

	// Step 3 — Apply Namespace manifests from the workload layer to the target
	// cluster BEFORE calling guardian rbac-intake. The compiler packbuild injects
	// Namespace manifests into the workload OCI layer for any namespace referenced
	// by RBAC resources that Helm does not emit. Applying them first ensures the
	// namespace exists when guardian applies ServiceAccounts into it.
	// These are from the pack artifact -- not synthesised at runtime. Governor-approved session/13.
	nsStart := time.Now().UTC()
	nsApplied := 0
	for _, m := range workloadManifests {
		if !strings.EqualFold(m.kind, "Namespace") {
			continue
		}
		if err := applyParsedManifest(ctx, tenantDynClient(params), m); err != nil {
			nsStep := runnerlib.StepResult{
				Name:        "apply-namespaces",
				Status:      runnerlib.ResultFailed,
				StartedAt:   nsStart,
				CompletedAt: time.Now().UTC(),
				Message:     fmt.Sprintf("apply Namespace %s: %v", m.name, err),
			}
			return runnerlib.OperationResultSpec{
				Capability:  runnerlib.CapabilityPackDeploy,
				Status:      runnerlib.ResultFailed,
				StartedAt:   now,
				CompletedAt: time.Now().UTC(),
				Artifacts:   artifacts,
				Steps:       []runnerlib.StepResult{pullRBACStep, pullWorkloadStep, nsStep},
				FailureReason: &runnerlib.FailureReason{
					Category:   runnerlib.ExecutionFailure,
					Reason:     fmt.Sprintf("apply Namespace %s: %v", m.name, err),
					FailedStep: "apply-namespaces",
				},
			}, nil
		}
		nsApplied++
	}
	applyNSStep := runnerlib.StepResult{
		Name:        "apply-namespaces",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   nsStart,
		CompletedAt: time.Now().UTC(),
		Message:     fmt.Sprintf("%d namespace(s) applied", nsApplied),
	}

	// Step 4 — Submit RBAC manifests to guardian intake. INV-004.
	intakeStart := time.Now().UTC()
	wrapped, intakeErr := params.GuardianClient.SubmitPackRBACLayer(ctx, componentName, rbacYAMLs, params.ClusterRef)
	if intakeErr != nil {
		steps := []runnerlib.StepResult{pullRBACStep, pullWorkloadStep, applyNSStep, {
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

	// Step 5 — Wait for guardian RBACProfile provisioned=true.
	waitStart := time.Now().UTC()
	if waitErr := params.GuardianClient.WaitForRBACProfileProvisioned(ctx, params.ClusterRef, componentName); waitErr != nil {
		steps := []runnerlib.StepResult{pullRBACStep, pullWorkloadStep, applyNSStep, intakeStep, {
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

	// Step 6 — Pull and apply cluster-scoped layer (if present). Webhooks, CRDs,
	// and other cluster-scoped non-RBAC resources. Applied directly by the Job
	// using the wrapper-runner-cluster-scoped ClusterRole. wrapper-schema.md §4.
	var clusterScopedManifests []parsedManifest
	var applyCSStep runnerlib.StepResult
	if clusterScopedDigest != "" {
		csRef := registryURL + "@" + clusterScopedDigest
		csStart := time.Now().UTC()
		csBlobs, err := params.OCIClient.PullManifests(ctx, csRef)
		if err != nil {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExternalDependencyFailure,
				fmt.Sprintf("pull cluster-scoped layer %s: %v", csRef, err)), nil
		}
		for blobIdx, blob := range csBlobs {
			yamlFiles, err := extractYAMLsFromTarGz(blob)
			if err != nil {
				return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
					fmt.Sprintf("extract cluster-scoped tar.gz blob[%d]: %v", blobIdx, err)), nil
			}
			for fileIdx, data := range yamlFiles {
				for docIdx, doc := range splitYAMLDocuments(data) {
					pm, err := parseManifestYAML(doc)
					if err != nil {
						return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
							fmt.Sprintf("parse cluster-scoped manifest blob[%d][%d][%d]: %v", blobIdx, fileIdx, docIdx, err)), nil
					}
					if pm != nil {
						clusterScopedManifests = append(clusterScopedManifests, *pm)
					}
				}
			}
		}
		csApplied := 0
		for _, m := range clusterScopedManifests {
			if err := applyParsedManifest(ctx, tenantDynClient(params), m); err != nil {
				applyCSStep = runnerlib.StepResult{
					Name:        "apply-cluster-scoped",
					Status:      runnerlib.ResultFailed,
					StartedAt:   csStart,
					CompletedAt: time.Now().UTC(),
					Message:     fmt.Sprintf("apply %s %s: %v", m.kind, m.name, err),
				}
				return runnerlib.OperationResultSpec{
					Capability:  runnerlib.CapabilityPackDeploy,
					Status:      runnerlib.ResultFailed,
					StartedAt:   now,
					CompletedAt: time.Now().UTC(),
					Artifacts:   artifacts,
					Steps:       []runnerlib.StepResult{pullRBACStep, pullWorkloadStep, applyNSStep, intakeStep, waitStep, applyCSStep},
					FailureReason: &runnerlib.FailureReason{
						Category:   runnerlib.ExecutionFailure,
						Reason:     fmt.Sprintf("apply cluster-scoped %s %s: %v", m.kind, m.name, err),
						FailedStep: "apply-cluster-scoped",
					},
				}, nil
			}
			csApplied++
		}
		applyCSStep = runnerlib.StepResult{
			Name:        "apply-cluster-scoped",
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   csStart,
			CompletedAt: time.Now().UTC(),
			Message:     fmt.Sprintf("%d cluster-scoped manifests applied", csApplied),
		}
	}

	// Step 7 — Apply all workload manifests. SSA is idempotent -- Namespace
	// manifests applied in step 3 are safely re-applied without effect.
	applyStart := time.Now().UTC()
	applied := 0
	for _, m := range workloadManifests {
		if err := applyParsedManifest(ctx, tenantDynClient(params), m); err != nil {
			steps := []runnerlib.StepResult{pullRBACStep, pullWorkloadStep, applyNSStep, intakeStep, waitStep}
			if clusterScopedDigest != "" {
				steps = append(steps, applyCSStep)
			}
			return runnerlib.OperationResultSpec{
				Capability:  runnerlib.CapabilityPackDeploy,
				Status:      runnerlib.ResultFailed,
				StartedAt:   now,
				CompletedAt: time.Now().UTC(),
				Artifacts:   artifacts,
				Steps:       steps,
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

	// Step 8 — Wait for workload Deployments, StatefulSets, and DaemonSets to become ready.
	readyStart := time.Now().UTC()
	if waitErr := waitForStageReady(ctx, tenantDynClient(params), workloadManifests); waitErr != nil {
		preSteps := []runnerlib.StepResult{pullRBACStep, pullWorkloadStep, applyNSStep, intakeStep, waitStep}
		if clusterScopedDigest != "" {
			preSteps = append(preSteps, applyCSStep)
		}
		preSteps = append(preSteps, applyStep)
		return runnerlib.OperationResultSpec{
			Capability:  runnerlib.CapabilityPackDeploy,
			Status:      runnerlib.ResultFailed,
			StartedAt:   now,
			CompletedAt: time.Now().UTC(),
			Artifacts:   artifacts,
			Steps: append(preSteps, runnerlib.StepResult{
				Name:        "wait-ready",
				Status:      runnerlib.ResultFailed,
				StartedAt:   readyStart,
				CompletedAt: time.Now().UTC(),
				Message:     waitErr.Error(),
			}),
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

	// Collect deployed resources for PackInstance.Status.DeployedResources.
	// RBAC via guardian intake, cluster-scoped and workload via direct apply.
	// wrapper-schema.md §3, Decision 11.
	var deployedResources []runnerlib.DeployedResource
	for _, yaml := range rbacYAMLs {
		if pm, err := parseManifestYAML([]byte(yaml)); err == nil && pm != nil {
			deployedResources = append(deployedResources, runnerlib.DeployedResource{
				APIVersion: pm.apiVersion,
				Kind:       pm.kind,
				Namespace:  pm.namespace,
				Name:       pm.name,
			})
		}
	}
	for _, m := range clusterScopedManifests {
		deployedResources = append(deployedResources, runnerlib.DeployedResource{
			APIVersion: m.apiVersion,
			Kind:       m.kind,
			Namespace:  m.namespace,
			Name:       m.name,
		})
	}
	for _, m := range workloadManifests {
		deployedResources = append(deployedResources, runnerlib.DeployedResource{
			APIVersion: m.apiVersion,
			Kind:       m.kind,
			Namespace:  m.namespace,
			Name:       m.name,
		})
	}

	finalSteps := []runnerlib.StepResult{pullRBACStep, pullWorkloadStep, applyNSStep, intakeStep, waitStep}
	if clusterScopedDigest != "" {
		finalSteps = append(finalSteps, applyCSStep)
	}
	finalSteps = append(finalSteps, applyStep, readyStep)

	if err := writePackReceipt(ctx, tenantDynClient(params), componentName, params.ClusterRef, packSignature, rbacDigest, workloadDigest, deployedResources); err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("write PackReceipt for %s: %v", componentName, err)), nil
	}

	return runnerlib.OperationResultSpec{
		Capability:         runnerlib.CapabilityPackDeploy,
		Status:             runnerlib.ResultSucceeded,
		StartedAt:          now,
		CompletedAt:        time.Now().UTC(),
		Artifacts:          artifacts,
		Steps:              finalSteps,
		DeployedResources:  deployedResources,
		ClusterPackRef:     componentName,
		ClusterPackVersion: clusterPackVersion,
		RBACDigest:         rbacDigest,
		WorkloadDigest:     workloadDigest,
	}, nil
}

// ---------------------------------------------------------------------------
// Namespace pre-creation
// ---------------------------------------------------------------------------

// tenantDynClient returns TenantDynamicClient when non-nil, falling back to
// DynamicClient. All pack-deploy manifest operations target the tenant cluster;
// only RunnerConfig/PackExecution/ClusterPack reads use the management client.
func tenantDynClient(params ExecuteParams) dynamic.Interface {
	if params.TenantDynamicClient != nil {
		return params.TenantDynamicClient
	}
	return params.DynamicClient
}

// writePackReceipt SSA-patches an InfrastructurePackReceipt into ont-system on
// the tenant cluster. The receipt is the sole local desired-state reference on
// tenant clusters and enables conductor role=tenant drift detection.
// Decision D (revised 2026-04-30), conductor-schema.md.
func writePackReceipt(ctx context.Context, tenantClient dynamic.Interface, clusterPackRef, targetCluster, packSignature, rbacDigest, workloadDigest string, resources []runnerlib.DeployedResource) error {
	deployedItems := make([]map[string]interface{}, 0, len(resources))
	for _, r := range resources {
		item := map[string]interface{}{
			"apiVersion": r.APIVersion,
			"kind":       r.Kind,
			"name":       r.Name,
		}
		if r.Namespace != "" {
			item["namespace"] = r.Namespace
		}
		deployedItems = append(deployedItems, item)
	}

	spec := map[string]interface{}{
		"clusterPackRef":    clusterPackRef,
		"targetClusterRef":  targetCluster,
		"packSignature":     packSignature,
		"signatureVerified": false,
		"rbacDigest":        rbacDigest,
		"workloadDigest":    workloadDigest,
	}
	if len(deployedItems) > 0 {
		spec["deployedResources"] = deployedItems
	}

	receiptJSON, err := json.Marshal(map[string]interface{}{
		"apiVersion": "infrastructure.ontai.dev/v1alpha1",
		"kind":       "InfrastructurePackReceipt",
		"metadata": map[string]interface{}{
			"name":      clusterPackRef,
			"namespace": "ont-system",
		},
		"spec": spec,
	})
	if err != nil {
		return fmt.Errorf("marshal PackReceipt: %w", err)
	}
	force := true
	_, err = tenantClient.Resource(packReceiptGVR).Namespace("ont-system").Patch(
		ctx,
		clusterPackRef,
		types.ApplyPatchType,
		receiptJSON,
		metav1.PatchOptions{FieldManager: "conductor-pack-deploy", Force: &force},
	)
	return err
}

// manifestsToDeployedResources converts parsed manifests to the DeployedResource
// type used by PackReceipt for drift detection inventory.
func manifestsToDeployedResources(manifests []parsedManifest) []runnerlib.DeployedResource {
	out := make([]runnerlib.DeployedResource, 0, len(manifests))
	for _, m := range manifests {
		out = append(out, runnerlib.DeployedResource{
			APIVersion: m.apiVersion,
			Kind:       m.kind,
			Namespace:  m.namespace,
			Name:       m.name,
		})
	}
	return out
}

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
	case "Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "PersistentVolumeClaim":
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

	case "Job":
		obj, err := dynClient.Resource(jobGVR).Namespace(m.namespace).
			Get(ctx, m.name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get Job %s/%s: %w", m.namespace, m.name, err)
		}
		// A Job is complete when succeeded >= spec.completions (default 1).
		completions, ok, _ := unstructuredInt64(obj.Object, "spec", "completions")
		if !ok {
			completions = 1
		}
		succeeded, _, _ := unstructuredInt64(obj.Object, "status", "succeeded")
		// Also treat a failed Job as terminal to avoid infinite wait.
		conditions, _, _ := unstructuredList(obj.Object, "status", "conditions")
		for _, c := range conditions {
			cmap, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			if cmap["type"] == "Failed" && cmap["status"] == "True" {
				return false, fmt.Errorf("Job %s/%s has condition Failed=True", m.namespace, m.name)
			}
		}
		return succeeded >= completions, nil

	case "CronJob":
		// A CronJob has no terminal ready state -- it schedules Jobs on a recurring basis.
		// Ready when: the resource exists and spec.suspend is not true.
		obj, err := dynClient.Resource(cronJobGVR).Namespace(m.namespace).
			Get(ctx, m.name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get CronJob %s/%s: %w", m.namespace, m.name, err)
		}
		spec, _ := obj.Object["spec"].(map[string]interface{})
		if suspend, _ := spec["suspend"].(bool); suspend {
			return false, fmt.Errorf("CronJob %s/%s has spec.suspend=true", m.namespace, m.name)
		}
		return true, nil

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
// resource name (e.g., "deployments"). Three pluralization rules apply:
//
//  1. Kinds ending in 's' (e.g., "IngressClass", "Ingress") → append "es"
//     ("ingressclasses", "ingresses").
//  2. Kinds ending in 'y' preceded by a consonant (e.g., "NetworkPolicy") →
//     replace 'y' with "ies" ("networkpolicies").
//  3. All other kinds → append "s".
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
	s := string(lower)
	switch {
	case len(s) > 0 && s[len(s)-1] == 's':
		return s + "es"
	case len(s) > 1 && s[len(s)-1] == 'y' && !isVowel(s[len(s)-2]):
		return s[:len(s)-1] + "ies"
	default:
		return s + "s"
	}
}

// isVowel returns true for lowercase ASCII vowels.
func isVowel(c byte) bool {
	return c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u'
}
