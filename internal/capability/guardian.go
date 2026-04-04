// Package capability — Guardian domain capability implementations.
// rbac-provision: provisions ClusterRoles and ClusterRoleBindings on the
// target cluster from the current PermissionSnapshot. conductor-schema.md §6.
package capability

import (
	"context"
	"fmt"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// permissionSnapshotGVR is the GroupVersionResource for PermissionSnapshot.
// security.ontai.dev/v1alpha1/permissionsnapshots — guardian-schema.md §7.
var permissionSnapshotGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "permissionsnapshots",
}

// rbacProvisionHandler implements the rbac-provision named capability.
// Reads the current PermissionSnapshot for the cluster and applies the
// resulting ClusterRoles and ClusterRoleBindings via the kube RBAC client.
// conductor-schema.md §6, guardian-schema.md §7.
type rbacProvisionHandler struct{}

func (h *rbacProvisionHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.DynamicClient == nil || params.KubeClient == nil {
		return failureResult(runnerlib.CapabilityRBACProvision, now, runnerlib.ValidationFailure,
			"rbac-provision requires DynamicClient and KubeClient; both must be non-nil"), nil
	}

	// Read the PermissionSnapshot for this cluster from security-system.
	// guardian-schema.md §7: one PermissionSnapshot per target cluster, lives in security-system.
	snapList, err := params.DynamicClient.
		Resource(permissionSnapshotGVR).
		Namespace("security-system").
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityRBACProvision, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list PermissionSnapshots in security-system: %v", err)), nil
	}

	// Find the snapshot whose spec.targetCluster matches the ClusterRef.
	var snapshotPrincipalPerms []interface{}
	var snapshotVersion string
	for _, item := range snapList.Items {
		tc, _, _ := unstructuredString(item.Object, "spec", "targetCluster")
		if tc != params.ClusterRef {
			continue
		}
		snapshotVersion, _, _ = unstructuredString(item.Object, "spec", "version")
		perms, _, _ := unstructuredList(item.Object, "spec", "principalPermissions")
		snapshotPrincipalPerms = perms
		break
	}

	if snapshotVersion == "" {
		return failureResult(runnerlib.CapabilityRBACProvision, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("no PermissionSnapshot found for cluster %q in security-system", params.ClusterRef)), nil
	}

	// Apply ClusterRoles from the snapshot's principalPermissions.
	// Each principal gets one ClusterRole named "seam:principal:{principalRef}".
	// guardian-schema.md §7 (PrincipalPermissionEntry schema).
	var steps []runnerlib.StepResult
	rbacClient := params.KubeClient.RbacV1()
	for _, entry := range snapshotPrincipalPerms {
		entryMap, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		principalRef, _, _ := unstructuredStringFromMap(entryMap, "principalRef")
		if principalRef == "" {
			continue
		}
		roleName := fmt.Sprintf("seam:principal:%s", principalRef)
		ops, _, _ := unstructuredList(entryMap, "allowedOperations")
		rules := buildPolicyRules(ops)

		cr := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name: roleName,
				Labels: map[string]string{
					"ontai.dev/managed-by":    "conductor",
					"ontai.dev/cluster":       params.ClusterRef,
					"ontai.dev/snapshot-vers": snapshotVersion,
				},
			},
			Rules: rules,
		}

		stepStart := time.Now().UTC()
		if _, applyErr := rbacClient.ClusterRoles().Create(ctx, cr, metav1.CreateOptions{}); applyErr != nil {
			if errors.IsAlreadyExists(applyErr) {
				_, applyErr = rbacClient.ClusterRoles().Update(ctx, cr, metav1.UpdateOptions{})
			}
			if applyErr != nil {
				steps = append(steps, runnerlib.StepResult{
					Name:      fmt.Sprintf("apply-clusterrole:%s", principalRef),
					Status:    runnerlib.ResultFailed,
					StartedAt: stepStart,
					CompletedAt: time.Now().UTC(),
					Message: applyErr.Error(),
				})
				return failureResult(runnerlib.CapabilityRBACProvision, now, runnerlib.ExecutionFailure,
					fmt.Sprintf("apply ClusterRole %q: %v", roleName, applyErr)), nil
			}
		}
		steps = append(steps, runnerlib.StepResult{
			Name:        fmt.Sprintf("apply-clusterrole:%s", principalRef),
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   stepStart,
			CompletedAt: time.Now().UTC(),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityRBACProvision,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Steps:       steps,
		Artifacts:   []runnerlib.ArtifactRef{},
	}, nil
}

// ---------------------------------------------------------------------------
// Helpers — unstructured field access
// ---------------------------------------------------------------------------

// unstructuredString traverses obj via keys and returns the string value.
func unstructuredString(obj map[string]interface{}, keys ...string) (string, bool, error) {
	cur := obj
	for i, k := range keys {
		v, ok := cur[k]
		if !ok {
			return "", false, nil
		}
		if i == len(keys)-1 {
			s, ok := v.(string)
			return s, ok, nil
		}
		cur, ok = v.(map[string]interface{})
		if !ok {
			return "", false, fmt.Errorf("expected map at key %q", k)
		}
	}
	return "", false, nil
}

// unstructuredStringFromMap returns the string value at key from m.
func unstructuredStringFromMap(m map[string]interface{}, key string) (string, bool, error) {
	v, ok := m[key]
	if !ok {
		return "", false, nil
	}
	s, ok := v.(string)
	return s, ok, nil
}

// unstructuredList traverses obj via keys and returns the []interface{} value.
func unstructuredList(obj interface{}, keys ...string) ([]interface{}, bool, error) {
	cur, ok := obj.(map[string]interface{})
	if !ok {
		return nil, false, nil
	}
	for i, k := range keys {
		v, ok := cur[k]
		if !ok {
			return nil, false, nil
		}
		if i == len(keys)-1 {
			l, ok := v.([]interface{})
			return l, ok, nil
		}
		cur, ok = v.(map[string]interface{})
		if !ok {
			return nil, false, fmt.Errorf("expected map at key %q", k)
		}
	}
	return nil, false, nil
}

// buildPolicyRules converts AllowedOperation entries to Kubernetes PolicyRules.
// guardian-schema.md §7 — AllowedOperation: apiGroup, resource, verbs, clusters.
func buildPolicyRules(ops []interface{}) []rbacv1.PolicyRule {
	rules := make([]rbacv1.PolicyRule, 0, len(ops))
	for _, op := range ops {
		opMap, ok := op.(map[string]interface{})
		if !ok {
			continue
		}
		apiGroup, _, _ := unstructuredStringFromMap(opMap, "apiGroup")
		resource, _, _ := unstructuredStringFromMap(opMap, "resource")
		if resource == "" {
			continue
		}
		verbsRaw, _, _ := unstructuredList(opMap, "verbs")
		verbs := make([]string, 0, len(verbsRaw))
		for _, v := range verbsRaw {
			if s, ok := v.(string); ok {
				verbs = append(verbs, s)
			}
		}
		rules = append(rules, rbacv1.PolicyRule{
			APIGroups: []string{apiGroup},
			Resources: []string{resource},
			Verbs:     verbs,
		})
	}
	return rules
}

// failureResult is a convenience constructor for a failed OperationResultSpec.
func failureResult(capability string, startedAt time.Time, category runnerlib.FailureCategory, reason string) runnerlib.OperationResultSpec {
	now := time.Now().UTC()
	return runnerlib.OperationResultSpec{
		Capability:  capability,
		Status:      runnerlib.ResultFailed,
		StartedAt:   startedAt,
		CompletedAt: now,
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       []runnerlib.StepResult{},
		FailureReason: &runnerlib.FailureReason{
			Category: category,
			Reason:   reason,
		},
	}
}
