// Package capability_test -- unit tests for WaitForRBACProfileProvisioned.
//
// Tests verify that the polling loop:
//  1. Returns nil immediately when RBACProfile is provisioned=true on first poll.
//  2. Retries until provisioned=true when provisioned=false on the first poll.
//  3. Retries on NotFound until the profile appears provisioned.
//  4. Returns error when timeout elapses and profile is never provisioned.
//  5. Error message names the profile and duration on timeout.
//  6. Non-NotFound API errors propagate immediately without retry.
//
// conductor-schema.md §6, guardian-schema.md §7, wrapper-schema.md §4.
package capability_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/ontai-dev/conductor/internal/capability"
)

var rbacProfileTestGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "rbacprofiles",
}

// newRBACProfileDynClient builds a fake dynamic client with RBACProfile GVR registered.
func newRBACProfileDynClient(objects ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: "RBACProfile"},
		&unstructured.Unstructured{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: "RBACProfileList"},
		&unstructured.UnstructuredList{},
	)
	client := dynamicfake.NewSimpleDynamicClient(s)
	for _, obj := range objects {
		ns := obj.GetNamespace()
		if ns == "" {
			_, _ = client.Resource(rbacProfileTestGVR).Create(context.Background(), obj, metav1.CreateOptions{})
		} else {
			_, _ = client.Resource(rbacProfileTestGVR).Namespace(ns).Create(context.Background(), obj, metav1.CreateOptions{})
		}
	}
	return client
}

// rbacProfileCR builds an RBACProfile with a Provisioned condition.
// provisioned=true sets status "True"; false sets "False".
func rbacProfileCR(name, namespace string, provisioned bool) *unstructured.Unstructured {
	status := "False"
	if provisioned {
		status = "True"
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "security.ontai.dev/v1alpha1",
		"kind":       "RBACProfile",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":   "Provisioned",
					"status": status,
				},
			},
		},
	}}
}

// TestWaitForRBACProfileProvisioned_ProvisionedOnFirstPoll verifies that the
// function returns nil immediately when the profile is already provisioned.
func TestWaitForRBACProfileProvisioned_ProvisionedOnFirstPoll(t *testing.T) {
	profile := rbacProfileCR("nginx-ingress", "seam-tenant-ccs-mgmt", true)
	client := newRBACProfileDynClient(profile)

	err := capability.WaitForRBACProfileProvisioned(
		context.Background(),
		client,
		"nginx-ingress",
		"seam-tenant-ccs-mgmt",
		2*time.Second,
		10*time.Millisecond,
	)
	if err != nil {
		t.Errorf("expected nil; got %v", err)
	}
}

// TestWaitForRBACProfileProvisioned_RetryUntilProvisioned verifies that the
// function retries when provisioned=false and returns nil after provisioned=true.
func TestWaitForRBACProfileProvisioned_RetryUntilProvisioned(t *testing.T) {
	// Start with provisioned=false. After the first Get, switch to true.
	profile := rbacProfileCR("nginx-ingress", "seam-tenant-ccs-mgmt", false)
	client := newRBACProfileDynClient(profile)

	calls := 0
	client.Fake.PrependReactor("get", "rbacprofiles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls < 2 {
			// First call: return false
			return true, rbacProfileCR("nginx-ingress", "seam-tenant-ccs-mgmt", false), nil
		}
		// Subsequent calls: return true
		return true, rbacProfileCR("nginx-ingress", "seam-tenant-ccs-mgmt", true), nil
	})

	err := capability.WaitForRBACProfileProvisioned(
		context.Background(),
		client,
		"nginx-ingress",
		"seam-tenant-ccs-mgmt",
		2*time.Second,
		10*time.Millisecond,
	)
	if err != nil {
		t.Errorf("expected nil after second poll; got %v", err)
	}
	if calls < 2 {
		t.Errorf("expected at least 2 Get calls; got %d", calls)
	}
}

// TestWaitForRBACProfileProvisioned_NotFoundRetried verifies that NotFound errors
// are retried until the profile appears with provisioned=true.
func TestWaitForRBACProfileProvisioned_NotFoundRetried(t *testing.T) {
	client := newRBACProfileDynClient() // empty -- profile absent initially

	calls := 0
	client.Fake.PrependReactor("get", "rbacprofiles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls < 3 {
			// First two calls: not found (proper API error so apierrors.IsNotFound passes)
			return true, nil, apierrors.NewNotFound(rbacProfileTestGVR.GroupResource(), "nginx-ingress")
		}
		return true, rbacProfileCR("nginx-ingress", "seam-tenant-ccs-mgmt", true), nil
	})

	err := capability.WaitForRBACProfileProvisioned(
		context.Background(),
		client,
		"nginx-ingress",
		"seam-tenant-ccs-mgmt",
		2*time.Second,
		10*time.Millisecond,
	)
	if err != nil {
		t.Errorf("expected nil after profile appeared; got %v", err)
	}
}

// TestWaitForRBACProfileProvisioned_TimeoutReturnsError verifies that the function
// returns a non-nil error when the profile never reaches provisioned=true.
func TestWaitForRBACProfileProvisioned_TimeoutReturnsError(t *testing.T) {
	// Profile always returns provisioned=false.
	profile := rbacProfileCR("nginx-ingress", "seam-tenant-ccs-mgmt", false)
	client := newRBACProfileDynClient(profile)

	client.Fake.PrependReactor("get", "rbacprofiles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, rbacProfileCR("nginx-ingress", "seam-tenant-ccs-mgmt", false), nil
	})

	err := capability.WaitForRBACProfileProvisioned(
		context.Background(),
		client,
		"nginx-ingress",
		"seam-tenant-ccs-mgmt",
		50*time.Millisecond,
		10*time.Millisecond,
	)
	if err == nil {
		t.Error("expected error on timeout; got nil")
	}
}

// TestWaitForRBACProfileProvisioned_ErrorMessageNamesProfileAndDuration verifies
// that the timeout error message identifies the profile name and timeout duration.
func TestWaitForRBACProfileProvisioned_ErrorMessageNamesProfileAndDuration(t *testing.T) {
	client := newRBACProfileDynClient()
	client.Fake.PrependReactor("get", "rbacprofiles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(rbacProfileTestGVR.GroupResource(), "my-pack")
	})

	const timeout = 50 * time.Millisecond
	err := capability.WaitForRBACProfileProvisioned(
		context.Background(),
		client,
		"my-pack",
		"seam-tenant-ccs-dev",
		timeout,
		10*time.Millisecond,
	)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "my-pack") {
		t.Errorf("error message should name the profile; got %q", msg)
	}
	if !strings.Contains(msg, timeout.String()) {
		t.Errorf("error message should name the timeout duration; got %q", msg)
	}
}

// TestWaitForRBACProfileProvisioned_NonNotFoundErrorPropagates verifies that a
// non-NotFound API error (e.g., Forbidden) propagates immediately without retry.
func TestWaitForRBACProfileProvisioned_NonNotFoundErrorPropagates(t *testing.T) {
	client := newRBACProfileDynClient()

	calls := 0
	client.Fake.PrependReactor("get", "rbacprofiles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		return true, nil, fmt.Errorf("rbacprofiles.security.ontai.dev is forbidden: User cannot get")
	})

	err := capability.WaitForRBACProfileProvisioned(
		context.Background(),
		client,
		"my-pack",
		"seam-tenant-ccs-mgmt",
		2*time.Second,
		10*time.Millisecond,
	)
	if err == nil {
		t.Fatal("expected error on Forbidden; got nil")
	}
	if calls != 1 {
		t.Errorf("expected exactly 1 Get call before propagating error; got %d", calls)
	}
}
