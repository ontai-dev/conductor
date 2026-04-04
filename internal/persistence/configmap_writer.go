// Package persistence implements the Persistence Layer for execute mode:
// writing OperationResultSpec to a Kubernetes ConfigMap before Job exit.
// conductor-design.md §2.8, conductor-schema.md §8.
package persistence

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// ResultDataKey is the ConfigMap key under which the JSON-encoded
// OperationResultSpec is stored. The operator reads this key after Job exit.
// conductor-schema.md §8.
const ResultDataKey = "result"

// ConfigMapWriter writes an OperationResultSpec to a named Kubernetes ConfigMap.
// This is the only output channel between operator and Conductor Job.
// conductor-design.md §2.8, conductor-schema.md §8.
type ConfigMapWriter interface {
	WriteResult(ctx context.Context, namespace, name string, result runnerlib.OperationResultSpec) error
}

// kubeConfigMapWriter is the production ConfigMapWriter backed by a Kubernetes client.
type kubeConfigMapWriter struct {
	client kubernetes.Interface
}

// NewKubeConfigMapWriter constructs a ConfigMapWriter backed by the provided
// Kubernetes client. In execute mode, the client is always the in-cluster client.
func NewKubeConfigMapWriter(client kubernetes.Interface) ConfigMapWriter {
	return &kubeConfigMapWriter{client: client}
}

// WriteResult serializes the OperationResultSpec to JSON and writes it to the
// named ConfigMap under key ResultDataKey. Creates the ConfigMap if absent;
// updates it if it already exists. conductor-schema.md §8.
func (w *kubeConfigMapWriter) WriteResult(ctx context.Context, namespace, name string, result runnerlib.OperationResultSpec) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("configmap writer: marshal OperationResultSpec: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string]string{
			ResultDataKey: string(data),
		},
	}

	_, createErr := w.client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
	if createErr == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(createErr) {
		return fmt.Errorf("configmap writer: create %q in %q: %w", name, namespace, createErr)
	}

	// ConfigMap already exists — get and update.
	existing, err := w.client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("configmap writer: get existing %q in %q: %w", name, namespace, err)
	}
	if existing.Data == nil {
		existing.Data = make(map[string]string)
	}
	existing.Data[ResultDataKey] = string(data)
	if _, err := w.client.CoreV1().ConfigMaps(namespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("configmap writer: update existing %q in %q: %w", name, namespace, err)
	}
	return nil
}

// NoopConfigMapWriter is a ConfigMapWriter that discards all writes.
// Used in unit tests where the ConfigMap write side-effect is not under test.
type NoopConfigMapWriter struct{}

// WriteResult discards the result without writing anything.
func (NoopConfigMapWriter) WriteResult(_ context.Context, _, _ string, _ runnerlib.OperationResultSpec) error {
	return nil
}
