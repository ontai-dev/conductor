// Package agent implements the Agent Control Loop Layer: leader election,
// capability publishing, and receipt reconciliation.
// conductor-design.md §2.10, conductor-schema.md §10.
package agent

import (
	"context"
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	// leaseDuration is the duration a leader holds its lease before another
	// replica may attempt to acquire it. conductor-design.md §8.
	leaseDuration = 15 * time.Second

	// renewDeadline is the deadline within which the leader must renew its lease.
	// conductor-design.md §8.
	renewDeadline = 10 * time.Second

	// retryPeriod is the interval at which non-leaders poll for lease availability.
	// conductor-design.md §8.
	retryPeriod = 2 * time.Second
)

// LeaderCallbacks are the functions invoked by the leader election lifecycle.
type LeaderCallbacks struct {
	// OnStartedLeading is called when this replica wins the leader election lease.
	// It receives a context that is cancelled when leadership is lost.
	// All write-path goroutines must start here and honour this context.
	OnStartedLeading func(ctx context.Context)

	// OnStoppedLeading is called when this replica loses the leader election lease.
	// All write-path goroutines must stop when this is called.
	OnStoppedLeading func()

	// OnNewLeader is called when a new leader identity is observed.
	OnNewLeader func(identity string)
}

// RunLeaderElection starts the leader election loop for the Conductor agent.
//
// The lease is named "conductor-{clusterRef}" and lives in the provided
// namespace (always ont-system in production). The identity defaults to the
// pod hostname if empty. The call blocks until ctx is cancelled.
//
// On context cancellation with ReleaseOnCancel=true the lease is released
// cleanly and the function returns nil. conductor-design.md §8, §4.3.
func RunLeaderElection(ctx context.Context, client kubernetes.Interface, namespace, clusterRef, identity string, callbacks LeaderCallbacks) error {
	if identity == "" {
		h, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("leader election: resolve identity: %w", err)
		}
		identity = h
	}

	leaseName := "conductor-" + clusterRef

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: callbacks.OnStartedLeading,
			OnStoppedLeading: callbacks.OnStoppedLeading,
			OnNewLeader:      callbacks.OnNewLeader,
		},
	})
	if err != nil {
		return fmt.Errorf("leader election: construct elector: %w", err)
	}

	le.Run(ctx)
	return nil
}
