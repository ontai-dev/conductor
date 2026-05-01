// compile_packbuild_raw.go implements the rawCompilePackBuild function.
// Reads all .yaml/.yml files from a RawSource directory, splits manifests into
// RBAC, cluster-scoped, and workload OCI layers, pushes each layer, and emits
// a ClusterPack CR. Reuses the same split and OCI-push helpers as the Helm path.
// conductor-schema.md §9, wrapper-schema.md §4.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// RawSource describes a directory of raw YAML manifest files for automated packbuild.
// All .yaml and .yml files in the directory are read, concatenated, and split into
// the three-layer OCI artifact contract (RBAC, cluster-scoped, workload).
// wrapper-schema.md §4, conductor-schema.md §9.
type RawSource struct {
	// Path is the directory containing raw YAML manifest files.
	// Relative paths are resolved from the PackBuildInput file's directory.
	Path string `yaml:"path"`
}

// rawCompilePackBuild reads all .yaml/.yml files from RawSource.Path, parses and
// splits the manifests into three OCI layers (RBAC, cluster-scoped, workload),
// pushes each layer to in.RegistryURL, and emits a ClusterPack CR to output.
// inputDir is the directory of the PackBuildInput file (for relative path resolution).
func rawCompilePackBuild(ctx context.Context, in PackBuildInput, inputDir, output string) error {
	rs := in.RawSource
	if rs.Path == "" {
		return fmt.Errorf("rawCompilePackBuild: rawSource.path is required")
	}
	if in.RegistryURL == "" {
		return fmt.Errorf("rawCompilePackBuild: registryUrl is required for raw packbuild")
	}
	if in.Namespace == "" {
		in.Namespace = "seam-system"
	}

	srcPath := rs.Path
	if !filepath.IsAbs(srcPath) && inputDir != "" {
		srcPath = filepath.Join(inputDir, srcPath)
	}

	entries, err := os.ReadDir(srcPath)
	if err != nil {
		return fmt.Errorf("rawCompilePackBuild: read directory %q: %w", srcPath, err)
	}
	var allYAML strings.Builder
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(srcPath, name))
		if err != nil {
			return fmt.Errorf("rawCompilePackBuild: read file %q: %w", name, err)
		}
		allYAML.Write(data)
		allYAML.WriteString("\n")
	}
	if allYAML.Len() == 0 {
		return fmt.Errorf("rawCompilePackBuild: no YAML files found in %q", srcPath)
	}

	manifests, err := ParsePackManifests(allYAML.String())
	if err != nil {
		return fmt.Errorf("rawCompilePackBuild: parse manifests: %w", err)
	}
	rbacManifests, clusterScopedManifests, workloadManifests := SplitManifests(manifests)

	rbacLayer := serializeManifests(rbacManifests)
	clusterScopedLayer := serializeManifests(clusterScopedManifests)
	workloadLayer := serializeManifests(workloadManifests)

	rbacDigest, err := ociPushLayer(ctx, in.RegistryURL, in.Version+"-rbac", []byte(rbacLayer))
	if err != nil {
		return fmt.Errorf("rawCompilePackBuild: push RBAC layer: %w", err)
	}

	var clusterScopedDigest string
	if len(clusterScopedManifests) > 0 {
		clusterScopedDigest, err = ociPushLayer(ctx, in.RegistryURL, in.Version+"-cluster-scoped", []byte(clusterScopedLayer))
		if err != nil {
			return fmt.Errorf("rawCompilePackBuild: push cluster-scoped layer: %w", err)
		}
	}

	workloadDigest, err := ociPushLayer(ctx, in.RegistryURL, in.Version+"-workload", []byte(workloadLayer))
	if err != nil {
		return fmt.Errorf("rawCompilePackBuild: push workload layer: %w", err)
	}

	checksum := computeChecksum(rbacLayer + clusterScopedLayer + workloadLayer)

	cp := seamcorev1alpha1.InfrastructureClusterPack{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "infrastructure.ontai.dev/v1alpha1",
			Kind:       "InfrastructureClusterPack",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: in.Namespace,
		},
		Spec: seamcorev1alpha1.InfrastructureClusterPackSpec{
			Version: in.Version,
			RegistryRef: seamcorev1alpha1.InfrastructurePackRegistryRef{
				URL:    in.RegistryURL,
				Digest: workloadDigest,
			},
			Checksum:            checksum,
			SourceBuildRef:      in.SourceBuildRef,
			TargetClusters:      in.TargetClusters,
			RBACDigest:          rbacDigest,
			WorkloadDigest:      workloadDigest,
			ClusterScopedDigest: clusterScopedDigest,
			BasePackName:        in.BasePackName,
			ValuesFile:          in.ValuesFile,
		},
	}
	return writeCRYAML(output, in.Name, cp)
}
