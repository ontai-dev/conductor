// compile_packbuild_kustomize.go implements the kustomize automation path for packbuild.
// Runs krusty.Kustomizer on a local overlay directory, renders all resources,
// splits into RBAC/cluster-scoped/workload OCI layers, pushes them, and emits
// a ClusterPack CR. Requires INV-014 build tag: kustomize client is compile-mode
// only. conductor-schema.md §9, wrapper-schema.md §4, INV-001.
package main

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// KustomizeSource describes the kustomize overlay directory for automated packbuild.
// All resources produced by `kustomize build` on the directory are split into the
// three-layer OCI artifact contract (RBAC, cluster-scoped, workload).
// wrapper-schema.md §4, conductor-schema.md §9, T-12.
type KustomizeSource struct {
	// Path is the directory containing the kustomization.yaml file.
	// Must be an absolute path or relative to the PackBuildInput file location.
	Path string `yaml:"path"`
}

// kustomizeCompilePackBuild implements the kustomize automation path for packbuild.
// Runs krusty.Kustomizer on KustomizeSource.Path, renders all resources, splits
// the output into three OCI layers (RBAC, cluster-scoped, workload), pushes each
// layer to in.RegistryURL, and emits a ClusterPack CR YAML to output.
// inputDir is the directory of the PackBuildInput file (for relative path resolution).
// T-12, conductor-schema.md §9.
func kustomizeCompilePackBuild(ctx context.Context, in PackBuildInput, inputDir, output string) error {
	ks := in.KustomizeSource
	if ks == nil || ks.Path == "" {
		return fmt.Errorf("kustomizeCompilePackBuild: kustomizeSource.path is required")
	}
	if in.RegistryURL == "" {
		return fmt.Errorf("kustomizeCompilePackBuild: registryUrl is required for kustomize packbuild")
	}
	if in.Namespace == "" {
		in.Namespace = "seam-system"
	}

	srcPath := ks.Path
	if !isAbsPath(srcPath) && inputDir != "" {
		srcPath = joinPath(inputDir, srcPath)
	}

	fSys := filesys.MakeFsOnDisk()
	k := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	resMap, err := k.Run(fSys, srcPath)
	if err != nil {
		return fmt.Errorf("kustomizeCompilePackBuild: kustomize build %q: %w", srcPath, err)
	}

	renderedYAML, err := resMap.AsYaml()
	if err != nil {
		return fmt.Errorf("kustomizeCompilePackBuild: serialize kustomize output: %w", err)
	}
	if len(renderedYAML) == 0 {
		return fmt.Errorf("kustomizeCompilePackBuild: kustomize build produced no output from %q", srcPath)
	}

	manifests, err := ParsePackManifests(string(renderedYAML))
	if err != nil {
		return fmt.Errorf("kustomizeCompilePackBuild: parse kustomize manifests: %w", err)
	}
	rbacManifests, clusterScopedManifests, workloadManifests := SplitManifests(manifests)

	rbacLayer := serializeManifests(rbacManifests)
	clusterScopedLayer := serializeManifests(clusterScopedManifests)
	workloadLayer := serializeManifests(workloadManifests)

	rbacDigest, err := ociPushLayer(ctx, in.RegistryURL, in.Version+"-rbac", []byte(rbacLayer))
	if err != nil {
		return fmt.Errorf("kustomizeCompilePackBuild: push RBAC layer: %w", err)
	}

	var clusterScopedDigest string
	if len(clusterScopedManifests) > 0 {
		clusterScopedDigest, err = ociPushLayer(ctx, in.RegistryURL, in.Version+"-cluster-scoped", []byte(clusterScopedLayer))
		if err != nil {
			return fmt.Errorf("kustomizeCompilePackBuild: push cluster-scoped layer: %w", err)
		}
	}

	workloadDigest, err := ociPushLayer(ctx, in.RegistryURL, in.Version+"-workload", []byte(workloadLayer))
	if err != nil {
		return fmt.Errorf("kustomizeCompilePackBuild: push workload layer: %w", err)
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

// isAbsPath reports whether p is an absolute file path.
func isAbsPath(p string) bool {
	return len(p) > 0 && p[0] == '/'
}

// joinPath joins two path segments with a '/'.
func joinPath(dir, rel string) string {
	if dir == "" {
		return rel
	}
	if rel == "" {
		return dir
	}
	return dir + "/" + rel
}
