// Package kubernetes holds no Go code of its own; this test file exists
// only to keep deploy/kubernetes's manifests from rotting silently.
//
// It does NOT stand in for deploying to a real cluster -- see this
// directory's README.md for exactly what was and was not verified against
// a live Kubernetes API server, which is nothing: no cluster was available
// in the environment this was written in (no kind/minikube, no Docker
// Desktop Kubernetes context). What this test does check is real, just
// narrower: `kubectl kustomize` parses every manifest, resolves
// cross-references (a Deployment's envFrom naming a ConfigMap/Secret that
// actually exists, a Service's selector actually matching a Deployment's
// pod labels), and produces the resource set a real `kubectl apply -k`
// would submit.
package kubernetes

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func requireKubectl(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("skipping: kubectl is not installed")
	}
}

// TestKustomizationBuilds checks the manifests kustomization.yaml
// actually references: the namespace, coordinator and worker Deployments
// and Services, the ConfigMap, and the NetworkPolicies.
func TestKustomizationBuilds(t *testing.T) {
	requireKubectl(t)

	output := runKustomize(t, ".")

	for _, want := range []string{
		"kind: Namespace",
		"kind: ConfigMap",
		"kind: Deployment",
		"kind: Service",
		"kind: NetworkPolicy",
		"name: safer-coordinator",
		"name: safer-worker",
		"name: safer-config",
		"name: default-deny-ingress",
		"name: coordinator-ingress",
		"name: worker-ingress",
		// The coordinator's no-overlap rollout policy (Phase 4.4) is a
		// correctness property, not an implementation detail -- worth
		// asserting directly so an edit that quietly drops it fails CI.
		"type: Recreate",
		"replicas: 1",
		"replicas: 3",
		// The headless Service (Phase 4.5) is what makes real cross-pod
		// gRPC balancing possible at all -- see worker-service-headless.yaml
		// and cmd/loadgen/dial.go. Losing "clusterIP: None" silently turns
		// it back into an ordinary Service that resolves to one IP.
		"name: safer-worker-headless",
		"clusterIP: None",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("kustomize output does not contain %q", want)
		}
	}
}

// TestStandaloneManifestsParse checks the manifests kustomization.yaml
// deliberately does NOT include (secret.yaml is a dev placeholder,
// loadgen-job.yaml is applied on demand, the mongo NetworkPolicy is
// conditional -- see kustomization.yaml's own comments), by building each
// through its own throwaway kustomization.
func TestStandaloneManifestsParse(t *testing.T) {
	requireKubectl(t)

	for _, file := range []string{
		"secret.yaml",
		"loadgen-job.yaml",
		"networkpolicy-mongo-optional.yaml",
	} {
		file := file
		t.Run(file, func(t *testing.T) {
			dir := t.TempDir()
			copyFile(t, file, filepath.Join(dir, file))
			writeKustomization(t, dir, file)
			output := runKustomize(t, dir)
			if !strings.Contains(output, "namespace: safer-distributed") {
				t.Errorf("%s: kustomize output missing namespace: safer-distributed:\n%s", file, output)
			}
		})
	}
}

func runKustomize(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("kubectl", "kustomize", dir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl kustomize %s: %v\n%s", dir, err, output)
	}
	return string(output)
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("reading %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("writing %s: %v", dst, err)
	}
}

func writeKustomization(t *testing.T, dir, resource string) {
	t.Helper()
	content := "resources:\n  - " + resource + "\n"
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(content), 0o644); err != nil {
		t.Fatalf("writing kustomization.yaml: %v", err)
	}
}
