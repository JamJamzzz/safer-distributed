package kubernetes

// Real semantic cross-reference checks, as opposed to manifest_test.go's
// plain string-containment checks.
//
// `kubectl kustomize` only renders YAML: it does not verify that a
// Service's selector actually matches any Deployment's pod labels, or
// that a Deployment's envFrom actually names a ConfigMap/Secret that
// exists. An earlier version of this package's own doc comment claimed
// kustomize did those things; it does not, and this file is what actually
// does, by parsing the rendered YAML and checking the references by hand.
//
// Resources are decoded into a generic map[string]interface{} rather than
// a fixed struct: Service's spec.selector is a flat map[string]string,
// but Deployment's spec.selector is a nested LabelSelector
// ({matchLabels: {...}}) -- the same field name means two different
// shapes depending on kind, which a single rigid struct cannot represent
// without one of them failing to decode.

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// parseResources splits a multi-document kustomize/kubectl YAML stream
// into individual resources, each as a generic map.
func parseResources(t *testing.T, output string) []map[string]interface{} {
	t.Helper()
	var resources []map[string]interface{}
	decoder := yaml.NewDecoder(strings.NewReader(output))
	for {
		var r map[string]interface{}
		if err := decoder.Decode(&r); err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("decoding YAML: %v", err)
		}
		if r != nil {
			resources = append(resources, r)
		}
	}
	return resources
}

func kindOf(r map[string]interface{}) string { return stringField(r, "kind") }
func nameOf(r map[string]interface{}) string {
	return stringField(mapField(r, "metadata"), "name")
}

func stringField(m map[string]interface{}, key string) string {
	s, _ := m[key].(string)
	return s
}

func mapField(m map[string]interface{}, key string) map[string]interface{} {
	sub, _ := m[key].(map[string]interface{})
	return sub
}

// stringMapField reads a field expected to be a flat map[string]string
// (a Service's spec.selector, or a pod template's metadata.labels).
func stringMapField(m map[string]interface{}, key string) map[string]string {
	raw, _ := m[key].(map[string]interface{})
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		s, _ := v.(string)
		out[k] = s
	}
	return out
}

// labelsContain reports whether pod carries every key/value in selector --
// exactly what a Kubernetes Service's selector match requires.
func labelsContain(pod, selector map[string]string) bool {
	for k, v := range selector {
		if pod[k] != v {
			return false
		}
	}
	return true
}

// podTemplateLabels digs a Deployment's spec.template.metadata.labels out
// of its generic map form.
func podTemplateLabels(deployment map[string]interface{}) map[string]string {
	spec := mapField(deployment, "spec")
	template := mapField(spec, "template")
	metadata := mapField(template, "metadata")
	return stringMapField(metadata, "labels")
}

// TestServiceSelectorsMatchDeploymentLabels is the check the package doc
// used to claim kustomize performed on its own. For every Service with a
// selector, at least one Deployment in the same render must have pod
// template labels matching it -- otherwise the Service has no endpoints
// and silently routes nowhere.
func TestServiceSelectorsMatchDeploymentLabels(t *testing.T) {
	requireKubectl(t)
	resources := parseResources(t, runKustomize(t, "."))

	var deployments []map[string]interface{}
	for _, r := range resources {
		if kindOf(r) == "Deployment" {
			deployments = append(deployments, r)
		}
	}
	if len(deployments) == 0 {
		t.Fatal("no Deployment resources found in kustomize output")
	}

	checked := 0
	for _, r := range resources {
		if kindOf(r) != "Service" {
			continue
		}
		selector := stringMapField(mapField(r, "spec"), "selector")
		if len(selector) == 0 {
			continue
		}
		checked++
		matched := false
		for _, d := range deployments {
			if labelsContain(podTemplateLabels(d), selector) {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("Service %q selector %v matches no Deployment's pod template labels",
				nameOf(r), selector)
		}
	}
	if checked == 0 {
		t.Fatal("no Service with a selector found to check -- this test would pass vacuously")
	}
}

// TestEnvFromReferencesExist checks that every ConfigMap/Secret a
// Deployment's envFrom names is actually present. The real Secret
// (safer-mongo-credentials) is created out-of-band in a real deployment
// -- see deploy/kubernetes/README.md -- so this test merges in the local
// dev placeholder (secret.yaml) as the stand-in with the same name, which
// is the best available check without a live cluster: the reference is
// still confirmed to name something that is meant to exist, not a typo.
func TestEnvFromReferencesExist(t *testing.T) {
	requireKubectl(t)
	resources := parseResources(t, runKustomize(t, "."))
	resources = append(resources, parseResources(t, runKustomizeOnFile(t, "secret.yaml"))...)

	known := make(map[string]bool) // "ConfigMap/name" or "Secret/name"
	for _, r := range resources {
		if kind := kindOf(r); kind == "ConfigMap" || kind == "Secret" {
			known[kind+"/"+nameOf(r)] = true
		}
	}
	if len(known) == 0 {
		t.Fatal("no ConfigMap or Secret resources found -- this test would pass vacuously")
	}

	checked := 0
	for _, r := range resources {
		if kindOf(r) != "Deployment" {
			continue
		}
		spec := mapField(mapField(mapField(r, "spec"), "template"), "spec")
		containers, _ := spec["containers"].([]interface{})
		for _, c := range containers {
			container, _ := c.(map[string]interface{})
			envFrom, _ := container["envFrom"].([]interface{})
			for _, e := range envFrom {
				entry, _ := e.(map[string]interface{})
				if cmRef := mapField(entry, "configMapRef"); cmRef != nil {
					checked++
					name := stringField(cmRef, "name")
					if !known["ConfigMap/"+name] {
						t.Errorf("Deployment %q references ConfigMap %q, which does not exist", nameOf(r), name)
					}
				}
				if secretRef := mapField(entry, "secretRef"); secretRef != nil {
					checked++
					name := stringField(secretRef, "name")
					if !known["Secret/"+name] {
						t.Errorf("Deployment %q references Secret %q, which does not exist", nameOf(r), name)
					}
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no envFrom references found to check -- this test would pass vacuously")
	}
}

// runKustomizeOnFile builds a single standalone manifest through a
// throwaway kustomization, the same way TestStandaloneManifestsParse
// does, returning just the rendered YAML.
func runKustomizeOnFile(t *testing.T, file string) string {
	t.Helper()
	dir := t.TempDir()
	copyFile(t, file, filepath.Join(dir, file))
	writeKustomization(t, dir, file)
	cmd := exec.Command("kubectl", "kustomize", dir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl kustomize %s: %v\n%s", dir, err, output)
	}
	return string(output)
}
