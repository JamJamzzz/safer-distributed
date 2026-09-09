# Datadog Agent OTLP configuration

This directory is deliberately separate from the rest of `deploy/kubernetes/`:
it configures which telemetry BACKEND SAFER's application code sends
OpenTelemetry data to, not SAFER's own application manifests. Swapping
backends, or turning telemetry off entirely, means touching only this
directory -- see `internal/telemetry`'s package doc in the Go source for the
"optional by construction" design this relies on.

## What this repository does NOT manage

The Datadog Agent itself (Operator, Cluster Agent, node Agent, the
`datadog-secret` API key Secret) is a managed, pre-existing dependency of the
deployment, the same way MongoDB is (see `docker/README.md`). This directory
does not install it. It assumes one is already running with:

- a `DatadogAgent` custom resource (`datadoghq.com/v2alpha1`) in a `datadog`
  namespace, installed via the Datadog Operator (the supported path this was
  developed against) or the `datadog/datadog` Helm chart
- a `datadog-secret` Secret in that namespace holding a real API key under
  the `api-key` key
- the Agent healthy: `kubectl -n datadog get datadogagent` shows
  `Running (N/N/N)` for the agent

## Enabling OTLP ingestion (one-time, per cluster)

Check first -- do not assume infrastructure telemetry working means OTLP is
already on:

```bash
kubectl -n datadog get datadogagent datadog -o yaml | grep -A5 "features:"
```

If there is no `otlp:` block under `spec.features`, add just that block with
a merge patch, which leaves every other field (the API secret reference, the
site, the cluster name, `env` tags, `kubelet.tlsVerify`, APM instrumentation,
cluster checks, orchestrator explorer -- whatever else the CR already has)
untouched:

```bash
kubectl -n datadog patch datadogagent datadog --type merge -p \
  '{"spec":{"features":{"otlp":{"receiver":{"protocols":{"grpc":{"enabled":true}}}}}}}'
```

Wait for the Agent DaemonSet to roll (it does, since this changes the
container's config) and confirm it comes back healthy:

```bash
kubectl -n datadog get datadogagent datadog -w   # until AGENT shows Running (1/1/1)
kubectl -n datadog get pods
```

Confirm the receiver is actually listening, not just configured, from
inside the Agent itself:

```bash
AGENT_POD=$(kubectl -n datadog get pod -l app.kubernetes.io/component=agent -o jsonpath='{.items[0].metadata.name}')
kubectl -n datadog exec "$AGENT_POD" -c agent -- agent status | grep -A3 "^OTLP"
# OTLP
# ====
#   Status: Enabled
#   Collector status: Running
```

And that the Agent's Service now exposes the gRPC port:

```bash
kubectl -n datadog get svc datadog-agent
# datadog-agent   ClusterIP   ...   8126/TCP,8125/UDP,4317/TCP
```

This was run exactly as above against a real Datadog environment (site
`us3.datadoghq.com`, cluster name `safer-test`, `env:dev`) while building
Phase 5; the Agent returned to healthy afterward, and OTLP came up enabled
with no other change to the CR.

## Pointing SAFER at it

`otel-config.yaml` is the one ConfigMap that does this: it sets
`OTEL_EXPORTER_OTLP_ENDPOINT` to the Agent's in-cluster Service DNS name
(`datadog-agent.datadog.svc.cluster.local:4317`) and
`OTEL_RESOURCE_ATTRIBUTES=deployment.environment=dev` to match the `env:dev`
tag the DatadogAgent CR already applies to infrastructure telemetry.
`worker-deployment.yaml`, `coordinator-deployment.yaml`, and
`loadgen-job.yaml` reference it via an `optional: true` `envFrom` entry, so
applying the rest of `deploy/kubernetes/` without this ConfigMap present is
not an error -- it just means no telemetry is exported, exactly the
optional-observability property `internal/telemetry` implements in code.

```bash
kubectl apply -f deploy/kubernetes/telemetry/otel-config.yaml
kubectl -n safer-distributed rollout restart deployment/safer-worker deployment/safer-coordinator
```

(A restart is needed because these are environment variables, read once at
process startup -- the same reason any other ConfigMap change in this
repository's manifests needs one.)

## What "OTLP-compatible" does and does not mean here

The application never imports a Datadog-specific tracing or metrics library
-- only `go.opentelemetry.io/otel` and its SDK/exporter packages (see
`internal/telemetry`). Nothing about the Go code assumes it is talking to a
Datadog Agent specifically; it is talking to whatever OTLP gRPC endpoint
`OTEL_EXPORTER_OTLP_ENDPOINT` names. The Datadog Agent's OTLP ingestion path
is what this cluster happens to run, not something the application depends
on. Pointing this ConfigMap at a different OTLP-compatible collector needs
no code change.
