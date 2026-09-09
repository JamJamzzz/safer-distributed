# Kubernetes deployment

Deploys the coordinator (1 replica) and workers (3 replicas) from the
images built in `docker/`. MongoDB is not deployed by these manifests --
see `docker/README.md` for why -- so a cluster needs one reachable
separately (an in-cluster MongoDB, a managed service, or `kind`'s
host-network access to a local `docker run` MongoDB).

## Apply

```bash
kubectl create secret generic safer-mongo-credentials \
  --namespace safer-distributed \
  --from-literal=SAFER_MONGO_URI='mongodb://<host>:27017/?replicaSet=rs0&directConnection=true'
# (the namespace must exist first; `kubectl apply -k .` below creates it,
# so create the namespace alone first if scripting this: `kubectl apply
# -f namespace.yaml`)

kubectl apply -k deploy/kubernetes

kubectl -n safer-distributed rollout status deployment/safer-coordinator
kubectl -n safer-distributed rollout status deployment/safer-worker

kubectl apply -f deploy/kubernetes/loadgen-job.yaml
kubectl -n safer-distributed logs job/safer-loadgen -f
```

`secret.yaml` in this directory is a local/dev placeholder and is
deliberately NOT part of `kustomization.yaml` -- create the real Secret as
shown above (or through whatever secrets-management the target cluster
uses) before applying the rest.

## What was and was not actually run

This environment has Docker but no running Kubernetes cluster (no
`kind`/`minikube`, no Docker Desktop Kubernetes context -- `kubectl
cluster-info` fails to connect). Concretely, for Phase 4:

- **Actually run**: all three Docker images built successfully and were
  smoke-tested together on a plain Docker network against a real MongoDB
  replica-set container -- coordinator, worker, and loadgen all
  interoperating, plus the coordinator's fail-closed behavior verified
  inside its own container. See `docker/README.md`.
- **Statically validated only, not applied to a live cluster**: every
  manifest in this directory. `kubectl kustomize deploy/kubernetes`
  builds cleanly (confirms syntax and cross-references -- Service
  selectors matching Deployment labels, ConfigMap/Secret names matching
  `envFrom` references, etc.), and the standalone files
  (`secret.yaml`, `loadgen-job.yaml`,
  `networkpolicy-mongo-optional.yaml`) were validated the same way
  through a throwaway kustomization. None of this exercises the
  Kubernetes API server itself: no pod has actually been scheduled, no
  probe has actually fired, no NetworkPolicy has actually been enforced
  by a CNI plugin, and the `Recreate` rollout behavior on the coordinator
  has not been observed in a live rollout.

If a cluster becomes available (`kind create cluster`, Docker Desktop's
Kubernetes toggle, or similar), running the Apply steps above and then
`cmd/loadgen`'s Job is the natural next verification step -- nothing about
these manifests is expected to need cluster-specific changes beyond
pointing at wherever MongoDB actually lives.

## Load balancing across worker replicas

`loadgen-job.yaml` targets `dns:///safer-worker-headless:50052`, not the
plain `safer-worker` ClusterIP Service. That is not a stylistic choice: a
gRPC client that dials a ClusterIP Service once (as any long-lived client,
including a naive load generator, naturally would) gets one HTTP/2
connection that kube-proxy has pinned to one backend pod, and every RPC
multiplexed over it lands on that same pod regardless of how many replicas
exist. `worker-service-headless.yaml` (`clusterIP: None`) is what makes
DNS resolution return every ready pod's own IP instead of one virtual IP;
`cmd/loadgen/dial.go` is what turns that address list into real per-RPC
`round_robin` balancing, client-side. See that file's package doc and
`worker-service.yaml`'s own header comment for the full explanation.

This was verified without a Kubernetes cluster by reproducing the same DNS
shape with plain Docker: three worker containers sharing one
`--network-alias`, which makes Docker's embedded DNS return all three
containers' IPs for that one name -- functionally identical, for gRPC's
purposes, to a Kubernetes headless Service. `docker/README.md` has the
exact commands and output: `replicas_served=3`, read from the response-
header instance identifier `cmd/worker` attaches to every RPC
(`internal/workerdiag`), not assumed. That is the check to repeat against
this Job's actual output once a real cluster is available -- see "What was
and was not actually run" above for why it has not been yet.

## Health semantics

See the inline comments in `worker-deployment.yaml` and
`cmd/worker/health.go`: readiness tracks MongoDB/coordinator reachability
(an unready worker leaves the Service's endpoint list), liveness only asks
whether the process itself is alive and is set once at startup, so a
shared dependency outage cannot make Kubernetes kill every worker replica
at once.

## Coordinator rollout

See the inline comments in `coordinator-deployment.yaml`: `replicas: 1`
and `strategy: Recreate` are both permanent, not placeholders to revisit.
Two coordinator pods running at once would each hold an independent
in-memory lock table, splitting coordination between workers -- silently,
since nothing about strict 2PL detects a second lock authority existing.
`Recreate` tears the old pod down before the new one starts, trading
deployment availability (a brief coordination outage on every coordinator
rollout) for coordination safety.

`Recreate` earns exactly that one property -- no overlap -- and nothing more. It
does **not** make a coordinator restart fault-tolerant, and it does **not** preserve
in-flight lock state across the restart: the old pod's in-memory lock table is gone
the instant it terminates, identically to an unplanned crash. This is not high
availability, and nothing here adds leader election, consensus, or Raft to make it
one.

## Network isolation

See `networkpolicy.yaml`'s header comment for the full reasoning. Short
version: `default-deny-ingress` plus two allow rules mean the coordinator
is reachable only from worker pods, and workers are reachable only from
pods carrying the `safer-client: "true"` label (see `loadgen-job.yaml`,
which carries it). This is network-layer restriction of *which pods* can
reach these services, not cryptographic authentication of *what they
send* once they can -- neither the coordinator's nor the worker's gRPC
transport is encrypted or authenticated at the application layer, and
building mTLS or any other service-authentication layer is out of scope
for this phase.

**The two channels are not equally risky, and the NetworkPolicies are not
sized the same for that reason.** The coordinator's channel
(`client/coordination/grpccoord`'s package doc) carries only resource
identifiers and lock modes -- SAFER encrypts and authenticates its
objects before anything reaches storage or the lock layer, so there is no
plaintext content or credentials on that wire regardless of who reaches
it. The worker's channel (`proto/worker/v1/worker.proto`'s package doc,
`cmd/worker`'s) is different: it sits *before* that encryption layer, so
every request carries the caller's plaintext username and password, and
StoreFile/AppendToFile carry plaintext file content. That is why
`worker-ingress` is scoped to an opt-in label rather than the whole
namespace the way an earlier version of this policy had it -- **the
worker gRPC surface is not suitable for exposure to an untrusted
network**, and namespace-wide ingress was wider than that risk warrants.
Any pod that legitimately needs to reach the worker Service adds the
`safer-client: "true"` label to its own pod template; this is a
convention, not a hardcoded allowlist, so it does not need editing here
as new callers are added. None of this is cryptographic identity or
transport encryption -- it narrows *who is on the network*, not what an
already-admitted pod can see.

`networkpolicy-mongo-optional.yaml` is a template for the case where
MongoDB itself runs as an in-cluster pod; it is not applied by
`kustomization.yaml` because this repository's manifests do not deploy
MongoDB at all (see `docker/README.md`).
