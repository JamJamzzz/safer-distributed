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
rollout) for coordination safety. This is not high availability, and nothing
here adds leader election or consensus to make it one.

## Network isolation

See `networkpolicy.yaml`'s header comment for the full reasoning. Short
version: `default-deny-ingress` plus two allow rules mean the coordinator
is reachable only from worker pods, and workers are reachable only from
within the `safer-distributed` namespace. This is network-layer
restriction of *which pods* can reach these services, not cryptographic
authentication of *what they send* once they can -- the coordinator's gRPC
transport itself remains unauthenticated at the application layer (see
`client/coordination/grpccoord`'s package doc), and building mTLS or any
other service-authentication layer is out of scope for this phase.
`networkpolicy-mongo-optional.yaml` is a template for the case where
MongoDB itself runs as an in-cluster pod; it is not applied by
`kustomization.yaml` because this repository's manifests do not deploy
MongoDB at all (see `docker/README.md`).
