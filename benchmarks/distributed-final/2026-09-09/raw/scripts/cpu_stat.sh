#!/usr/bin/env bash
# Read-only capture of cgroup v2 CPU throttling counters for the
# safer-worker pods, taken from the kind node container's own cgroup
# filesystem. Nothing in the cluster is modified: this only reads
# /sys/fs/cgroup. Emits one line per worker pod:
#   <pod> nr_periods=<n> nr_throttled=<n> throttled_usec=<n> usage_usec=<n>
set -u
NS=safer-distributed
NODE_CTR=safer-test-control-plane

kubectl -n $NS get pods -l app=safer-worker \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.uid}{"\n"}{end}' 2>/dev/null |
while read -r POD PODUID; do
  [ -z "${PODUID:-}" ] && continue
  SLICE=$(echo "$PODUID" | tr '-' '_')
  PATHS="/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-burstable.slice/kubelet-kubepods-burstable-pod${SLICE}.slice/cpu.stat"
  OUT=$(docker exec "$NODE_CTR" sh -c "cat $PATHS 2>/dev/null" 2>/dev/null)
  if [ -z "$OUT" ]; then
    echo "$POD UNAVAILABLE"
    continue
  fi
  NP=$(echo "$OUT" | awk '/^nr_periods/{print $2}')
  NT=$(echo "$OUT" | awk '/^nr_throttled/{print $2}')
  TU=$(echo "$OUT" | awk '/^throttled_usec/{print $2}')
  UU=$(echo "$OUT" | awk '/^usage_usec/{print $2}')
  echo "$POD nr_periods=$NP nr_throttled=$NT throttled_usec=$TU usage_usec=$UU"
done
