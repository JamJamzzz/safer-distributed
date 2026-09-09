#!/usr/bin/env bash
# Temporary benchmark runner for the safer-distributed final measurement
# campaign. Lives outside the repository on purpose: the checked-in
# loadgen-job.yaml is read but never modified.
#
# usage: run_one.sh <jobname> <workload> <concurrency> <value> <count|duration>
#
# Only metadata.name, -workload=, -concurrency= and -count=/-duration= are
# substituted. Everything else (safer-client label, image, imagePullPolicy,
# safer-otel-config envFrom, dns:///safer-worker-headless:50052 target,
# resources, restartPolicy, namespace) is inherited unchanged from the
# checked-in Job. -content-size is deliberately NOT passed so it keeps the
# binary default of 64.
set -u

CAMPAIGN=/c/Users/duanh/AppData/Local/Temp/claude/campaign
SRC=/c/Users/duanh/Desktop/safer-distributed/deploy/kubernetes/loadgen-job.yaml
NS=safer-distributed

NAME="$1"; WL="$2"; CONC="$3"; VAL="$4"; MODE="$5"
YAML="$CAMPAIGN/yaml/$NAME.yaml"
LOG="$CAMPAIGN/logs/$NAME.log"
META="$CAMPAIGN/meta/$NAME.meta"

if [ -e "$LOG" ]; then
  echo "REFUSING: $LOG already exists (never overwrite raw evidence)" >&2
  exit 2
fi

sed -e "s|^  name: safer-loadgen\$|  name: $NAME|" \
    -e "s|- \"-workload=mixed\"|- \"-workload=$WL\"|" \
    -e "s|- \"-concurrency=8\"|- \"-concurrency=$CONC\"|" \
    "$SRC" > "$YAML"

if [ "$MODE" = "count" ]; then
  sed -i "s|- \"-count=200\"|- \"-count=$VAL\"|" "$YAML"
else
  sed -i "s|- \"-count=200\"|- \"-duration=$VAL\"|" "$YAML"
fi

# Restart counts BEFORE the run, per worker pod.
RESTARTS_BEFORE=$(kubectl -n $NS get pods -l app=safer-worker \
  -o jsonpath='{range .items[*]}{.metadata.name}={.status.containerStatuses[0].restartCount}{" "}{end}' 2>/dev/null)

START=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
APPLY_OUT=$(kubectl apply -f "$YAML" 2>&1)
if [ $? -ne 0 ]; then
  echo "HARNESS FAILURE: kubectl apply rejected the Job -- workload never ran:" >&2
  echo "$APPLY_OUT" >&2
  exit 3
fi

STATUS="timeout"
for i in $(seq 1 200); do
  S=$(kubectl -n $NS get job "$NAME" -o jsonpath='{.status.succeeded}' 2>/dev/null)
  F=$(kubectl -n $NS get job "$NAME" -o jsonpath='{.status.failed}' 2>/dev/null)
  if [ "${S:-0}" = "1" ]; then STATUS="succeeded"; break; fi
  if [ "${F:-0}" != "" ] && [ "${F:-0}" != "0" ]; then STATUS="failed"; break; fi
  sleep 2
done
END=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

kubectl -n $NS logs "job/$NAME" --tail=100 > "$LOG" 2>&1

RESTARTS_AFTER=$(kubectl -n $NS get pods -l app=safer-worker \
  -o jsonpath='{range .items[*]}{.metadata.name}={.status.containerStatuses[0].restartCount}{" "}{end}' 2>/dev/null)

{
  echo "name=$NAME"
  echo "workload=$WL"
  echo "concurrency=$CONC"
  echo "mode=$MODE"
  echo "value=$VAL"
  echo "utc_start=$START"
  echo "utc_end=$END"
  echo "job_status=$STATUS"
  echo "restarts_before=$RESTARTS_BEFORE"
  echo "restarts_after=$RESTARTS_AFTER"
} > "$META"

echo "### $NAME status=$STATUS start=$START end=$END"
grep -E "workload=|latency_samples=|replicas_served=|DATA CORRUPTION|VERIFICATION ERROR|error:" "$LOG"
if [ "$RESTARTS_BEFORE" != "$RESTARTS_AFTER" ]; then
  echo "!!! RESTART CHANGE: before[$RESTARTS_BEFORE] after[$RESTARTS_AFTER]"
fi

# Evidence is captured; remove the completed Job.
kubectl -n $NS delete job "$NAME" --ignore-not-found=true >/dev/null 2>&1
