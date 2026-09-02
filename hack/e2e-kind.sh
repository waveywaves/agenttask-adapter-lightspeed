#!/usr/bin/env bash
# Copyright 2026 The AgentTask Authors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORE_DIR="${CORE_DIR:-$(cd "$ROOT/../agenttask" && pwd)}"
CLUSTER_NAME="${CLUSTER_NAME:-agenttask-poc}"
CONTEXT="kind-$CLUSTER_NAME"
KEEP_CLUSTER="${KEEP_CLUSTER:-false}"
created=false
overlay="$(mktemp -d)"

cleanup() {
  status=$?
  rm -rf "$overlay"
  if [[ "$created" == true && "$KEEP_CLUSTER" != true ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null
  fi
  exit "$status"
}
trap cleanup EXIT

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --name "$CLUSTER_NAME" --wait 120s
  created=true
fi

kubectl --context "$CONTEXT" apply -f \
  https://github.com/tektoncd/pipeline/releases/download/v1.0.2/release.yaml >/dev/null
kubectl --context "$CONTEXT" -n tekton-pipelines wait --for=condition=Available \
  deployment/tekton-pipelines-controller deployment/tekton-pipelines-webhook \
  --timeout=180s >/dev/null
kubectl --context "$CONTEXT" delete namespace agenttask-system --ignore-not-found --wait=true >/dev/null

kubectl --context "$CONTEXT" apply -f "$CORE_DIR/config/crd/bases/agent.tekton.dev_agenttasks.yaml" >/dev/null
kubectl --context "$CONTEXT" apply -f "$ROOT/testdata/agentic-crds.yaml" >/dev/null

docker_host="${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}"
image="$(cd "$ROOT" && DOCKER_HOST="$docker_host" KO_DOCKER_REPO=ko.local \
  ko build --local --platform="linux/$(go env GOARCH)" ./cmd/controller)"
kind load docker-image --name "$CLUSTER_NAME" "$image" >/dev/null
cp -R "$ROOT/config/." "$overlay/"
IMAGE="$image" yq -i '.spec.template.spec.containers[0].image = strenv(IMAGE)' "$overlay/deployment.yaml"
kubectl --context "$CONTEXT" apply -k "$overlay" >/dev/null
kubectl --context "$CONTEXT" -n agenttask-system wait --for=condition=Available \
  deployment/lightspeed-agenttask-adapter --timeout=120s >/dev/null

service_account="system:serviceaccount:agenttask-system:lightspeed-agenttask-adapter"
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" get secrets -n agenttask-system)" = no
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" patch agenticrunapprovals.agentic.openshift.io -n agenttask-system)" = no
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" update customruns/finalizers.tekton.dev -n agenttask-system)" = yes

kubectl --context "$CONTEXT" -n agenttask-system delete pipelinerun/lightspeed-agenttask-poc --ignore-not-found >/dev/null
kubectl --context "$CONTEXT" create -f "$ROOT/examples/lightspeed-analysis.yaml" >/dev/null

for _ in $(seq 1 120); do
  customrun="$(kubectl --context "$CONTEXT" -n agenttask-system get customruns.tekton.dev -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [[ -n "$customrun" ]] && break
  sleep 1
done
test -n "${customrun:-}"

for _ in $(seq 1 120); do
  native="$(kubectl --context "$CONTEXT" -n agenttask-system get agenticruns.agentic.openshift.io -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [[ -n "$native" ]] && break
  sleep 1
done
test -n "${native:-}"
native_uid="$(kubectl --context "$CONTEXT" -n agenttask-system get agenticrun "$native" -o jsonpath='{.metadata.uid}')"

cat <<EOF | kubectl --context "$CONTEXT" create -f - >/dev/null
apiVersion: agentic.openshift.io/v1alpha1
kind: AnalysisResult
metadata:
  name: analysis-result
  namespace: agenttask-system
  ownerReferences:
    - apiVersion: agentic.openshift.io/v1alpha1
      kind: AgenticRun
      name: $native
      uid: $native_uid
      controller: true
      blockOwnerDeletion: true
spec:
  agenticRunName: $native
EOF
kubectl --context "$CONTEXT" -n agenttask-system patch analysisresult/analysis-result \
  --subresource=status --type=merge -p '{"status":{"actionRequired":"False"}}' >/dev/null
kubectl --context "$CONTEXT" -n agenttask-system patch agenticrun/"$native" \
  --subresource=status --type=merge -p "{\"status\":{\"conditions\":[{\"type\":\"Verified\",\"status\":\"True\",\"reason\":\"Succeeded\",\"lastTransitionTime\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}],\"steps\":{\"analysis\":{\"results\":[{\"name\":\"analysis-result\",\"outcome\":\"Succeeded\"}]}}}}" >/dev/null

kubectl --context "$CONTEXT" -n agenttask-system wait --for=condition=Succeeded \
  pipelinerun/lightspeed-agenttask-poc --timeout=180s >/dev/null
outcome="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.status.results[?(@.name=="outcome")].value}')"
result_name="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.status.results[?(@.name=="analysis-result-name")].value}')"
execution_uid="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.status.extraFields.executionRef.uid}')"
test "$outcome" = no-action-required
test "$result_name" = analysis-result
test "$execution_uid" = "$native_uid"
test "$(kubectl --context "$CONTEXT" -n agenttask-system get agenticruns -o jsonpath='{.items[*].metadata.uid}' | wc -w | tr -d ' ')" = 1

sed 's/name: lightspeed-agenttask-poc/name: lightspeed-agenttask-cancel/' \
  "$ROOT/examples/lightspeed-analysis.yaml" >"$overlay/cancel.yaml"
kubectl --context "$CONTEXT" apply -f "$overlay/cancel.yaml" >/dev/null
for _ in $(seq 1 120); do
  customrun_cancel="$(kubectl --context "$CONTEXT" -n agenttask-system get customruns -o json 2>/dev/null | \
    jq -r '.items[] | select(any(.metadata.ownerReferences[]?; .name == "lightspeed-agenttask-cancel")) | .metadata.name' | head -1)"
  [[ -n "$customrun_cancel" ]] && break
  sleep 1
done
test -n "${customrun_cancel:-}"
customrun_cancel_uid="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun_cancel" -o jsonpath='{.metadata.uid}')"
for _ in $(seq 1 120); do
  native_cancel="$(kubectl --context "$CONTEXT" -n agenttask-system get agenticruns -o json 2>/dev/null | \
    jq -r --arg uid "$customrun_cancel_uid" '.items[] | select(any(.metadata.ownerReferences[]?; .uid == $uid)) | .metadata.name' | head -1)"
  [[ -n "$native_cancel" ]] && break
  sleep 1
done
test -n "${native_cancel:-}"
kubectl --context "$CONTEXT" -n agenttask-system patch pipelinerun/lightspeed-agenttask-cancel \
  --type=merge -p '{"spec":{"status":"Cancelled"}}' >/dev/null
kubectl --context "$CONTEXT" -n agenttask-system wait --for=condition=Succeeded=False \
  customrun/"$customrun_cancel" --timeout=120s >/dev/null
cancel_reason="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun_cancel" -o jsonpath='{.status.conditions[?(@.type=="Succeeded")].reason}')"
test "$cancel_reason" = CustomRunCancelled
if kubectl --context "$CONTEXT" -n agenttask-system get agenticrun "$native_cancel" >/dev/null 2>&1; then
  echo "cancelled AgenticRun still exists" >&2
  exit 1
fi

echo "e2e_kind=passed customrun=$customrun agenticrun=$native outcome=$outcome cancelled=$customrun_cancel"
