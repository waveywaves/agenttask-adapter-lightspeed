# Lightspeed AgentTask Adapter

Experimental controller mapping a Tekton `AgentTask` `CustomRun` to an
analysis-only OpenShift Lightspeed `AgenticRun`. It is developed alongside
[TEP-0170](https://github.com/tektoncd/community/pull/1263).

TEP-0170 remains proposed. The API is unstable and this repository provides no
compatibility or product-support guarantee.

## Prototype scope

The controller currently:

- accepts only the fixed `analysis-v1` profile, a string `request` param, and
  the `outcome` and `analysis-result-name` results;
- restricts the native target to the `CustomRun` namespace;
- rejects workspaces and custom service accounts;
- deterministically creates or adopts one same-namespace `AgenticRun` per
  attempt and persists its UID before acceptance;
- observes native analysis approval without modifying it;
- maps terminal `AnalysisResult.status.actionRequired` to a bounded result and
  returns a credential-free Kubernetes reference;
- deletes only the correlated `AgenticRun` during cancellation and waits for
  `NotFound` before confirming cleanup.

The trusted profile selects the cluster-scoped `Agent/tekton-analysis`. Cluster
administrators must configure that Agent with analysis-only, read-only tools.
Prompt text is not an authorization boundary.

This remains a PoC. It omits retries, remote definitions, distributed claiming,
cleanup deadlines, metrics, conformance certification, and production support.
It uses one replica and one namespace.

## Prerequisites

- Tekton Pipelines with `CustomRun` support;
- the `AgentTask` CRD from
  [`openshift-pipelines/agenttask`](https://github.com/openshift-pipelines/agenttask);
- OpenShift Lightspeed Agentic Operator API compatible with commit
  `e4506ee41ddb099d80cdb78ddd87287fac20853f`;
- a cluster-scoped `Agent` named `tekton-analysis` and an appropriate approval
  policy.

The adapter pins an unreleased Lightspeed API pseudo-version and requires Go
1.25.7. This pin must move to a supported release before any compatibility
claim.

## Development

Until the next experimental `agenttask` module tag is published, test both
repositories in one Go workspace:

```sh
go work init ./agenttask ./agenttask-adapter-lightspeed
go test ./agenttask/... ./agenttask-adapter-lightspeed/...
```

Run the local Kind vertical slice with:

```sh
make e2e-kind
```

It installs Tekton, uses minimal test doubles for the three Agentic API CRDs,
then proves `PipelineRun` to `CustomRun` to `AgenticRun` to `AnalysisResult` to
downstream Task result consumption and correlated cancellation cleanup. It does
not replace a live Agentic Operator compatibility test.

Build the controller and manifests with `ko`:

```sh
ko apply -k config
kubectl create -f examples/lightspeed-analysis.yaml
```

The namespaced Role intentionally cannot read Secrets, modify
`AgenticRunApproval`, impersonate identities, or mutate native status.

## License

Apache License 2.0.
