# Lightspeed AgentTask Adapter

Experimental AgentTask Adapter for mapping a Tekton `CustomRun` to an OpenShift
Lightspeed `AgenticRun`. It is being prototyped alongside
[TEP-0170: AgentTask and Pluggable Agent Execution](https://github.com/tektoncd/community/pull/1263).

This repository is an experimental PoC scaffold. TEP-0170 is proposed, the
shared API is unstable, and the adapter is not usable yet. Publication does not
imply TEP acceptance, API compatibility, or product support.

## Current slice

Implemented:

- a fail-closed shell that compiles against the shared `AgentTaskAdapter`
  interface and selects `lightspeed.openshift.io/agenticrun`;
- the two bounded analysis outcomes derived from an already validated
  `actionRequired` boolean.

Not implemented:

- a controller or Kubernetes deployment;
- `AgenticRun` creation or adoption;
- approval observation; this adapter will not mutate native approvals;
- `AnalysisResult` lookup or ownership validation;
- cancellation or cleanup;
- status writing, claiming, restart recovery, RBAC, or conformance tests.

Every lifecycle method returns `ErrNotImplemented` without creating or mutating
anything. A future PoC deployment will use one active controller leader and
will not implement distributed adapter claiming. This scaffold is therefore
**not TEP-0170 conformant**.

## Entry gates

Do not import the Lightspeed API or implement its lifecycle until all of these
are resolved:

1. select a supported Lightspeed release and Go API module version;
2. confirm that an analysis-only advisory run reaches `Completed` instead of
   remaining `Proposed`;
3. confirm the public signal for waiting on `AgenticRunApproval`;
4. confirm DELETE as the supported per-run cancellation operation and define
   when cleanup is complete;
5. locate the documented `lightspeed-component-owner` role or approve the
   narrower namespaced Role from the PoC plan.

The provisional research commits are
`3e7b1ac9027aab853e395876074533d0216da1ac` and
`e4506ee41ddb099d80cdb78ddd87287fac20853f`; neither is a supported dependency
pin.

## Development

```sh
make verify
make test
```

`go.mod` pins an experimental `github.com/openshift-pipelines/agenttask`
version. Update that pin deliberately with any matching adapter contract
change.

## License

Apache License 2.0.
