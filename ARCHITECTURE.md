# Architecture

The Lightspeed AgentTask Adapter embeds the AgentTask reconciler and maps one
Tekton `CustomRun` to one analysis-only Lightspeed `AgenticRun`.

```text
PipelineRun
  -> CustomRun (AgentTask)
    -> AgenticRun
      -> AnalysisResult
    -> CustomRun results
  -> downstream Pipeline tasks
```

The adapter owns only the mapping and lifecycle boundary. The Lightspeed
Agentic Operator owns approvals, sandbox resources, provider credentials, model
execution, and native results.

## Current namespace contract

The current integration requires these resources to share one namespace:

- the adapter Deployment;
- AgentTask definitions and their CustomRuns;
- AgenticRuns and AgenticRunApprovals;
- AnalysisResults;
- Agentic Operator sandbox inputs, Pods, ServiceAccounts, and namespaced RBAC;
- provider credential Secrets referenced by the Agentic configuration.

The supplied manifests use `agenttask-system`. The Agentic Operator must also be
configured to use `agenttask-system` for this integration.

This constraint exists because:

1. Kubernetes does not allow a namespaced owner reference to target an object in
   another namespace. The adapter uses a same-namespace CustomRun controller
   reference on each AgenticRun for trusted correlation and garbage collection.
2. The adapter creates AgenticRuns and reads AnalysisResults in the CustomRun
   namespace.
3. The tested Agentic Operator creates sandbox inputs and results in its
   configured namespace and resolves sandbox pod events back to AgenticRuns in
   that namespace.

## Downside

Co-location is a known prototype limitation:

- PipelineRuns outside the shared namespace cannot use this adapter installation;
- one central installation cannot serve multiple tenant namespaces;
- workload and control-plane resources share a namespace and therefore require
  especially careful namespace RBAC;
- moving either component independently breaks reconciliation and result lookup.

Co-location is the smallest reliable contract for the current implementations.
It is not a claim that this layout is suitable for production multi-tenancy.

## Future namespace models

Supporting multiple workload namespaces requires an explicit shared contract,
not only changing one `Namespace` field. Viable designs include:

1. Make the Agentic Operator run-namespace aware: watch permitted namespaces and
   create inputs, sandboxes, approvals, and results beside each AgenticRun.
2. Keep AgenticRuns in a central operator namespace: replace cross-namespace
   ownership with finalizer-based cleanup and explicit source references, then
   define cross-namespace authorization and result access.

Either design changes watches, RBAC, identity, cleanup, and tenancy boundaries
and requires cross-namespace end-to-end tests.

## Security boundaries

- The adapter cannot read Secrets, approve runs, impersonate users, or update
  native status.
- CustomRun service-account overrides are rejected because the fixed native
  profile cannot preserve that identity.
- Native sandbox permissions come from the Agentic Operator and selected Agent,
  not from the PipelineRun ServiceAccount.
- Prompt text is not an authorization boundary.
