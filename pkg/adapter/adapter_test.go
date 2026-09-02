/*
Copyright 2026 The AgentTask Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package adapter

import (
	"context"
	"strings"
	"testing"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	"github.com/openshift-pipelines/agenttask/pkg/framework"
	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestValidate(t *testing.T) {
	adapter := &Adapter{Client: newFakeClient(t)}
	if err := adapter.Validate(context.Background(), testTask(), testCustomRun()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := map[string]func(*agentv1alpha1.AgentTask, *pipelinev1beta1.CustomRun){
		"workspace": func(task *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun) {
			task.Spec.Workspaces = []pipelinev1.WorkspaceDeclaration{{Name: "source"}}
			run.Spec.Workspaces = []pipelinev1beta1.WorkspaceBinding{{Name: "source"}}
		},
		"custom service account": func(_ *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun) {
			run.Spec.ServiceAccountName = "other"
		},
		"unknown profile": func(task *agentv1alpha1.AgentTask, _ *pipelinev1beta1.CustomRun) {
			task.Spec.AdapterRef.Params[0].Value.StringVal = "other"
		},
		"extra declared param": func(task *agentv1alpha1.AgentTask, _ *pipelinev1beta1.CustomRun) {
			task.Spec.Params = append(task.Spec.Params, pipelinev1.ParamSpec{Name: "extra", Type: pipelinev1.ParamTypeString})
		},
		"missing result": func(task *agentv1alpha1.AgentTask, _ *pipelinev1beta1.CustomRun) {
			task.Spec.Results = task.Spec.Results[:1]
		},
		"missing request": func(_ *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun) {
			run.Spec.Params = nil
		},
		"oversized request": func(_ *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun) {
			run.Spec.Params[0].Value.StringVal = strings.Repeat("x", maxRequestBytes+1)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			task, run := testTask(), testCustomRun()
			mutate(task, run)
			if err := adapter.Validate(context.Background(), task, run); err == nil {
				t.Fatal("Validate() accepted invalid input")
			}
		})
	}
}

func TestReconcileCreatesAndAdoptsOneAgenticRun(t *testing.T) {
	creates := 0
	c := newFakeClientWithCreateUID(t, &creates)
	adapter := &Adapter{Client: c}
	request := testRequest()

	first, err := adapter.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	if first.State != framework.StateAccepted || first.ExecutionRef == nil || first.ExecutionRef.UID == "" {
		t.Fatalf("first observation = %#v", first)
	}
	second, err := adapter.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("lost-response Reconcile() error = %v", err)
	}
	request.ExecutionRef = first.ExecutionRef
	third, err := adapter.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("adoption Reconcile() error = %v", err)
	}
	if creates != 1 || second.ExecutionRef == nil || third.ExecutionRef == nil ||
		second.ExecutionRef.UID != first.ExecutionRef.UID || third.ExecutionRef.UID != first.ExecutionRef.UID {
		t.Fatalf("creates = %d, first = %#v, second = %#v, third = %#v", creates, first.ExecutionRef, second.ExecutionRef, third.ExecutionRef)
	}

	var native agenticv1alpha1.AgenticRun
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "test", Name: first.ExecutionRef.Name}, &native); err != nil {
		t.Fatalf("get AgenticRun: %v", err)
	}
	if native.Spec.Request != "analyze the failed PipelineRun" || len(native.Spec.TargetNamespaces) != 1 || native.Spec.TargetNamespaces[0] != "test" {
		t.Fatalf("AgenticRun spec = %#v", native.Spec)
	}
	if native.Spec.Analysis.Agent != ProfileAgent || !native.Spec.Execution.IsZero() || !native.Spec.Verification.IsZero() {
		t.Fatalf("AgenticRun does not use the trusted analysis-only profile: %#v", native.Spec)
	}
}

func TestReconcileRejectsDeterministicNameCollision(t *testing.T) {
	request := testRequest()
	native, err := desiredAgenticRun(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatalf("desiredAgenticRun() error = %v", err)
	}
	native.UID = "native-uid"
	native.Annotations[attemptAnnotation] = "other-attempt"
	adapter := &Adapter{Client: newFakeClient(t, native)}
	if _, err := adapter.Reconcile(context.Background(), request); err == nil {
		t.Fatal("Reconcile() adopted a foreign deterministic-name collision")
	}
}

func TestReconcileMapsCompletedAnalysis(t *testing.T) {
	for _, test := range []struct {
		name   string
		action agenticv1alpha1.ActionRequiredValue
		want   string
	}{
		{name: "action required", action: agenticv1alpha1.ActionRequiredTrue, want: OutcomeActionRequired},
		{name: "no action required", action: agenticv1alpha1.ActionRequiredFalse, want: OutcomeNoActionRequired},
	} {
		t.Run(test.name, func(t *testing.T) {
			native, result, reference := completedObjects(t, test.action)
			adapter := &Adapter{Client: newFakeClient(t, native, result)}
			request := testRequest()
			request.ExecutionRef = reference

			observation, err := adapter.Reconcile(context.Background(), request)
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if observation.State != framework.StateSucceeded || len(observation.Results) != 2 || observation.Results[0].Value != test.want {
				t.Fatalf("observation = %#v", observation)
			}
			if len(observation.Artifacts) != 1 || observation.Artifacts[0].Name != "analysis-result" {
				t.Fatalf("artifacts = %#v", observation.Artifacts)
			}
		})
	}
}

func TestReconcileRejectsForeignAnalysisResult(t *testing.T) {
	native, result, reference := completedObjects(t, agenticv1alpha1.ActionRequiredFalse)
	result.OwnerReferences[0].UID = "foreign"
	adapter := &Adapter{Client: newFakeClient(t, native, result)}
	request := testRequest()
	request.ExecutionRef = reference

	observation, err := adapter.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if observation.State != framework.StateFailed || observation.Reason != framework.ReasonInfrastructureFailed {
		t.Fatalf("observation = %#v", observation)
	}
}

func TestReconcileTreatsMissingPersistedExecutionAsFailure(t *testing.T) {
	adapter := &Adapter{Client: newFakeClient(t)}
	request := testRequest()
	request.ExecutionRef = &framework.ExecutionReference{
		APIVersion: agenticv1alpha1.GroupVersion.String(), Kind: "AgenticRun", Namespace: "test", Name: executionName(request.AttemptID), UID: "gone",
	}
	observation, err := adapter.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if observation.State != framework.StateFailed || observation.Reason != framework.ReasonInfrastructureFailed {
		t.Fatalf("observation = %#v", observation)
	}
}

func TestReconcileObservesNativeAnalysisApproval(t *testing.T) {
	request := testRequest()
	native, err := desiredAgenticRun(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatalf("desiredAgenticRun() error = %v", err)
	}
	native.UID = "native-uid"
	approval := &agenticv1alpha1.AgenticRunApproval{
		ObjectMeta: metav1.ObjectMeta{Namespace: native.Namespace, Name: native.Name},
	}
	adapter := &Adapter{Client: newFakeClient(t, native, approval)}
	request.ExecutionRef, _ = executionReference(native)

	observation, err := adapter.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if observation.State != framework.StateWaiting {
		t.Fatalf("observation = %#v, want Waiting", observation)
	}
}

func TestCancelDeletesOnlyCorrelatedAgenticRun(t *testing.T) {
	native, err := desiredAgenticRun(testRequest(), executionName(testRequest().AttemptID))
	if err != nil {
		t.Fatalf("desiredAgenticRun() error = %v", err)
	}
	native.UID = "native-uid"
	c := newFakeClient(t, native)
	adapter := &Adapter{Client: c}
	request := testRequest()
	request.ExecutionRef, _ = executionReference(native)

	first, err := adapter.Cancel(context.Background(), request)
	if err != nil {
		t.Fatalf("first Cancel() error = %v", err)
	}
	if first.State != framework.StateCancelling || first.CleanupComplete {
		t.Fatalf("first cancellation = %#v", first)
	}
	second, err := adapter.Cancel(context.Background(), request)
	if err != nil {
		t.Fatalf("second Cancel() error = %v", err)
	}
	if second.State != framework.StateCancelled || !second.CleanupComplete {
		t.Fatalf("second cancellation = %#v", second)
	}
}

func TestNativeFailureMessageDoesNotCopyNativeDetail(t *testing.T) {
	run := &agenticv1alpha1.AgenticRun{Status: agenticv1alpha1.AgenticRunStatus{Conditions: []metav1.Condition{{
		Type: "Analyzed", Status: metav1.ConditionFalse, Reason: "ProviderFailed", Message: "token sensitive-value",
	}}}}
	message := nativeFailureMessage(run)
	if !strings.Contains(message, "ProviderFailed") || strings.Contains(message, "sensitive-value") {
		t.Fatalf("nativeFailureMessage() = %q", message)
	}
}

func TestAnalysisOutcome(t *testing.T) {
	if AnalysisOutcome(true) != OutcomeActionRequired || AnalysisOutcome(false) != OutcomeNoActionRequired {
		t.Fatal("AnalysisOutcome() mapping changed")
	}
}

func completedObjects(t *testing.T, action agenticv1alpha1.ActionRequiredValue) (*agenticv1alpha1.AgenticRun, *agenticv1alpha1.AnalysisResult, *framework.ExecutionReference) {
	t.Helper()
	request := testRequest()
	native, err := desiredAgenticRun(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatalf("desiredAgenticRun() error = %v", err)
	}
	native.UID = "native-uid"
	native.Status.Conditions = []metav1.Condition{{
		Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue, Reason: "Succeeded",
	}}
	native.Status.Steps.Analysis.Results = []agenticv1alpha1.StepResultRef{{Name: "analysis-result", Outcome: agenticv1alpha1.ActionOutcomeSucceeded}}
	result := &agenticv1alpha1.AnalysisResult{
		TypeMeta: metav1.TypeMeta{APIVersion: agenticv1alpha1.GroupVersion.String(), Kind: "AnalysisResult"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test", Name: "analysis-result", UID: "result-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(native, agenticv1alpha1.GroupVersion.WithKind("AgenticRun"))},
		},
		Spec:   agenticv1alpha1.AnalysisResultSpec{AgenticRunName: native.Name},
		Status: agenticv1alpha1.AnalysisResultStatus{ActionRequired: action},
	}
	reference, err := executionReference(native)
	if err != nil {
		t.Fatalf("executionReference() error = %v", err)
	}
	return native, result, reference
}

func testTask() *agentv1alpha1.AgentTask {
	return &agentv1alpha1.AgentTask{
		TypeMeta:   metav1.TypeMeta{APIVersion: agentv1alpha1.SchemeGroupVersion.String(), Kind: "AgentTask"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "analysis", UID: "task-uid"},
		Spec: agentv1alpha1.AgentTaskSpec{
			Params: []pipelinev1.ParamSpec{{Name: ParamRequest, Type: pipelinev1.ParamTypeString}},
			Results: []agentv1alpha1.AgentTaskResult{
				{Name: ResultOutcome}, {Name: ResultAnalysisResultName},
			},
			AdapterRef: agentv1alpha1.AgentTaskAdapterRef{
				Name: Selector,
				Params: []pipelinev1.Param{{
					Name: ParamProfile, Value: pipelinev1.ParamValue{Type: pipelinev1.ParamTypeString, StringVal: ProfileAnalysis},
				}},
			},
		},
	}
}

func testCustomRun() *pipelinev1beta1.CustomRun {
	return &pipelinev1beta1.CustomRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: pipelinev1beta1.SchemeGroupVersion.String(), Kind: "CustomRun"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "run", UID: types.UID("customrun-uid")},
		Spec: pipelinev1beta1.CustomRunSpec{
			Params: []pipelinev1beta1.Param{{Name: ParamRequest, Value: *pipelinev1beta1.NewStructuredValues("analyze the failed PipelineRun")}},
		},
	}
}

func testRequest() framework.Request {
	return framework.Request{
		AgentTask: testTask(), CustomRun: testCustomRun(), AttemptNumber: 0,
		AttemptID: "customrun-uid:0", ServiceAccountName: "default",
	}
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).Build()
}

func newFakeClientWithCreateUID(t *testing.T, creates *int) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			(*creates)++
			if run, ok := object.(*agenticv1alpha1.AgenticRun); ok {
				run.UID = types.UID("native-uid")
			}
			return c.Create(ctx, object, opts...)
		}}).
		Build()
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"agenttask": agentv1alpha1.AddToScheme,
		"agentic":   agenticv1alpha1.AddToScheme,
		"pipeline":  pipelinev1beta1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s scheme: %v", name, err)
		}
	}
	return scheme
}
