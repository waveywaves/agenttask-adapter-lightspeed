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
	"testing"

	"github.com/openshift-pipelines/agenttask/pkg/framework"
	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestCustomRunToAgenticRunVerticalSlice(t *testing.T) {
	task := testTask()
	run := testCustomRun()
	run.Spec.CustomRef = &pipelinev1beta1.TaskRef{
		APIVersion: task.APIVersion, Kind: "AgentTask", Name: task.Name,
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&pipelinev1beta1.CustomRun{}, &agenticv1alpha1.AgenticRun{}, &agenticv1alpha1.AnalysisResult{}).
		WithObjects(task, run).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, inner client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if native, ok := object.(*agenticv1alpha1.AgenticRun); ok {
				native.UID = types.UID("native-uid")
			}
			return inner.Create(ctx, object, opts...)
		}}).
		Build()
	implementation := &Adapter{Client: c}
	reconciler := &framework.Reconciler{
		Client: c, Adapter: implementation,
		Options: framework.ControllerOptions{Version: "poc", InstallationID: "test"},
	}
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	for i := 0; i < 3; i++ {
		if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
			t.Fatalf("initial Reconcile() call %d error = %v", i+1, err)
		}
	}

	var native agenticv1alpha1.AgenticRun
	name := executionName("customrun-uid:0")
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "test", Name: name}, &native); err != nil {
		t.Fatalf("get AgenticRun: %v", err)
	}
	native.Status.Conditions = []metav1.Condition{{
		Type: agenticv1alpha1.AgenticRunConditionVerified, Status: metav1.ConditionTrue, Reason: "Succeeded",
	}}
	native.Status.Steps.Analysis.Results = []agenticv1alpha1.StepResultRef{{Name: "analysis-result", Outcome: agenticv1alpha1.ActionOutcomeSucceeded}}
	if err := c.Status().Update(context.Background(), &native); err != nil {
		t.Fatalf("update AgenticRun status: %v", err)
	}
	result := &agenticv1alpha1.AnalysisResult{
		TypeMeta: metav1.TypeMeta{APIVersion: agenticv1alpha1.GroupVersion.String(), Kind: "AnalysisResult"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test", Name: "analysis-result", UID: "result-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&native, agenticv1alpha1.GroupVersion.WithKind("AgenticRun"))},
		},
		Spec:   agenticv1alpha1.AnalysisResultSpec{AgenticRunName: native.Name},
		Status: agenticv1alpha1.AnalysisResultStatus{ActionRequired: agenticv1alpha1.ActionRequiredFalse},
	}
	if err := c.Create(context.Background(), result); err != nil {
		t.Fatalf("create AnalysisResult: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
		t.Fatalf("terminal Reconcile() error = %v", err)
	}

	var completed pipelinev1beta1.CustomRun
	if err := c.Get(context.Background(), key.NamespacedName, &completed); err != nil {
		t.Fatalf("get completed CustomRun: %v", err)
	}
	condition := completed.Status.GetCondition(apis.ConditionSucceeded)
	if condition == nil || !condition.IsTrue() {
		t.Fatalf("CustomRun condition = %#v", condition)
	}
	if len(completed.Status.Results) != 2 || completed.Status.Results[0].Value != OutcomeNoActionRequired {
		t.Fatalf("CustomRun results = %#v", completed.Status.Results)
	}
	profile, err := framework.DecodeStatusProfile(&completed)
	if err != nil || profile.ExecutionRef == nil || profile.ExecutionRef.UID != native.UID {
		t.Fatalf("status profile = %#v, error = %v", profile, err)
	}
}
