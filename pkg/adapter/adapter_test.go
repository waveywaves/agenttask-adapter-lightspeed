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
	"errors"
	"reflect"
	"testing"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	"github.com/openshift-pipelines/agenttask/pkg/framework"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
)

func TestSelector(t *testing.T) {
	if got := (&Adapter{}).Name(context.Background()); got != "lightspeed.openshift.io/agenticrun" {
		t.Fatalf("Name() = %q, want lightspeed.openshift.io/agenticrun", got)
	}
}

func TestAnalysisOutcome(t *testing.T) {
	tests := []struct {
		name           string
		actionRequired bool
		want           string
	}{
		{name: "action required", actionRequired: true, want: "action-required"},
		{name: "no action required", actionRequired: false, want: "no-action-required"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := AnalysisOutcome(test.actionRequired); got != test.want {
				t.Fatalf("AnalysisOutcome(%t) = %q, want %q", test.actionRequired, got, test.want)
			}
		})
	}
}

func TestLifecycleFailsClosedWithoutMutation(t *testing.T) {
	ctx := context.Background()
	definition := &agentv1alpha1.AgentTask{}
	run := &pipelinev1beta1.CustomRun{}
	definitionBefore := definition.DeepCopy()
	runBefore := run.DeepCopy()
	request := framework.Request{AgentTask: definition, CustomRun: run}
	adapter := &Adapter{}

	if err := adapter.Validate(ctx, definition, run); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Validate() error = %v, want ErrNotImplemented", err)
	}
	for name, call := range map[string]func() (framework.Observation, error){
		"Reconcile": func() (framework.Observation, error) { return adapter.Reconcile(ctx, request) },
		"Cancel":    func() (framework.Observation, error) { return adapter.Cancel(ctx, request) },
	} {
		t.Run(name, func(t *testing.T) {
			observation, err := call()
			if !errors.Is(err, ErrNotImplemented) {
				t.Fatalf("%s() error = %v, want ErrNotImplemented", name, err)
			}
			if !reflect.DeepEqual(observation, framework.Observation{}) {
				t.Fatalf("%s() observation = %#v, want zero value", name, observation)
			}
		})
	}

	if !reflect.DeepEqual(definition, definitionBefore) {
		t.Fatal("lifecycle methods mutated AgentTask")
	}
	if !reflect.DeepEqual(run, runBefore) {
		t.Fatal("lifecycle methods mutated CustomRun")
	}
}
