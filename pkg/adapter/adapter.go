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

// Package adapter contains the experimental Lightspeed AgentTask Adapter.
package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	"unicode/utf8"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	"github.com/openshift-pipelines/agenttask/pkg/framework"
	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	Selector = "lightspeed.openshift.io/agenticrun"

	ProfileAnalysis = "analysis-v1"
	ProfileAgent    = "tekton-analysis"
	ParamProfile    = "profile"
	ParamRequest    = "request"

	ResultOutcome            = "outcome"
	ResultAnalysisResultName = "analysis-result-name"

	OutcomeActionRequired   = "action-required"
	OutcomeNoActionRequired = "no-action-required"

	attemptAnnotation = "agent.tekton.dev/attempt-id"
	profileAnnotation = "agent.tekton.dev/profile-digest"
	sourceLabel       = "agentic.openshift.io/source"
	profileLabel      = "agent.tekton.dev/profile"
	customRunUIDLabel = "agent.tekton.dev/customrun-uid"
)

const (
	pollInterval    = 2 * time.Second
	maxRequestBytes = 32768
)

// Adapter maps one AgentTask CustomRun to an analysis-only AgenticRun.
type Adapter struct {
	Client client.Client
}

var _ framework.AgentTaskAdapter = (*Adapter)(nil)

func (*Adapter) Name(context.Context) string { return Selector }

func (a *Adapter) Validate(_ context.Context, task *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun) error {
	if a.Client == nil {
		return fmt.Errorf("kubernetes client is required")
	}
	if task == nil || run == nil {
		return fmt.Errorf("agent task and custom run are required")
	}
	if task.Namespace != run.Namespace {
		return fmt.Errorf("agent task and custom run must share a namespace")
	}
	if len(task.Spec.Workspaces) != 0 || len(run.Spec.Workspaces) != 0 {
		return fmt.Errorf("%w: profile %s", framework.ErrWorkspaceNotSupported, ProfileAnalysis)
	}
	if run.Spec.ServiceAccountName != "" && run.Spec.ServiceAccountName != "default" {
		return fmt.Errorf("profile %s does not map CustomRun service accounts", ProfileAnalysis)
	}
	if err := validateProfile(task); err != nil {
		return err
	}
	if err := validateParams(task); err != nil {
		return err
	}
	if err := validateResults(task); err != nil {
		return err
	}
	_, err := requestValue(run)
	return err
}

func (a *Adapter) Reconcile(ctx context.Context, request framework.Request) (framework.Observation, error) {
	name := executionName(request.AttemptID)
	var run agenticv1alpha1.AgenticRun
	err := a.Client.Get(ctx, client.ObjectKey{Namespace: request.CustomRun.Namespace, Name: name}, &run)
	if apierrors.IsNotFound(err) {
		if request.ExecutionRef != nil {
			return failedObservation(request.ExecutionRef, "The AgenticRun disappeared before completion"), nil
		}
		desired, buildErr := desiredAgenticRun(request, name)
		if buildErr != nil {
			return framework.Observation{}, buildErr
		}
		if createErr := a.Client.Create(ctx, desired); createErr != nil {
			if !apierrors.IsAlreadyExists(createErr) {
				return framework.Observation{}, createErr
			}
			if getErr := a.Client.Get(ctx, client.ObjectKeyFromObject(desired), &run); getErr != nil {
				return framework.Observation{}, getErr
			}
		} else {
			run = *desired
		}
	} else if err != nil {
		return framework.Observation{}, err
	}

	if err := validateOwnedRun(&run, request); err != nil {
		return framework.Observation{}, err
	}
	reference, err := executionReference(&run)
	if err != nil {
		return framework.Observation{}, err
	}
	return a.observe(ctx, &run, reference)
}

func (a *Adapter) Cancel(ctx context.Context, request framework.Request) (framework.Observation, error) {
	name := executionName(request.AttemptID)
	var run agenticv1alpha1.AgenticRun
	if err := a.Client.Get(ctx, client.ObjectKey{Namespace: request.CustomRun.Namespace, Name: name}, &run); err != nil {
		if apierrors.IsNotFound(err) {
			return framework.Observation{State: framework.StateCancelled, CleanupComplete: true}, nil
		}
		return framework.Observation{}, err
	}
	if err := validateOwnedRun(&run, request); err != nil {
		return framework.Observation{}, err
	}
	reference, err := executionReference(&run)
	if err != nil {
		return framework.Observation{}, err
	}
	if run.DeletionTimestamp.IsZero() {
		uid := run.UID
		if err := a.Client.Delete(ctx, &run, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
			return framework.Observation{}, err
		}
	}
	return framework.Observation{
		State:        framework.StateCancelling,
		Message:      "Waiting for AgenticRun deletion",
		ExecutionRef: reference,
		RequeueAfter: pollInterval,
	}, nil
}

func (a *Adapter) observe(ctx context.Context, run *agenticv1alpha1.AgenticRun, reference *framework.ExecutionReference) (framework.Observation, error) {
	phase := agenticv1alpha1.DerivePhase(run.Status.Conditions)
	base := framework.Observation{ExecutionRef: reference, RequeueAfter: pollInterval}
	switch phase {
	case agenticv1alpha1.AgenticRunPhasePending, agenticv1alpha1.AgenticRunPhaseAnalyzing:
		waiting, err := a.analysisApprovalWaiting(ctx, run)
		if err != nil {
			return framework.Observation{}, err
		}
		if waiting {
			base.State = framework.StateWaiting
			base.Message = "Waiting for Lightspeed analysis approval"
			return base, nil
		}
		if phase == agenticv1alpha1.AgenticRunPhasePending {
			base.State = framework.StateAccepted
			base.Message = "AgenticRun accepted"
			return base, nil
		}
		base.State = framework.StateRunning
		base.Message = "Lightspeed analysis is running"
		return base, nil
	case agenticv1alpha1.AgenticRunPhaseProposed,
		agenticv1alpha1.AgenticRunPhaseExecuting,
		agenticv1alpha1.AgenticRunPhaseVerifying,
		agenticv1alpha1.AgenticRunPhaseEscalating:
		base.State = framework.StateRunning
		base.Message = "Lightspeed is finalizing the advisory run"
		return base, nil
	case agenticv1alpha1.AgenticRunPhaseCompleted:
		return a.completedObservation(ctx, run, reference)
	case agenticv1alpha1.AgenticRunPhaseDenied:
		base.State = framework.StateFailed
		base.Reason = framework.ReasonAgentFailed
		base.Message = "The native run was denied"
		return base, nil
	case agenticv1alpha1.AgenticRunPhaseFailed,
		agenticv1alpha1.AgenticRunPhaseEscalated,
		agenticv1alpha1.AgenticRunPhaseEmergencyStopped:
		return failedObservation(reference, nativeFailureMessage(run)), nil
	default:
		return framework.Observation{}, fmt.Errorf("unsupported AgenticRun phase %q", phase)
	}
}

func (a *Adapter) analysisApprovalWaiting(ctx context.Context, run *agenticv1alpha1.AgenticRun) (bool, error) {
	var approval agenticv1alpha1.AgenticRunApproval
	if err := a.Client.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Name}, &approval); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	for _, stage := range approval.Spec.Stages {
		if stage.Type == agenticv1alpha1.ApprovalStageAnalysis {
			return false, nil
		}
	}
	return true, nil
}

func (a *Adapter) completedObservation(ctx context.Context, run *agenticv1alpha1.AgenticRun, reference *framework.ExecutionReference) (framework.Observation, error) {
	refs := run.Status.Steps.Analysis.Results
	if len(refs) == 0 {
		return waitingForResultObservation(reference), nil
	}
	latest := refs[len(refs)-1]
	if latest.Outcome != agenticv1alpha1.ActionOutcomeSucceeded {
		return failedObservation(reference, "Latest AnalysisResult did not succeed"), nil
	}
	name := latest.Name
	var result agenticv1alpha1.AnalysisResult
	if err := a.Client.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: name}, &result); err != nil {
		if apierrors.IsNotFound(err) {
			return waitingForResultObservation(reference), nil
		}
		return framework.Observation{}, err
	}
	if err := validateAnalysisResult(&result, run); err != nil {
		return failedObservation(reference, err.Error()), nil
	}

	var outcome string
	switch result.Status.ActionRequired {
	case agenticv1alpha1.ActionRequiredTrue:
		outcome = OutcomeActionRequired
	case agenticv1alpha1.ActionRequiredFalse:
		outcome = OutcomeNoActionRequired
	default:
		return waitingForResultObservation(reference), nil
	}
	return framework.Observation{
		State:        framework.StateSucceeded,
		Message:      "Lightspeed analysis completed",
		ExecutionRef: reference,
		Results: []pipelinev1beta1.CustomRunResult{
			{Name: ResultOutcome, Value: outcome},
			{Name: ResultAnalysisResultName, Value: result.Name},
		},
		Artifacts: []framework.Reference{{
			Name: "analysis-result",
			URI:  fmt.Sprintf("k8s://agentic.openshift.io/v1alpha1/namespaces/%s/analysisresults/%s", result.Namespace, result.Name),
		}},
	}, nil
}

func desiredAgenticRun(request framework.Request, name string) (*agenticv1alpha1.AgenticRun, error) {
	value, err := requestValue(request.CustomRun)
	if err != nil {
		return nil, err
	}
	run := &agenticv1alpha1.AgenticRun{
		TypeMeta: metav1.TypeMeta{APIVersion: agenticv1alpha1.GroupVersion.String(), Kind: "AgenticRun"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: request.CustomRun.Namespace,
			Labels: map[string]string{
				sourceLabel:       "agenttask",
				profileLabel:      ProfileAnalysis,
				customRunUIDLabel: correlationLabel(request.CustomRun.UID),
			},
			Annotations: map[string]string{attemptAnnotation: request.AttemptID},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(request.CustomRun, schema.GroupVersionKind{
				Group: pipelinev1beta1.SchemeGroupVersion.Group, Version: pipelinev1beta1.SchemeGroupVersion.Version, Kind: "CustomRun",
			})},
		},
		Spec: agenticv1alpha1.AgenticRunSpec{
			Request:          value,
			TargetNamespaces: []string{request.CustomRun.Namespace},
			Analysis:         agenticv1alpha1.AgenticRunStep{Agent: ProfileAgent},
			TTLAfterTerminal: ptr.To[int32](0),
		},
	}
	digest, err := specDigest(run.Spec)
	if err != nil {
		return nil, err
	}
	run.Annotations[profileAnnotation] = digest
	return run, nil
}

func validateOwnedRun(run *agenticv1alpha1.AgenticRun, request framework.Request) error {
	if run.Annotations[attemptAnnotation] != request.AttemptID {
		return fmt.Errorf("existing AgenticRun has a different attempt identity")
	}
	owner := metav1.GetControllerOf(run)
	if owner == nil || owner.UID != request.CustomRun.UID || owner.Kind != "CustomRun" {
		return fmt.Errorf("existing AgenticRun is not controlled by the CustomRun")
	}
	desired, err := desiredAgenticRun(request, run.Name)
	if err != nil {
		return err
	}
	if run.Annotations[profileAnnotation] != desired.Annotations[profileAnnotation] || !reflect.DeepEqual(run.Spec, desired.Spec) {
		return fmt.Errorf("existing AgenticRun does not match the trusted profile")
	}
	if reference := request.ExecutionRef; reference != nil {
		if reference.APIVersion != agenticv1alpha1.GroupVersion.String() || reference.Kind != "AgenticRun" ||
			reference.Namespace != run.Namespace || reference.Name != run.Name || reference.UID != run.UID {
			return fmt.Errorf("persisted AgenticRun identity does not match the native object")
		}
	}
	return nil
}

func validateAnalysisResult(result *agenticv1alpha1.AnalysisResult, run *agenticv1alpha1.AgenticRun) error {
	if result.Spec.AgenticRunName != run.Name {
		return fmt.Errorf("analysis result references a different agentic run")
	}
	owner := metav1.GetControllerOf(result)
	if owner == nil || owner.UID != run.UID || owner.Kind != "AgenticRun" {
		return fmt.Errorf("analysis result is not controlled by the agentic run")
	}
	return nil
}

func validateProfile(task *agentv1alpha1.AgentTask) error {
	if len(task.Spec.AdapterRef.Params) != 1 {
		return fmt.Errorf("adapterRef must contain only the %s parameter", ParamProfile)
	}
	profile := task.Spec.AdapterRef.Params[0]
	if profile.Name != ParamProfile || profile.Value.Type != pipelinev1.ParamTypeString || profile.Value.StringVal != ProfileAnalysis {
		return fmt.Errorf("adapterRef profile must be %s", ProfileAnalysis)
	}
	return nil
}

func validateParams(task *agentv1alpha1.AgentTask) error {
	if len(task.Spec.Params) != 1 || task.Spec.Params[0].Name != ParamRequest {
		return fmt.Errorf("profile %s requires only the %s param", ProfileAnalysis, ParamRequest)
	}
	paramType := task.Spec.Params[0].Type
	if paramType != "" && paramType != pipelinev1.ParamTypeString {
		return fmt.Errorf("profile %s requires a string %s param", ProfileAnalysis, ParamRequest)
	}
	return nil
}

func validateResults(task *agentv1alpha1.AgentTask) error {
	seen := make(map[string]struct{}, len(task.Spec.Results))
	for _, result := range task.Spec.Results {
		seen[result.Name] = struct{}{}
	}
	if len(seen) != 2 {
		return fmt.Errorf("profile %s requires exactly two results", ProfileAnalysis)
	}
	for _, name := range []string{ResultOutcome, ResultAnalysisResultName} {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("profile %s requires result %s", ProfileAnalysis, name)
		}
	}
	return nil
}

func requestValue(run *pipelinev1beta1.CustomRun) (string, error) {
	if run == nil {
		return "", fmt.Errorf("custom run is required")
	}
	param := run.Spec.GetParam(ParamRequest)
	if param == nil || param.Value.Type != pipelinev1beta1.ParamTypeString || param.Value.StringVal == "" {
		return "", fmt.Errorf("custom run param %s must be a nonempty string", ParamRequest)
	}
	if !utf8.ValidString(param.Value.StringVal) || len(param.Value.StringVal) > maxRequestBytes {
		return "", fmt.Errorf("custom run param %s must be valid UTF-8 and at most %d bytes", ParamRequest, maxRequestBytes)
	}
	return param.Value.StringVal, nil
}

func executionName(attemptID string) string {
	digest := sha256.Sum256([]byte(attemptID))
	return "agenttask-" + hex.EncodeToString(digest[:10])
}

func correlationLabel(uid types.UID) string {
	digest := sha256.Sum256([]byte(uid))
	return hex.EncodeToString(digest[:10])
}

func executionReference(run *agenticv1alpha1.AgenticRun) (*framework.ExecutionReference, error) {
	if run.UID == "" {
		return nil, fmt.Errorf("agentic run has no server assigned UID")
	}
	return &framework.ExecutionReference{
		APIVersion: agenticv1alpha1.GroupVersion.String(),
		Kind:       "AgenticRun",
		Namespace:  run.Namespace,
		Name:       run.Name,
		UID:        run.UID,
	}, nil
}

func specDigest(spec agenticv1alpha1.AgenticRunSpec) (string, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("serialize trusted AgenticRun profile: %w", err)
	}
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func waitingForResultObservation(reference *framework.ExecutionReference) framework.Observation {
	return framework.Observation{
		State: framework.StateRunning, Message: "Waiting for the completed AnalysisResult",
		ExecutionRef: reference, RequeueAfter: pollInterval,
	}
}

func failedObservation(reference *framework.ExecutionReference, message string) framework.Observation {
	return framework.Observation{
		State: framework.StateFailed, Reason: framework.ReasonInfrastructureFailed,
		Message: message, ExecutionRef: reference,
	}
}

func nativeFailureMessage(run *agenticv1alpha1.AgenticRun) string {
	for i := len(run.Status.Conditions) - 1; i >= 0; i-- {
		condition := run.Status.Conditions[i]
		if condition.Status == metav1.ConditionFalse && condition.Reason != "" {
			return boundedMessage("AgenticRun failed with reason " + condition.Reason)
		}
	}
	return "AgenticRun terminated without a successful analysis"
}

func boundedMessage(message string) string {
	if !utf8.ValidString(message) {
		return "AgenticRun returned an invalid UTF-8 status message"
	}
	if len(message) <= framework.MaxConditionMessageBytes {
		return message
	}
	message = message[:framework.MaxConditionMessageBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}

func AnalysisOutcome(actionRequired bool) string {
	if actionRequired {
		return OutcomeActionRequired
	}
	return OutcomeNoActionRequired
}
