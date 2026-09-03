/*
Copyright 2026 The Kubernetes Authors.

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

package sessionbinding_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrsession "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/session"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionbinding"
	sessionidconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionid/constants"
	testutils "github.com/llm-d/llm-d-router/test/utils"
)

func mustTracker(t *testing.T, params string) *sessionbinding.Tracker {
	t.Helper()
	plg, err := sessionbinding.Factory("session-binding-tracker",
		fwkplugin.StrictDecoder(json.RawMessage(params)), testutils.NewTestHandle(t.Context()))
	require.NoError(t, err)
	tracker, ok := plg.(*sessionbinding.Tracker)
	require.True(t, ok, "factory must return *Tracker")
	return tracker
}

func requestWithSession(sessionID string) *fwksched.InferenceRequest {
	req := &fwksched.InferenceRequest{}
	if sessionID != "" {
		key := attrsession.SessionIDDataKey.WithNonEmptyProducerName(sessionidconstants.SessionIDProducerType)
		req.PutAttribute(key, attrsession.SessionID(sessionID))
	}
	return req
}

func schedulingResultFor(endpoint k8stypes.NamespacedName) *fwksched.SchedulingResult {
	ep := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: endpoint}, &fwkdl.Metrics{}, nil)
	return &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			"default": {TargetEndpoints: []fwksched.Endpoint{ep}},
		},
	}
}

func TestFactory_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		params    json.RawMessage
		nilHandle bool
		wantErr   string
	}{
		{name: "defaults", params: json.RawMessage(`{}`)},
		{name: "explicit ttl and bound", params: json.RawMessage(`{"ttl":"30m","maxSessions":1000}`)},
		{name: "nil raw message", params: nil},
		{name: "invalid ttl", params: json.RawMessage(`{"ttl":"soon"}`), wantErr: "invalid ttl"},
		{name: "negative maxSessions", params: json.RawMessage(`{"maxSessions":-1}`), wantErr: "non-negative maxSessions"},
		{name: "unknown field", params: json.RawMessage(`{"other":true}`), wantErr: "failed to parse"},
		{name: "nil handle", params: json.RawMessage(`{}`), nilHandle: true, wantErr: "requires a plugin handle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var handle fwkplugin.Handle
			if !tc.nilHandle {
				handle = testutils.NewTestHandle(t.Context())
			}
			_, err := sessionbinding.Factory("p", fwkplugin.StrictDecoder(tc.params), handle)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestPreRequestThenProduce(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()

	// First turn: no binding yet, Produce publishes nothing.
	first := requestWithSession("s1")
	require.NoError(t, tracker.Produce(ctx, first, nil))
	_, ok := attrsession.ReadSessionBinding(first)
	assert.False(t, ok, "unbound session must not get a binding attribute")

	// Picker selects pod-a; PreRequest records the binding.
	tracker.PreRequest(ctx, first, schedulingResultFor(podA))

	// Later turn: Produce publishes the binding.
	second := requestWithSession("s1")
	require.NoError(t, tracker.Produce(ctx, second, nil))
	binding, ok := attrsession.ReadSessionBinding(second)
	require.True(t, ok)
	assert.Equal(t, podA, binding.Endpoint)
}

func TestProduce_NoSessionID(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)

	req := requestWithSession("")
	require.NoError(t, tracker.Produce(context.Background(), req, nil))
	_, ok := attrsession.ReadSessionBinding(req)
	assert.False(t, ok)

	require.NoError(t, tracker.Produce(context.Background(), nil, nil))
}

func TestPreRequest_TableFull(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{"maxSessions":1}`)
	ctx := context.Background()

	tracker.PreRequest(ctx, requestWithSession("s1"), schedulingResultFor(podA))
	tracker.PreRequest(ctx, requestWithSession("s2"), schedulingResultFor(podB))

	_, ok := tracker.LookupSession("s1")
	assert.True(t, ok)
	_, ok = tracker.LookupSession("s2")
	assert.False(t, ok, "session beyond the bound must stay unbound")
}

func TestPreRequest_IgnoresIncompleteResults(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()
	req := requestWithSession("s1")

	tracker.PreRequest(ctx, req, nil)
	tracker.PreRequest(ctx, req, &fwksched.SchedulingResult{PrimaryProfileName: "default"})
	tracker.PreRequest(ctx, req, &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults:     map[string]*fwksched.ProfileRunResult{"default": {}},
	})
	tracker.PreRequest(ctx, requestWithSession(""), schedulingResultFor(podA))

	assert.Equal(t, 0, tracker.ActiveSessions(podA))
}

func TestExtract_EventDeleteDropsBindings(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()

	tracker.PreRequest(ctx, requestWithSession("s1"), schedulingResultFor(podA))
	tracker.PreRequest(ctx, requestWithSession("s2"), schedulingResultFor(podA))
	tracker.PreRequest(ctx, requestWithSession("s3"), schedulingResultFor(podB))

	deleted := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: podA}, &fwkdl.Metrics{})
	require.NoError(t, tracker.Extract(ctx, fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: deleted}))

	assert.Equal(t, 0, tracker.ActiveSessions(podA))
	assert.Equal(t, 1, tracker.ActiveSessions(podB))

	require.NoError(t, tracker.Extract(ctx, fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: nil}))
}

func TestRemoveSession(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()

	tracker.PreRequest(ctx, requestWithSession("s1"), schedulingResultFor(podA))

	endpoint, ok := tracker.RemoveSession("s1", sessionbinding.ReasonClose)
	require.True(t, ok)
	assert.Equal(t, podA, endpoint)

	_, ok = tracker.RemoveSession("s1", sessionbinding.ReasonClose)
	assert.False(t, ok)
}

func TestDumpState(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()

	tracker.PreRequest(ctx, requestWithSession("s1"), schedulingResultFor(podA))

	raw, err := tracker.DumpState()
	require.NoError(t, err)

	var state struct {
		Bindings []sessionbinding.BindingInfo `json:"bindings"`
		Total    int                          `json:"total"`
	}
	require.NoError(t, json.Unmarshal(raw, &state))
	require.Equal(t, 1, state.Total)
	assert.Equal(t, "s1", state.Bindings[0].SessionID)
	assert.Equal(t, podA, state.Bindings[0].Endpoint)
}

func TestProducesAndConsumes(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)

	expectedKey := attrsession.SessionBindingDataKey.WithNonEmptyProducerName("session-binding-tracker")
	_, ok := tracker.Produces()[expectedKey]
	assert.True(t, ok, "Produces() must declare %v", expectedKey)

	_, ok = tracker.Consumes().Required[attrsession.SessionIDDataKey]
	assert.True(t, ok, "Consumes() must require the SessionID attribute")
}
