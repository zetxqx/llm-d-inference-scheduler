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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrsession "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/session"
	sessionidconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionid/constants"
)

func closeRequest(sessionID string) *fwksched.InferenceRequest {
	req := &fwksched.InferenceRequest{Headers: map[string]string{":path": "/close_session"}}
	if sessionID != "" {
		key := attrsession.SessionIDDataKey.WithNonEmptyProducerName(sessionidconstants.SessionIDProducerType).String()
		req.PutAttribute(key, attrsession.SessionID(sessionID))
	}
	return req
}

// closeRecorder is an httptest handler that records /close_session calls.
type closeRecorder struct {
	received chan string
}

func newCloseRecorder() *closeRecorder {
	return &closeRecorder{received: make(chan string, 16)}
}

func (c *closeRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var payload map[string]string
	_ = json.Unmarshal(body, &payload)
	if r.URL.Path == "/close_session" {
		c.received <- payload["session_id"]
	}
	w.WriteHeader(http.StatusOK)
}

func endpointEventFor(t *testing.T, name k8stypes.NamespacedName, serverURL string) fwkdl.EndpointEvent {
	t.Helper()
	host, port, err := net.SplitHostPort(serverURL[len("http://"):])
	require.NoError(t, err)
	ep := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{NamespacedName: name, Address: host, Port: port}, &fwkdl.Metrics{})
	return fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate, Endpoint: ep}
}

func TestClose_BoundSessionRemovesBinding(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()

	tracker.PreRequest(ctx, requestWithSession("s1"), schedulingResultFor(podA))
	require.Equal(t, 1, tracker.ActiveSessions(podA))

	// The close is scheduled to the bound endpoint by the normal path;
	// PreRequest then removes the binding.
	tracker.PreRequest(ctx, closeRequest("s1"), schedulingResultFor(podA))

	assert.Equal(t, 0, tracker.ActiveSessions(podA))
	_, ok := tracker.LookupSession("s1")
	assert.False(t, ok)
}

func TestClose_DoesNotCreateBinding(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)

	tracker.PreRequest(context.Background(), closeRequest("unknown"), schedulingResultFor(podA))

	assert.Equal(t, 0, tracker.ActiveSessions(podA))
	_, ok := tracker.LookupSession("unknown")
	assert.False(t, ok, "a close must never bind a session")
}

func TestClose_UnboundSessionBroadcasts(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()

	recorderB := newCloseRecorder()
	serverB := httptest.NewServer(recorderB)
	defer serverB.Close()
	recorderC := newCloseRecorder()
	serverC := httptest.NewServer(recorderC)
	defer serverC.Close()

	podC := k8stypes.NamespacedName{Namespace: "ns", Name: "pod-c"}
	require.NoError(t, tracker.Extract(ctx, endpointEventFor(t, podB, serverB.URL)))
	require.NoError(t, tracker.Extract(ctx, endpointEventFor(t, podC, serverC.URL)))

	// Close for a session the router has no binding for; scheduled to pod-a
	// (which receives the original request via the proxy, so it is excluded
	// from the broadcast).
	tracker.PreRequest(ctx, closeRequest("lost-session"), schedulingResultFor(podA))

	for _, recorder := range []*closeRecorder{recorderB, recorderC} {
		select {
		case got := <-recorder.received:
			assert.Equal(t, "lost-session", got)
		case <-time.After(5 * time.Second):
			t.Fatal("expected a broadcast close to reach the endpoint")
		}
	}
}

func TestClose_BroadcastExcludesScheduledEndpoint(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()

	recorderA := newCloseRecorder()
	serverA := httptest.NewServer(recorderA)
	defer serverA.Close()
	recorderB := newCloseRecorder()
	serverB := httptest.NewServer(recorderB)
	defer serverB.Close()

	require.NoError(t, tracker.Extract(ctx, endpointEventFor(t, podA, serverA.URL)))
	require.NoError(t, tracker.Extract(ctx, endpointEventFor(t, podB, serverB.URL)))

	tracker.PreRequest(ctx, closeRequest("lost-session"), schedulingResultFor(podA))

	select {
	case got := <-recorderB.received:
		assert.Equal(t, "lost-session", got)
	case <-time.After(5 * time.Second):
		t.Fatal("expected a broadcast close to reach the non-scheduled endpoint")
	}
	select {
	case <-recorderA.received:
		t.Fatal("scheduled endpoint must not receive a duplicate direct close")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestClose_BoundSessionDoesNotBroadcast(t *testing.T) {
	t.Parallel()
	tracker := mustTracker(t, `{}`)
	ctx := context.Background()

	recorderB := newCloseRecorder()
	serverB := httptest.NewServer(recorderB)
	defer serverB.Close()
	require.NoError(t, tracker.Extract(ctx, endpointEventFor(t, podB, serverB.URL)))

	tracker.PreRequest(ctx, requestWithSession("s1"), schedulingResultFor(podA))
	tracker.PreRequest(ctx, closeRequest("s1"), schedulingResultFor(podA))

	select {
	case <-recorderB.received:
		t.Fatal("a close with a binding must go only to the bound endpoint")
	case <-time.After(100 * time.Millisecond):
	}
}
