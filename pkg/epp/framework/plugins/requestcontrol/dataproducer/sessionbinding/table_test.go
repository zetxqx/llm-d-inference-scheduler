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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionbinding"
)

var (
	podA = k8stypes.NamespacedName{Namespace: "ns", Name: "pod-a"}
	podB = k8stypes.NamespacedName{Namespace: "ns", Name: "pod-b"}
)

type removal struct {
	sessionID string
	endpoint  k8stypes.NamespacedName
	reason    sessionbinding.Reason
}

// removalRecorder collects OnRemove invocations; safe for concurrent use.
type removalRecorder struct {
	mu       sync.Mutex
	removals []removal
}

func (r *removalRecorder) record(sessionID string, endpoint k8stypes.NamespacedName, reason sessionbinding.Reason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removals = append(r.removals, removal{sessionID, endpoint, reason})
}

func (r *removalRecorder) all() []removal {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]removal(nil), r.removals...)
}

func TestBindAndLookup(t *testing.T) {
	t.Parallel()
	table := sessionbinding.NewTable(sessionbinding.TableConfig{})

	require.True(t, table.Bind("s1", podA))

	got, ok := table.Lookup("s1")
	require.True(t, ok)
	assert.Equal(t, podA, got)

	_, ok = table.Lookup("unknown")
	assert.False(t, ok)

	assert.Equal(t, 1, table.Len())
	assert.Equal(t, 1, table.ActiveSessions(podA))
}

func TestBindRebindsToNewEndpoint(t *testing.T) {
	t.Parallel()
	rec := &removalRecorder{}
	table := sessionbinding.NewTable(sessionbinding.TableConfig{OnRemove: rec.record})

	require.True(t, table.Bind("s1", podA))
	require.True(t, table.Bind("s1", podB))

	got, ok := table.Lookup("s1")
	require.True(t, ok)
	assert.Equal(t, podB, got)
	assert.Equal(t, 0, table.ActiveSessions(podA))
	assert.Equal(t, 1, table.ActiveSessions(podB))
	assert.Empty(t, rec.all(), "a rebind is a move, not a removal")
}

func TestBindRejectsBeyondMaxSessions(t *testing.T) {
	t.Parallel()
	table := sessionbinding.NewTable(sessionbinding.TableConfig{MaxSessions: 2})

	require.True(t, table.Bind("s1", podA))
	require.True(t, table.Bind("s2", podA))
	assert.False(t, table.Bind("s3", podB), "new session beyond bound must be rejected")

	assert.True(t, table.Bind("s1", podB), "existing session must refresh at capacity")
	assert.Equal(t, 2, table.Len())
}

func TestRemove(t *testing.T) {
	t.Parallel()
	rec := &removalRecorder{}
	table := sessionbinding.NewTable(sessionbinding.TableConfig{OnRemove: rec.record})

	require.True(t, table.Bind("s1", podA))

	endpoint, ok := table.Remove("s1", sessionbinding.ReasonClose)
	require.True(t, ok)
	assert.Equal(t, podA, endpoint)
	assert.Equal(t, 0, table.Len())
	assert.Equal(t, 0, table.ActiveSessions(podA))
	assert.Equal(t, []removal{{"s1", podA, sessionbinding.ReasonClose}}, rec.all())

	_, ok = table.Remove("s1", sessionbinding.ReasonClose)
	assert.False(t, ok, "second remove must report absence")
}

func TestRemoveEndpoint(t *testing.T) {
	t.Parallel()
	rec := &removalRecorder{}
	table := sessionbinding.NewTable(sessionbinding.TableConfig{OnRemove: rec.record})

	require.True(t, table.Bind("s1", podA))
	require.True(t, table.Bind("s2", podA))
	require.True(t, table.Bind("s3", podB))

	removed := table.RemoveEndpoint(podA, sessionbinding.ReasonPodDelete)
	assert.Equal(t, 2, removed)
	assert.Equal(t, 1, table.Len())
	assert.Equal(t, 0, table.ActiveSessions(podA))
	assert.Equal(t, 1, table.ActiveSessions(podB))

	for _, r := range rec.all() {
		assert.Equal(t, sessionbinding.ReasonPodDelete, r.reason)
		assert.Equal(t, podA, r.endpoint)
	}

	_, ok := table.Lookup("s3")
	assert.True(t, ok, "bindings on other endpoints must survive")
}

func TestIdleExpiry(t *testing.T) {
	t.Parallel()
	rec := &removalRecorder{}
	table := sessionbinding.NewTable(sessionbinding.TableConfig{
		TTL:           20 * time.Millisecond,
		SweepInterval: 5 * time.Millisecond,
		OnRemove:      rec.record,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	table.Start(ctx)

	require.True(t, table.Bind("s1", podA))

	require.Eventually(t, func() bool {
		_, ok := table.Lookup("s1")
		return !ok
	}, 2*time.Second, 5*time.Millisecond, "binding must expire after the TTL")

	removals := rec.all()
	require.Len(t, removals, 1)
	assert.Equal(t, sessionbinding.ReasonIdle, removals[0].reason)
	assert.Equal(t, 0, table.ActiveSessions(podA))
}

func TestBindRefreshesIdleDeadline(t *testing.T) {
	t.Parallel()
	table := sessionbinding.NewTable(sessionbinding.TableConfig{
		TTL:           150 * time.Millisecond,
		SweepInterval: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	table.Start(ctx)

	require.True(t, table.Bind("s1", podA))
	for range 5 {
		time.Sleep(50 * time.Millisecond)
		require.True(t, table.Bind("s1", podA))
	}

	_, ok := table.Lookup("s1")
	assert.True(t, ok, "refreshed binding must not expire")
}

func TestSnapshot(t *testing.T) {
	t.Parallel()
	table := sessionbinding.NewTable(sessionbinding.TableConfig{})

	require.True(t, table.Bind("s1", podA))
	require.True(t, table.Bind("s2", podB))

	snapshot := table.Snapshot()
	require.Len(t, snapshot, 2)
	byID := map[string]k8stypes.NamespacedName{}
	for _, b := range snapshot {
		byID[b.SessionID] = b.Endpoint
		assert.False(t, b.LastSeen.IsZero())
	}
	assert.Equal(t, map[string]k8stypes.NamespacedName{"s1": podA, "s2": podB}, byID)
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()
	table := sessionbinding.NewTable(sessionbinding.TableConfig{OnRemove: func(string, k8stypes.NamespacedName, sessionbinding.Reason) {}})

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			pod := podA
			if n%2 == 0 {
				pod = podB
			}
			for j := range 100 {
				id := string(rune('a'+n)) + "-session"
				table.Bind(id, pod)
				table.Lookup(id)
				table.ActiveSessions(pod)
				if j%10 == 9 {
					table.Remove(id, sessionbinding.ReasonError)
				}
			}
		}(i)
	}
	wg.Wait()
}
