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

// Package sessionbinding provides the router-owned binding table mapping a
// session identifier to the endpoint holding that session's KV cache, and
// the session-binding-tracker plugin that maintains it.
package sessionbinding

import (
	"context"
	"sync"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"
)

// Reason classifies why a binding was removed from the table.
type Reason string

const (
	// ReasonIdle marks bindings dropped because no request refreshed them
	// within the table TTL.
	ReasonIdle Reason = "idle"
	// ReasonPodDelete marks bindings dropped because their endpoint left the
	// inference pool.
	ReasonPodDelete Reason = "pod_delete"
	// ReasonClose marks bindings removed by a client-initiated session close.
	ReasonClose Reason = "close"
	// ReasonError marks bindings invalidated after a backend session error.
	ReasonError Reason = "error"
)

const defaultSweepInterval = time.Minute

// TableConfig configures a binding Table.
type TableConfig struct {
	// TTL is the idle lifetime of a binding; a binding not refreshed within
	// TTL is removed by the sweeper. Zero disables idle expiry.
	TTL time.Duration
	// MaxSessions bounds the number of concurrent bindings; Bind rejects new
	// sessions beyond it. Session identifiers are client-supplied, so the
	// bound is the table's protection against cardinality abuse. Zero means
	// unbounded.
	MaxSessions int
	// SweepInterval is how often the sweeper scans for idle bindings.
	// Defaults to one minute when zero.
	SweepInterval time.Duration
	// OnRemove, when non-nil, is invoked for every binding removed from the
	// table. It is called with the table lock held and must not call back
	// into the Table.
	OnRemove func(sessionID string, endpoint k8stypes.NamespacedName, reason Reason)
}

type entry struct {
	endpoint k8stypes.NamespacedName
	lastSeen time.Time
}

// BindingInfo is a point-in-time view of one binding, for state dumps.
type BindingInfo struct {
	SessionID string                  `json:"sessionID"`
	Endpoint  k8stypes.NamespacedName `json:"endpoint"`
	LastSeen  time.Time               `json:"lastSeen"`
}

// Table maps session identifiers to the endpoint their turns are pinned to,
// with idle expiry, a capacity bound, and a per-endpoint index for session
// counting and pod-deletion cleanup. All methods are safe for concurrent
// use.
//
// The table owns its synchronization rather than wrapping a TTL cache:
// cache libraries run eviction callbacks under their internal lock, which
// deadlocks against a second lock guarding the per-endpoint index.
type Table struct {
	mu       sync.RWMutex
	sessions map[string]*entry
	perPod   map[k8stypes.NamespacedName]map[string]struct{}

	ttl           time.Duration
	maxSessions   int
	sweepInterval time.Duration
	onRemove      func(sessionID string, endpoint k8stypes.NamespacedName, reason Reason)
}

// NewTable builds a Table from cfg. Call Start to enable idle expiry.
func NewTable(cfg TableConfig) *Table {
	sweep := cfg.SweepInterval
	if sweep <= 0 {
		sweep = defaultSweepInterval
	}
	return &Table{
		sessions:      make(map[string]*entry),
		perPod:        make(map[k8stypes.NamespacedName]map[string]struct{}),
		ttl:           cfg.TTL,
		maxSessions:   cfg.MaxSessions,
		sweepInterval: sweep,
		onRemove:      cfg.OnRemove,
	}
}

// Start launches the idle sweeper; it stops when ctx is done. Start is a
// no-op when the table has no TTL.
func (t *Table) Start(ctx context.Context) {
	if t.ttl <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(t.sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				t.sweep(now)
			}
		}
	}()
}

// Bind records or refreshes the binding for sessionID. A binding to a
// different endpoint is moved (the session followed the picker to a new
// pod). Bind returns false only when the session is new and the table is at
// its MaxSessions bound.
func (t *Table) Bind(sessionID string, endpoint k8stypes.NamespacedName) bool {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	if e, ok := t.sessions[sessionID]; ok {
		if e.endpoint != endpoint {
			t.unindex(sessionID, e.endpoint)
			t.index(sessionID, endpoint)
			e.endpoint = endpoint
		}
		e.lastSeen = now
		return true
	}
	if t.maxSessions > 0 && len(t.sessions) >= t.maxSessions {
		return false
	}
	t.sessions[sessionID] = &entry{endpoint: endpoint, lastSeen: now}
	t.index(sessionID, endpoint)
	return true
}

// Lookup returns the endpoint bound to sessionID. It does not refresh the
// binding; Bind does.
func (t *Table) Lookup(sessionID string) (k8stypes.NamespacedName, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	e, ok := t.sessions[sessionID]
	if !ok {
		return k8stypes.NamespacedName{}, false
	}
	return e.endpoint, true
}

// Remove deletes the binding for sessionID, reporting the endpoint it was
// bound to.
func (t *Table) Remove(sessionID string, reason Reason) (k8stypes.NamespacedName, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.sessions[sessionID]
	if !ok {
		return k8stypes.NamespacedName{}, false
	}
	t.remove(sessionID, e.endpoint, reason)
	return e.endpoint, true
}

// RemoveEndpoint deletes every binding pinned to endpoint, returning the
// number removed.
func (t *Table) RemoveEndpoint(endpoint k8stypes.NamespacedName, reason Reason) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	removed := 0
	for sessionID := range t.perPod[endpoint] {
		t.remove(sessionID, endpoint, reason)
		removed++
	}
	return removed
}

// ActiveSessions returns the number of bindings pinned to endpoint.
func (t *Table) ActiveSessions(endpoint k8stypes.NamespacedName) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.perPod[endpoint])
}

// Len returns the number of bindings in the table.
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.sessions)
}

// Snapshot returns a copy of all bindings, for state dumps.
func (t *Table) Snapshot() []BindingInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]BindingInfo, 0, len(t.sessions))
	for sessionID, e := range t.sessions {
		out = append(out, BindingInfo{SessionID: sessionID, Endpoint: e.endpoint, LastSeen: e.lastSeen})
	}
	return out
}

func (t *Table) sweep(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for sessionID, e := range t.sessions {
		if now.Sub(e.lastSeen) > t.ttl {
			t.remove(sessionID, e.endpoint, ReasonIdle)
		}
	}
}

// remove must be called with t.mu held.
func (t *Table) remove(sessionID string, endpoint k8stypes.NamespacedName, reason Reason) {
	delete(t.sessions, sessionID)
	t.unindex(sessionID, endpoint)
	if t.onRemove != nil {
		t.onRemove(sessionID, endpoint, reason)
	}
}

// index must be called with t.mu held.
func (t *Table) index(sessionID string, endpoint k8stypes.NamespacedName) {
	set, ok := t.perPod[endpoint]
	if !ok {
		set = make(map[string]struct{})
		t.perPod[endpoint] = set
	}
	set[sessionID] = struct{}{}
}

// unindex must be called with t.mu held.
func (t *Table) unindex(sessionID string, endpoint k8stypes.NamespacedName) {
	set, ok := t.perPod[endpoint]
	if !ok {
		return
	}
	delete(set, sessionID)
	if len(set) == 0 {
		delete(t.perPod, endpoint)
	}
}
