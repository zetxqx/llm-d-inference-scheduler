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

package sessionbinding

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrsession "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/session"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
	sessionbindingconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionbinding/constants"
)

// SessionBindingTrackerType is the plugin type registered with the framework.
const SessionBindingTrackerType = sessionbindingconstants.SessionBindingTrackerType

const (
	defaultTTL      = 15 * time.Minute
	maxDumpBindings = 1000
)

// Parameters configures the session-binding tracker.
type Parameters struct {
	// TTL is the idle lifetime of a binding as a Go duration string
	// (default "15m"). A binding not refreshed by any request within TTL is
	// dropped; it should sit above the backend's own session idle handling
	// so router and engine state converge.
	TTL string `json:"ttl"`
	// MaxSessions bounds the binding table (default 0, unbounded). Session
	// identifiers are client-supplied; deployments exposed to untrusted
	// clients should set a bound.
	MaxSessions int `json:"maxSessions"`
}

var (
	_ requestcontrol.DataProducer = &Tracker{}
	_ requestcontrol.PreRequest   = &Tracker{}
	_ datalayer.EndpointExtractor = (*Tracker)(nil)
	_ datalayer.Registrant        = &Tracker{}
	_ fwkplugin.ConsumerPlugin    = &Tracker{}
	_ fwkplugin.StateDumper       = &Tracker{}
)

// Tracker owns the session binding table: it publishes the SessionBinding
// attribute for scheduling plugins, records bindings after the picker
// selects an endpoint, and drops bindings when endpoints leave the pool.
type Tracker struct {
	typedName fwkplugin.TypedName
	bindingDK fwkplugin.DataKey
	table     *Table
}

// Factory builds a Tracker from raw plugin parameters.
func Factory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if handle == nil {
		return nil, fmt.Errorf("'%s' requires a plugin handle", SessionBindingTrackerType)
	}

	params := Parameters{}
	if rawParameters != nil {
		if err := rawParameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' producer: %w", SessionBindingTrackerType, err)
		}
	}

	ttl := defaultTTL
	if params.TTL != "" {
		parsed, err := time.ParseDuration(params.TTL)
		if err != nil {
			return nil, fmt.Errorf("'%s' has invalid ttl %q: %w", SessionBindingTrackerType, params.TTL, err)
		}
		ttl = parsed
	}
	if params.MaxSessions < 0 {
		return nil, fmt.Errorf("'%s' requires a non-negative maxSessions, got %d", SessionBindingTrackerType, params.MaxSessions)
	}

	if err := registerMetrics(handle.Metrics()); err != nil {
		return nil, err
	}

	typedName := fwkplugin.TypedName{Type: SessionBindingTrackerType, Name: name}
	tracker := &Tracker{
		typedName: typedName,
		bindingDK: attrsession.SessionBindingDataKey.WithNonEmptyProducerName(name),
	}
	tracker.table = NewTable(TableConfig{
		TTL:         ttl,
		MaxSessions: params.MaxSessions,
		OnRemove: func(_ string, _ k8stypes.NamespacedName, reason Reason) {
			recordInvalidation(typedName.Name, typedName.Type, reason)
		},
	})
	tracker.table.Start(handle.Context())
	return tracker, nil
}

// TypedName returns the type and name of the plugin.
func (t *Tracker) TypedName() fwkplugin.TypedName {
	return t.typedName
}

// Produce publishes the SessionBinding attribute when the request carries a
// session identifier that is bound to an endpoint. Unbound or session-less
// requests get no attribute; schedulers treat absence as "no placement
// constraint".
func (t *Tracker) Produce(_ context.Context, request *fwksched.InferenceRequest, _ []fwksched.Endpoint) error {
	if request == nil {
		return nil
	}
	sessionID, ok := attrsession.ReadSessionID(request)
	if !ok {
		return nil
	}
	endpoint, ok := t.table.Lookup(string(sessionID))
	if !ok {
		return nil
	}
	request.PutAttribute(t.bindingDK.String(), attrsession.SessionBinding{Endpoint: endpoint})
	return nil
}

// Produces declares the SessionBinding attribute key written by this producer.
func (t *Tracker) Produces() map[fwkplugin.DataKey]any {
	return map[fwkplugin.DataKey]any{t.bindingDK: attrsession.SessionBinding{}}
}

// Consumes declares SessionID as required so the data-layer DAG orders a
// session-id producer ahead of this one and auto-creates it when none is
// configured.
func (t *Tracker) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Required: map[fwkplugin.DataKey]any{
			attrsession.SessionIDDataKey: attrsession.SessionID(""),
		},
	}
}

// PreRequest records or refreshes the binding after the picker selects an
// endpoint. A first turn creates the binding; later turns refresh it; a turn
// landing on a different endpoint moves it.
func (t *Tracker) PreRequest(ctx context.Context, request *fwksched.InferenceRequest, result *fwksched.SchedulingResult) {
	if request == nil || result == nil {
		return
	}
	sessionID, ok := attrsession.ReadSessionID(request)
	if !ok {
		return
	}
	profileResult := result.ProfileResults[result.PrimaryProfileName]
	if profileResult == nil || len(profileResult.TargetEndpoints) == 0 {
		return
	}
	metadata := profileResult.TargetEndpoints[0].GetMetadata()
	if metadata == nil {
		return
	}

	if !t.table.Bind(string(sessionID), metadata.NamespacedName) {
		recordBindRejection(t.typedName.Name, t.typedName.Type)
		log.FromContext(ctx).V(logutil.DEFAULT).Info("Session binding table full; session left unbound",
			"sessionID", string(sessionID), "endpoint", metadata.NamespacedName.String())
		return
	}
	recordBindings(t.typedName.Name, t.typedName.Type, t.table.Len())
}

// RegisterDependencies declares that this plugin needs an
// endpoint-notification-source to observe endpoint deletions. The source is
// auto-created if not already in the config.
func (t *Tracker) RegisterDependencies(r datalayer.Registrar) error {
	return r.Register(datalayer.PendingRegistration{
		Owner:         t.TypedName(),
		SourceType:    sourcenotifications.EndpointNotificationSourceType,
		Extractor:     t,
		DefaultSource: sourcenotifications.NewEndpointDataSource(sourcenotifications.EndpointNotificationSourceType, sourcenotifications.EndpointNotificationSourceType),
	})
}

// Extract drops all bindings for an endpoint when it leaves the pool. The
// next turn of an affected session schedules fresh and rebinds.
func (t *Tracker) Extract(ctx context.Context, event datalayer.EndpointEvent) error {
	if event.Type != datalayer.EventDelete || event.Endpoint == nil || event.Endpoint.GetMetadata() == nil {
		return nil
	}
	endpoint := event.Endpoint.GetMetadata().NamespacedName
	removed := t.table.RemoveEndpoint(endpoint, ReasonPodDelete)
	if removed > 0 {
		recordBindings(t.typedName.Name, t.typedName.Type, t.table.Len())
		log.FromContext(ctx).V(logutil.DEFAULT).Info("Dropped session bindings for deleted endpoint",
			"endpoint", endpoint.String(), "bindings", removed)
	}
	return nil
}

// RemoveSession deletes the binding for sessionID, reporting the endpoint it
// was bound to. It backs session-close handling and error invalidation.
func (t *Tracker) RemoveSession(sessionID string, reason Reason) (k8stypes.NamespacedName, bool) {
	endpoint, ok := t.table.Remove(sessionID, reason)
	if ok {
		recordBindings(t.typedName.Name, t.typedName.Type, t.table.Len())
	}
	return endpoint, ok
}

// LookupSession returns the endpoint bound to sessionID.
func (t *Tracker) LookupSession(sessionID string) (k8stypes.NamespacedName, bool) {
	return t.table.Lookup(sessionID)
}

// ActiveSessions returns the number of bindings pinned to endpoint.
func (t *Tracker) ActiveSessions(endpoint k8stypes.NamespacedName) int {
	return t.table.ActiveSessions(endpoint)
}

type dumpState struct {
	Bindings  []BindingInfo `json:"bindings"`
	Total     int           `json:"total"`
	Truncated bool          `json:"truncated"`
}

// DumpState returns a JSON snapshot of the binding table for debugging.
func (t *Tracker) DumpState() (json.RawMessage, error) {
	bindings := t.table.Snapshot()
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].SessionID < bindings[j].SessionID })
	state := dumpState{Bindings: bindings, Total: len(bindings)}
	if len(state.Bindings) > maxDumpBindings {
		state.Bindings = state.Bindings[:maxDumpBindings]
		state.Truncated = true
	}
	return json.Marshal(state)
}
