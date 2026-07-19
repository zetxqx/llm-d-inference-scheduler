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

// Package sessioncontrol provides a filter that pins a session's turns to
// the endpoint recorded in the session binding table, published as the
// SessionBinding request attribute by the session-binding-tracker.
package sessioncontrol

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrsession "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/session"
)

const (
	// SessionControlFilterType is the type of the SessionControl filter.
	SessionControlFilterType = "session-control-filter"
)

var (
	_ scheduling.Filter     = &SessionControl{}
	_ plugin.ConsumerPlugin = &SessionControl{}
)

// Factory defines the factory function for the SessionControl filter.
func Factory(name string, rawParameters *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	if rawParameters != nil {
		params := struct{}{}
		if err := rawParameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' filter - %w", SessionControlFilterType, err)
		}
	}
	return NewSessionControl(name), nil
}

// NewSessionControl returns a SessionControl filter.
func NewSessionControl(name string) *SessionControl {
	return &SessionControl{
		typedName: plugin.TypedName{Type: SessionControlFilterType, Name: name},
	}
}

// SessionControl narrows the candidate set to the endpoint the request's
// session is bound to. Requests without a binding, and bound requests whose
// endpoint is no longer among the candidates, pass all candidates through so
// downstream scorers place them fresh and the tracker rebinds.
type SessionControl struct {
	typedName plugin.TypedName
}

// TypedName returns the typed name of the plugin.
func (s *SessionControl) TypedName() plugin.TypedName {
	return s.typedName
}

// Filter returns the bound endpoint when it is among the candidates,
// otherwise all candidate endpoints.
func (s *SessionControl) Filter(_ context.Context, request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) []scheduling.Endpoint {
	binding, ok := attrsession.ReadSessionBinding(request)
	if !ok {
		return endpoints
	}
	for _, endpoint := range endpoints {
		if endpoint.GetMetadata() != nil && endpoint.GetMetadata().NamespacedName == binding.Endpoint {
			return []scheduling.Endpoint{endpoint}
		}
	}
	return endpoints
}

// Consumes declares SessionBinding as required so the data-layer DAG orders
// a session-binding-tracker ahead of scheduling and auto-creates one when
// none is configured.
func (s *SessionControl) Consumes() plugin.DataDependencies {
	return plugin.DataDependencies{
		Required: map[plugin.DataKey]any{
			attrsession.SessionBindingDataKey: attrsession.SessionBinding{},
		},
	}
}
