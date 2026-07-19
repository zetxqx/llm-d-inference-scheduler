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

// Package sessioncontrol provides a scorer that prefers the endpoint the
// request's session is bound to, composing with load and prefix scorers via
// profile weights instead of hard-pinning like the session-control-filter.
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
	// SessionControlScorerType is the type of the SessionControl scorer.
	SessionControlScorerType = "session-control-scorer"
)

var (
	_ scheduling.Scorer     = &SessionControl{}
	_ plugin.ConsumerPlugin = &SessionControl{}
)

// Factory defines the factory function for the SessionControl scorer.
func Factory(name string, rawParameters *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	if rawParameters != nil {
		params := struct{}{}
		if err := rawParameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' scorer - %w", SessionControlScorerType, err)
		}
	}
	return NewSessionControl(name), nil
}

// NewSessionControl returns a SessionControl scorer.
func NewSessionControl(name string) *SessionControl {
	return &SessionControl{
		typedName: plugin.TypedName{Type: SessionControlScorerType, Name: name},
	}
}

// SessionControl scores the endpoint the request's session is bound to at
// 1.0 and all others at 0.0. The pin breaks whenever the bound endpoint's
// weighted disadvantage on other scorers exceeds this scorer's weight, so
// profile weights are the session move policy; the session-control-filter
// is the strict alternative.
type SessionControl struct {
	typedName plugin.TypedName
}

// TypedName returns the typed name of the plugin.
func (s *SessionControl) TypedName() plugin.TypedName {
	return s.typedName
}

// Category returns the scorer category.
func (s *SessionControl) Category() scheduling.ScorerCategory {
	return scheduling.Affinity
}

// Score assigns 1.0 to the bound endpoint and 0.0 to every other candidate.
func (s *SessionControl) Score(_ context.Context, request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) map[scheduling.Endpoint]float64 {
	scores := make(map[scheduling.Endpoint]float64, len(endpoints))
	for _, endpoint := range endpoints {
		scores[endpoint] = 0.0
	}
	binding, ok := attrsession.ReadSessionBinding(request)
	if !ok {
		return scores
	}
	for _, endpoint := range endpoints {
		if endpoint.GetMetadata() != nil && endpoint.GetMetadata().NamespacedName == binding.Endpoint {
			scores[endpoint] = 1.0
			break
		}
	}
	return scores
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
