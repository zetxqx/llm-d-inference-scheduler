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

package sessioncontrol_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrsession "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/session"
	sessionbindingconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionbinding/constants"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/sessioncontrol"
)

var (
	podA = k8stypes.NamespacedName{Namespace: "ns", Name: "pod-a"}
	podB = k8stypes.NamespacedName{Namespace: "ns", Name: "pod-b"}
)

func endpointFor(name k8stypes.NamespacedName) scheduling.Endpoint {
	return scheduling.NewEndpoint(&fwkdl.EndpointMetadata{ID: name}, &fwkdl.Metrics{}, nil)
}

func requestBoundTo(endpoint k8stypes.NamespacedName) *scheduling.InferenceRequest {
	req := &scheduling.InferenceRequest{}
	key := attrsession.SessionBindingDataKey.WithNonEmptyProducerName(sessionbindingconstants.SessionBindingTrackerType)
	req.PutAttribute(key, attrsession.SessionBinding{Endpoint: endpoint})
	return req
}

func TestScore(t *testing.T) {
	t.Parallel()

	epA := endpointFor(podA)
	epB := endpointFor(podB)
	scorer := sessioncontrol.NewSessionControl("test")

	tests := []struct {
		name      string
		request   *scheduling.InferenceRequest
		endpoints []scheduling.Endpoint
		want      map[scheduling.Endpoint]float64
	}{
		{
			name:      "no binding scores all zero",
			request:   &scheduling.InferenceRequest{},
			endpoints: []scheduling.Endpoint{epA, epB},
			want:      map[scheduling.Endpoint]float64{epA: 0.0, epB: 0.0},
		},
		{
			name:      "bound endpoint scores one",
			request:   requestBoundTo(podA),
			endpoints: []scheduling.Endpoint{epA, epB},
			want:      map[scheduling.Endpoint]float64{epA: 1.0, epB: 0.0},
		},
		{
			name:      "bound endpoint absent scores all zero",
			request:   requestBoundTo(k8stypes.NamespacedName{Namespace: "ns", Name: "gone"}),
			endpoints: []scheduling.Endpoint{epA, epB},
			want:      map[scheduling.Endpoint]float64{epA: 0.0, epB: 0.0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := scorer.Score(context.Background(), tc.request, tc.endpoints)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCategory(t *testing.T) {
	t.Parallel()
	assert.Equal(t, scheduling.Affinity, sessioncontrol.NewSessionControl("test").Category())
}

func TestConsumesSessionBinding(t *testing.T) {
	t.Parallel()
	scorer := sessioncontrol.NewSessionControl("test")
	_, ok := scorer.Consumes().Required[attrsession.SessionBindingDataKey]
	assert.True(t, ok, "Consumes() must require the SessionBinding attribute")
}
