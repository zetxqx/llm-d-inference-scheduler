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
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

var (
	sessionBindings = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "session_control_bindings",
			Help:      metricsutil.HelpMsgWithStability("Number of session bindings currently held by the binding table.", compbasemetrics.ALPHA),
		},
		[]string{"plugin_name", "plugin_type"},
	)

	sessionInvalidations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "session_control_invalidations_total",
			Help:      metricsutil.HelpMsgWithStability("Session bindings removed from the binding table, by reason.", compbasemetrics.ALPHA),
		},
		[]string{"plugin_name", "plugin_type", "reason"},
	)

	sessionBindRejections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "session_control_bind_rejections_total",
			Help:      metricsutil.HelpMsgWithStability("New sessions rejected because the binding table reached maxSessions.", compbasemetrics.ALPHA),
		},
		[]string{"plugin_name", "plugin_type"},
	)

	sessionCloseBroadcasts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "session_control_close_broadcasts_total",
			Help:      metricsutil.HelpMsgWithStability("Session closes broadcast to all endpoints because no binding was found.", compbasemetrics.ALPHA),
		},
		[]string{"plugin_name", "plugin_type"},
	)
)

func registerMetrics(registerer prometheus.Registerer) error {
	if registerer == nil {
		return errors.New("session binding metrics registerer is required")
	}
	for _, collector := range []prometheus.Collector{
		sessionBindings,
		sessionInvalidations,
		sessionBindRejections,
		sessionCloseBroadcasts,
	} {
		if err := registerer.Register(collector); err != nil {
			var alreadyRegistered prometheus.AlreadyRegisteredError
			if errors.As(err, &alreadyRegistered) && alreadyRegistered.ExistingCollector == collector {
				continue
			}
			return fmt.Errorf("register session binding metric: %w", err)
		}
	}
	return nil
}

func recordBindings(pluginName, pluginType string, count int) {
	sessionBindings.WithLabelValues(pluginName, pluginType).Set(float64(count))
}

func recordInvalidation(pluginName, pluginType string, reason Reason) {
	sessionInvalidations.WithLabelValues(pluginName, pluginType, string(reason)).Inc()
}

func recordBindRejection(pluginName, pluginType string) {
	sessionBindRejections.WithLabelValues(pluginName, pluginType).Inc()
}

func recordCloseBroadcast(pluginName, pluginType string) {
	sessionCloseBroadcasts.WithLabelValues(pluginName, pluginType).Inc()
}
