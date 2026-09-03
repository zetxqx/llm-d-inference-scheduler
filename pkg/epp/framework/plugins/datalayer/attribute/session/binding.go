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

package session

import (
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	sessionbindingconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionbinding/constants"
)

// SessionBindingDataKey identifies the session-to-endpoint binding published
// on the request attribute store. The default producer is the
// session-binding-tracker.
var SessionBindingDataKey = plugin.NewDataKey("SessionBindingDataKey", sessionbindingconstants.SessionBindingTrackerType)

// SessionBinding is the endpoint the request's session is pinned to.
type SessionBinding struct {
	Endpoint k8stypes.NamespacedName
}

// ReadSessionBinding returns the SessionBinding published by the default
// producer on the request attribute store, or the zero value and false if
// absent. Absence means the session is unbound (first turn, or no session);
// schedulers must treat it as "no placement constraint".
func ReadSessionBinding(r *fwksched.InferenceRequest) (SessionBinding, bool) {
	key := SessionBindingDataKey.WithNonEmptyProducerName(sessionbindingconstants.SessionBindingTrackerType)
	return fwksched.ReadRequestAttribute[SessionBinding](r, key)
}
