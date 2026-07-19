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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	commonrequest "github.com/llm-d/llm-d-router/pkg/epp/framework/common/request"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrsession "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/session"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/sglangsession"
)

const closeRequestTimeout = 2 * time.Second

func isCloseRequest(request *fwksched.InferenceRequest) bool {
	if request.Headers == nil {
		return false
	}
	return commonrequest.MatchPathSuffix(commonrequest.GetRequestPath(request.Headers), sglangsession.CloseSessionPath)
}

// handleClose removes the binding for a client-initiated session close. The
// close request itself is forwarded to its scheduled endpoint by the normal
// request path (pinned to the bound endpoint by the affinity plugins, since
// the binding is removed only after scheduling). When no binding exists —
// the router restarted, the binding idled out, or the ID is unknown — the
// close is broadcast best-effort to every other known endpoint, so the
// session's KV is freed wherever it lives. Broadcasting is safe because a
// close for an unknown session is a backend no-op.
func (t *Tracker) handleClose(ctx context.Context, request *fwksched.InferenceRequest, result *fwksched.SchedulingResult) {
	sessionID, ok := attrsession.ReadSessionID(request)
	if !ok {
		return
	}
	logger := log.FromContext(ctx)
	if endpoint, removed := t.RemoveSession(string(sessionID), ReasonClose); removed {
		logger.V(logutil.DEBUG).Info("Session close routed to bound endpoint",
			"sessionID", string(sessionID), "endpoint", endpoint.String())
		return
	}
	t.broadcastClose(logger, string(sessionID), scheduledEndpoint(result))
}

func scheduledEndpoint(result *fwksched.SchedulingResult) k8stypes.NamespacedName {
	if result == nil {
		return k8stypes.NamespacedName{}
	}
	profileResult := result.ProfileResults[result.PrimaryProfileName]
	if profileResult == nil || len(profileResult.TargetEndpoints) == 0 || profileResult.TargetEndpoints[0].GetMetadata() == nil {
		return k8stypes.NamespacedName{}
	}
	return profileResult.TargetEndpoints[0].GetMetadata().NamespacedName
}

// broadcastClose fires a best-effort close at every known endpoint except
// exclude (which receives the original request via the proxy). Failures are
// logged and dropped: with a radix-native session backend a missed close
// only defers reclamation to cache eviction.
func (t *Tracker) broadcastClose(logger logr.Logger, sessionID string, exclude k8stypes.NamespacedName) {
	t.addrMu.RLock()
	targets := make(map[k8stypes.NamespacedName]string, len(t.addresses))
	for name, baseURL := range t.addresses {
		if name != exclude {
			targets[name] = baseURL
		}
	}
	t.addrMu.RUnlock()
	if len(targets) == 0 {
		return
	}

	recordCloseBroadcast(t.typedName.Name, t.typedName.Type)
	logger.V(logutil.DEFAULT).Info("Broadcasting session close for unbound session",
		"sessionID", sessionID, "endpoints", len(targets))

	body, err := json.Marshal(map[string]string{sglangsession.SessionIDField: sessionID})
	if err != nil {
		return
	}
	for name, baseURL := range targets {
		go func(endpoint, url string) {
			ctx, cancel := context.WithTimeout(t.parentCtx, closeRequestTimeout)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/"+sglangsession.CloseSessionPath, bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := t.httpClient.Do(req)
			if err != nil {
				logger.V(logutil.DEBUG).Info("Session close broadcast failed",
					"sessionID", sessionID, "endpoint", endpoint, "error", err.Error())
				return
			}
			_ = resp.Body.Close()
		}(name.String(), baseURL)
	}
}
