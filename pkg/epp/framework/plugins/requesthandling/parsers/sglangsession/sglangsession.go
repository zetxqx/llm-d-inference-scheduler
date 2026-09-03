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

// Package sglangsession provides a parser for SGLang's out-of-band session
// lifecycle endpoint /close_session, making it schedulable so session-aware
// plugins can route a close to the endpoint holding the session and clean
// up router state.
package sglangsession

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
)

const (
	// SGLangSessionParserType is the type of the SGLang session parser.
	SGLangSessionParserType = "sglang-session-parser"

	// CloseSessionPath is the path suffix claimed for session close.
	CloseSessionPath = "close_session"

	// SessionIDField is the body field carrying the session identifier.
	SessionIDField = "session_id"
)

var _ fwkrh.Parser = &Parser{}

// Parser claims SGLang's session lifecycle paths and parses their JSON
// bodies into a PayloadMap, so the session-id producer can extract the
// session identifier with its body-field source. It performs no
// normalization; the body is forwarded as received.
type Parser struct {
	typedName fwkplugin.TypedName
}

// Factory defines the factory function for the SGLang session parser.
func Factory(name string, _ *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	return &Parser{typedName: fwkplugin.TypedName{Type: SGLangSessionParserType, Name: name}}, nil
}

// TypedName returns the typed name of the plugin.
func (p *Parser) TypedName() fwkplugin.TypedName {
	return p.typedName
}

// ParseRequest parses the session request body. A close without a string
// session_id is rejected early: no backend could act on it.
func (p *Parser) ParseRequest(_ context.Context, body []byte, _ map[string]string) (*fwkrh.ParseResult, error) {
	payload := fwkrh.PayloadMap{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("invalid session request body: %w", err)
	}
	sessionID, _ := payload[SessionIDField].(string)
	if sessionID == "" {
		return nil, fmt.Errorf("session request body requires a string %q field", SessionIDField)
	}
	return &fwkrh.ParseResult{
		Body:                   &fwkrh.InferenceRequestBody{Payload: payload},
		SkipResponseProcessing: true,
	}, nil
}

// ParseResponse is not used: session lifecycle responses carry no usage and
// response processing is skipped at parse time.
func (p *Parser) ParseResponse(_ context.Context, _ []byte, _ map[string]string, _ bool) (*fwkrh.ParsedResponse, error) {
	return nil, nil //nolint:nilnil // no response parsing for session lifecycle endpoints
}

// Claims returns the paths and protocols claimed by this parser.
func (p *Parser) Claims() fwkrh.Claims {
	return fwkrh.Claims{
		Paths:     []string{CloseSessionPath},
		Protocols: []v1.AppProtocol{v1.AppProtocolH2C, v1.AppProtocolHTTP},
	}
}
