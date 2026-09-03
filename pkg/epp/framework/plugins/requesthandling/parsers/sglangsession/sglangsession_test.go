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

package sglangsession_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/sglangsession"
)

func mustParser(t *testing.T) *sglangsession.Parser {
	t.Helper()
	plg, err := sglangsession.Factory("sglang-session-parser", nil, nil)
	require.NoError(t, err)
	parser, ok := plg.(*sglangsession.Parser)
	require.True(t, ok, "factory must return *Parser")
	return parser
}

func TestParseRequest(t *testing.T) {
	t.Parallel()
	parser := mustParser(t)

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "valid close", body: `{"session_id":"subagent-42"}`},
		{name: "extra fields preserved", body: `{"session_id":"s1","reason":"done"}`},
		{name: "missing session_id", body: `{}`, wantErr: "requires a string"},
		{name: "session_id not a string", body: `{"session_id":42}`, wantErr: "requires a string"},
		{name: "invalid json", body: `not-json`, wantErr: "invalid session request body"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := parser.ParseRequest(context.Background(), []byte(tc.body), nil)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.True(t, result.SkipResponseProcessing, "session lifecycle responses need no processing")

			payload, ok := result.Body.Payload.AsMap()
			require.True(t, ok, "payload must be a parsed map for the session-id producer")
			_, ok = payload[sglangsession.SessionIDField].(string)
			assert.True(t, ok)
		})
	}
}

func TestClaims(t *testing.T) {
	t.Parallel()
	claims := mustParser(t).Claims()
	assert.Equal(t, []string{sglangsession.CloseSessionPath}, claims.Paths)
	assert.NotEmpty(t, claims.Protocols)
}

func TestParseResponse(t *testing.T) {
	t.Parallel()
	resp, err := mustParser(t).ParseResponse(context.Background(), []byte(`{}`), nil, true)
	require.NoError(t, err)
	assert.Nil(t, resp)
}
