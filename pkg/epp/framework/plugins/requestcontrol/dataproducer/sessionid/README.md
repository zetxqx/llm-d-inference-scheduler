# Session ID Producer Plugin

**Type:** `session-id-producer`

Extracts a session identifier from each inference request and publishes it as the `SessionID` attribute on the `InferenceRequest` attribute store. Affinity-aware scorers and filters consume this attribute via `session.ReadSessionID(request)` without needing to know whether the session was carried in a header, a cookie, or the request body.

The producer is a no-op when the configured sources are absent or empty; consumers must treat the missing attribute as "no session preference".

## Parameters

At least one source must be set; `headerName` and `cookieName` are mutually exclusive:

- `bodyField`: name of a top-level string field in the parsed JSON request body (for example `session_id`, the field SGLang uses to tag session KV). Bodies that are not parsed into a map (raw passthrough or protobuf payloads) yield no identifier from this source.
- `headerName`: name of the request header whose value is the session identifier. Comparison is case-insensitive (header names in the request are lowercased).
- `cookieName`: name of the cookie within the standard `Cookie` request header whose value is the session identifier.

When `bodyField` is combined with a header or cookie source, the body value takes precedence and the header or cookie serves as the fallback. The fallback covers paths whose parser does not produce a parsed JSON body.

## Examples

```yaml
plugins:
  - type: session-id-producer
    parameters:
      headerName: x-session-id
```

```yaml
plugins:
  - type: session-id-producer
    parameters:
      cookieName: llm-d-session
```

```yaml
plugins:
  - type: session-id-producer
    parameters:
      bodyField: session_id
      headerName: x-session-id
```

## Related Documentation

- [Session Attributes](../../../datalayer/attribute/session/README.md)
