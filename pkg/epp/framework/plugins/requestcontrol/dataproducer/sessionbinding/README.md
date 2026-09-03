# Session Binding Tracker Plugin

**Type:** `session-binding-tracker`

Owns the router-side session binding table: a mapping from session identifier to the endpoint holding that session's KV cache. On each request it publishes the `SessionBinding` attribute (read via `session.ReadSessionBinding(request)`) so affinity plugins can pin the session's turns to its endpoint; after the picker selects an endpoint it records or refreshes the binding; when an endpoint leaves the pool it drops that endpoint's bindings so affected sessions schedule fresh.

The tracker consumes the `SessionID` attribute and declares it as a required dependency, so a `session-id-producer` is ordered ahead of it (and auto-created when none is configured).

Bindings are in-memory and EPP-local. A router restart loses them; with a radix-native session backend (SGLang `--enable-session-radix-cache`) the next turn of each session re-pins at the cost of one re-prefill.

## Session close

A request whose path ends in `/close_session` (made schedulable by the [sglang-session-parser](../../../requesthandling/parsers/sglangsession/sglangsession.go)) is treated as a client-initiated session close:

- With a binding: the affinity plugins pin the close to the bound endpoint like any turn; the tracker then removes the binding. The binding is removed regardless of the backend result — with a radix-native backend a failed close only defers KV reclamation to cache eviction.
- Without a binding (router restarted, binding idled out, unknown ID): the tracker broadcasts the close best-effort, fire-and-forget, to every known endpoint except the one the request was scheduled to (which receives it via the proxy). A close for an unknown session is a backend no-op, so the broadcast is safe and preserves deterministic KV reclamation across router restarts.

The gateway `HTTPRoute` must route `/close_session` to the InferencePool for closes to reach the router at all.

## Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `ttl` | duration string | `15m` | Idle lifetime of a binding; a binding not refreshed by any request within `ttl` is dropped. Set it above the backend's own session idle handling so router and engine state converge. |
| `maxSessions` | int | `0` (unbounded) | Bound on concurrent bindings. Session identifiers are client-supplied; deployments exposed to untrusted clients should set a bound. New sessions beyond the bound stay unbound (scheduled without affinity), never rejected as requests. |

## Metrics

- `llm_d_epp_session_control_bindings{plugin_name, plugin_type}`: bindings currently held.
- `llm_d_epp_session_control_invalidations_total{plugin_name, plugin_type, reason}`: bindings removed, by reason (`idle`, `pod_delete`, `close`, `error`).
- `llm_d_epp_session_control_bind_rejections_total{plugin_name, plugin_type}`: new sessions rejected at the `maxSessions` bound.

## Example

```yaml
plugins:
  - type: session-id-producer
    parameters:
      bodyField: session_id
  - type: session-binding-tracker
    parameters:
      ttl: 15m
      maxSessions: 10000
```

## Related Documentation

- [Session Attributes](../../../datalayer/attribute/session/README.md)
- [Session ID Producer](../sessionid/README.md)
