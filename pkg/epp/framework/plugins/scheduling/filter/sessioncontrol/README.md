# Session Control Filter Plugin

**Type:** `session-control-filter`

Narrows the candidate set to the endpoint the request's session is bound to, using the `SessionBinding` attribute published by the [session-binding-tracker](../../../requestcontrol/dataproducer/sessionbinding/README.md). This is the strict pinning mode: while the bound endpoint is alive and among the candidates, every turn of the session lands on it regardless of load.

Requests without a binding (first turn, or no session identifier) and bound requests whose endpoint is no longer among the candidates pass all candidates through, so downstream scorers place them fresh and the tracker rebinds. With a radix-native session backend (SGLang `--enable-session-radix-cache`) that fallback costs one re-prefill, never an error.

The filter declares the `SessionBinding` attribute as a required dependency, so a `session-binding-tracker` (and transitively a `session-id-producer`) is auto-created when none is configured.

## Parameters

None.

### Example Configuration

```yaml
plugins:
  - type: session-id-producer
    parameters:
      bodyField: session_id
  - type: session-binding-tracker
  - type: session-control-filter
  - type: prefix-cache-scorer
  - type: max-score-picker
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: session-control-filter
      - pluginRef: prefix-cache-scorer
      - pluginRef: max-score-picker
```

## Relationship to the session control scorer

The [session-control-scorer](../../scorer/sessioncontrol/README.md) is the soft alternative: the bound endpoint gets the maximum affinity score but composes with load and prefix scorers through profile weights, so a sufficiently loaded endpoint can lose the request (moving the session at the cost of a re-prefill). Use the filter for deterministic pinning, the scorer to let weights arbitrate pin versus move. The stateless [session-affinity-filter](../sessionaffinity/README.md) differs from both: it echoes the pod in a client-held token and keeps no router state.
