# Session Control Scorer Plugin

**Type:** `session-control-scorer`

Scores the endpoint the request's session is bound to at 1.0 and all other candidates at 0.0, using the `SessionBinding` attribute published by the [session-binding-tracker](../../../requestcontrol/dataproducer/sessionbinding/README.md). Category: `Affinity`.

This is the soft pinning mode: the binding composes with load and prefix scorers through profile weights, so the pin breaks whenever the bound endpoint's weighted disadvantage on other scorers exceeds this scorer's weight. Profile weights are therefore the session move policy. When a turn lands on a different endpoint, the tracker rebinds the session there and subsequent turns follow; with a radix-native session backend the move costs one re-prefill.

The scorer declares the `SessionBinding` attribute as a required dependency, so a `session-binding-tracker` (and transitively a `session-id-producer`) is auto-created when none is configured.

## Parameters

None.

### Example Configuration

```yaml
plugins:
  - type: session-id-producer
    parameters:
      bodyField: session_id
  - type: session-binding-tracker
  - type: session-control-scorer
  - type: prefix-cache-scorer
  - type: kv-cache-utilization-scorer
  - type: max-score-picker
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: session-control-scorer
        weight: 3
      - pluginRef: prefix-cache-scorer
        weight: 2
      - pluginRef: kv-cache-utilization-scorer
        weight: 1
      - pluginRef: max-score-picker
```

## Relationship to the session control filter

The [session-control-filter](../../filter/sessioncontrol/README.md) is the strict alternative: it narrows the candidate set to the bound endpoint, so the pin never breaks while the endpoint is alive. Use the filter for deterministic pinning, the scorer to let weights arbitrate pin versus move.
