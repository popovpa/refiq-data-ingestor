# Data-ingestor click ratio audit

Read-only audit. No production code, schema, tracker, SDK, or config was changed.
Kafka broker was not used. Diagnostics ran through the existing mapper / strategy / writer-input path.

Final verdict: **B. DATA-INGESTOR CORRECT**

When the ingest pipeline receives flat canonical fields:

```json
"click_viewport_x_ratio": 0.7946,
"click_viewport_y_ratio": 0.0732,
"click_document_x_ratio": 0.7946,
"click_document_y_ratio": 0.5147
```

those values survive deserialize → map → enrichment → values handed to ClickHouse writer as `Float`:

`0.7946 / 0.0732 / 0.7946 / 0.5147`

`-1` observed in ClickHouse is therefore **not produced by a data-ingestor overwrite** of already-correct decimals. It is the missing/default sentinel that remains when the binder never sees the fields (or when upstream Kafka already carries `-1`).

## Input DTO

There is no separate Jackson DTO class for clickstream Kafka JSON.

Effective model:

| Layer | Class | How ratios are held |
|---|---|---|
| Parsed JSON | `tools.jackson.databind.JsonNode` via `ClickstreamValidator.parse` | raw numbers |
| Persistence row | `ClickstreamEventRecord` | `Object[] values` indexed by `ClickstreamColumn` ordinal |
| Column registry | `ClickstreamColumn` enum | descriptor + aliases + default |

`ClickstreamEventRecord` has **no** dedicated getters/setters for:

- `clickViewportXRatio`
- `clickViewportYRatio`
- `clickDocumentXRatio`
- `clickDocumentYRatio`
- `clickTargetX` / `clickTargetY`

Access is only generic:

```java
record.get(ClickstreamColumn.CLICK_VIEWPORT_X_RATIO)
record.set(ClickstreamColumn.CLICK_VIEWPORT_X_RATIO, (float) number)
```

That is enough for `ClickstreamRowBinary` / writer, which iterates all enum columns.

Column descriptors:

| Enum | CH column | Java after bind | Default | skipBrowserBind | serverOwned | jsonGroup | Aliases |
|---|---|---|---:|---:|---:|---|---|
| `CLICK_VIEWPORT_X_RATIO` | `click_viewport_x_ratio` | `Float` | `-1.0f` | `false` | `false` | `null` | `click_viewport_x_ratio`, `clickViewportXRatio` |
| `CLICK_VIEWPORT_Y_RATIO` | `click_viewport_y_ratio` | `Float` | `-1.0f` | `false` | `false` | `null` | `click_viewport_y_ratio`, `clickViewportYRatio` |
| `CLICK_DOCUMENT_X_RATIO` | `click_document_x_ratio` | `Float` | `-1.0f` | `false` | `false` | `null` | `click_document_x_ratio`, `clickDocumentXRatio` |
| `CLICK_DOCUMENT_Y_RATIO` | `click_document_y_ratio` | `Float` | `-1.0f` | `false` | `false` | `null` | `click_document_y_ratio`, `clickDocumentYRatio` |

Constructor signature used:

```text
(columnName, clickHouseType, defaultExpression, serverOwned, skipBrowserBind, jsonGroup, groupMember, aliases...)
```

## JSON deserialization

Path:

```text
byte[]
→ ClickstreamValidator.parse → JsonNode
→ ClickstreamMapper.mapEvent
→ ClickstreamJsonBinder.bind
→ ClickstreamEventRecord.set(column, (float) double)
```

Floating branch in `ClickstreamJsonBinder.apply`:

1. require `value.isNumber()`
2. `double number = value.doubleValue()`
3. reject non-finite
4. `record.set(column, (float) number)`

No long/int cast for these fields.

Diagnostic input (minimal valid event; bare `{event_type, ratios}` alone fails site_key validation):

```json
{
  "schemaVersion": 1,
  "event_type": "click",
  "site_key": "fpxkp",
  "client_event_id": 100,
  "occurred_ts": 1710000000000,
  "click_viewport_x_ratio": 0.7946,
  "click_viewport_y_ratio": 0.0732,
  "click_document_x_ratio": 0.7946,
  "click_document_y_ratio": 0.5147
}
```

After map:

```text
vx=0.7946 vy=0.0732 dx=0.7946 dy=0.5147 types=Float/Float/Float/Float
```

## Field types

| Stage | Type |
|---|---|
| Kafka JSON number | Jackson numeric (`JsonNode.isNumber`) |
| After binder | Java `Float` in `ClickstreamEventRecord` |
| RowBinary wire | `Float` via `ClickstreamRowBinary.wire` (`floating()` → float) |
| ClickHouse column | `Float32 DEFAULT -1` |

No `Double → long` / `String → long` path for these four columns.

`double → float` narrowing is present (`(float) number`). For `0.7946` this is expected Float32 quantization, not `-1`.

## ClickHouse schema

From `src/main/resources/clickhouse/clickstream_events.sql` and additive DDL:

```sql
click_viewport_x_ratio Float32 DEFAULT -1,
click_viewport_y_ratio Float32 DEFAULT -1,
click_document_x_ratio Float32 DEFAULT -1,
click_document_y_ratio Float32 DEFAULT -1,
```

Types are float-compatible. Not `Int*`.

## Field mapping

Name mapping is consistent:

| Kafka / JSON | Enum alias | CH column |
|---|---|---|
| `click_viewport_x_ratio` / `clickViewportXRatio` | yes | `click_viewport_x_ratio` |
| `click_viewport_y_ratio` / `clickViewportYRatio` | yes | `click_viewport_y_ratio` |
| `click_document_x_ratio` / `clickDocumentXRatio` | yes | `click_document_x_ratio` |
| `click_document_y_ratio` / `clickDocumentYRatio` | yes | `click_document_y_ratio` |

Important binder detail: `jsonGroup = null` for all four.

Unlike older click fields such as:

```text
CLICK_CLIENT_X(..., jsonGroup="click", groupMember="client_x", aliases flat...)
CLICK_RELATIVE_X(..., jsonGroup="click", groupMember="relative_x", ...)
```

the ratio columns bind **only from top-level flat keys**.

Diagnostic counter-example:

```json
"click": {
  "viewport_x_ratio": 0.7946,
  "viewport_y_ratio": 0.0732,
  "document_x_ratio": 0.7946,
  "document_y_ratio": 0.5147
}
```

Result after map:

```text
vx=-1.0 vy=-1.0 dx=-1.0 dy=-1.0
```

So nested short names under `click` are **not** read by data-ingestor. That is binder design, not an overwrite of already-bound floats.

Upstream note (context only; tracker/SDK not changed in this audit):

- SDK emits click metrics as flat payload fields: `clickViewportXRatio`, …
- Tracker `ClickstreamSdkBinder.bindClick` reads them from payload/`click` object and sets DTO floats
- Tracker Kafka serializer uses `PropertyNamingStrategies.SNAKE_CASE` → flat `click_viewport_x_ratio` on the published document

If tracker publishes the flat snake_case fields, data-ingestor binds them.

## Default -1 sources

All `-1` sources found:

1. **ClickHouse DDL** `DEFAULT -1` on the four columns.
2. **`ClickstreamColumn` defaultExpression** `"-1"` → `defaultValue()` returns `-1.0f` for floating columns.
3. **`ClickstreamEventRecord` constructor** fills every column with `column.defaultValue()`, so ratios start as `-1.0f` before bind.
4. **No assignments** in enrichment / strategy / writer that set these four fields to `-1` after a successful bind.

`ClickstreamEnrichmentPipeline` only mutates:

`captured_ts`, `event_id`, IP/geo/network stubs, entity attribution (`site_id`…`traffic_ownership`).

It does **not** touch click ratios.

No sanitizer/normalizer was found that resets ratios to `-1`.

## Event-type gating

No event-type gate in the binder for these columns.

If the keys are present on any event type, they bind.

Diagnostic used `event_type = "click"`.

Unsupported event type would fail earlier on `EVENT_TYPE` pattern validation, not silently coerce ratios to `-1`.

## Column ordering

Positional insert uses `ClickstreamColumn.values()` / `ClickstreamColumn.names()` in enum order.

Diagnostic ordinals:

```text
click_viewport_x_ratio = 244
click_viewport_y_ratio = 245
click_document_x_ratio = 246
click_document_y_ratio = 247
```

Consecutive and name-aligned. No evidence of cross-column swap among the four ratios.

`ClickstreamRowBinary.encode` writes `writer.setValue(column.columnName(), wire(...))` by name inside the client schema built from the same enum order.

## Diagnostic pipeline

Temporary untracked test:

`src/test/java/ru/refiq/strategy/clickstream/ClickRatioDiagnosticTempTest.java`

Ran:

```bash
mvn -q -Dtest=ClickRatioDiagnosticTempTest test
```

Observed:

| Stage | viewport_x | viewport_y | document_x | document_y | Java type |
|---|---:|---:|---:|---:|---|
| after-map | 0.7946 | 0.0732 | 0.7946 | 0.5147 | Float |
| after-enrich | 0.7946 | 0.0732 | 0.7946 | 0.5147 | Float |
| before writer.write (strategy output) | 0.7946 | 0.0732 | 0.7946 | 0.5147 | Float |

No live ClickHouse INSERT was executed (no Testcontainers / local CH write required for this conclusion). The values that would enter RowBinary encoding are already the expected floats.

## Existing tests

Coverage gap:

- `ClickstreamFlatMappingTest` covers many clickstream fields, including nested `viewport`/`scroll`, but **does not assert** the four ratio columns.
- No unit test name/string match for `click_viewport_*` / `click_document_*` in the permanent suite before this audit.
- Writer tests check batch insert / column list, not ratio values.

So a regression where ratios stay at default `-1` due to missing flat keys would not be caught by current permanent tests.

## Root cause

Inside data-ingestor, `-1` means **default never replaced**, not “decimal coerced to -1”.

Mechanism:

1. `ClickstreamEventRecord` initializes ratios to `-1.0f`.
2. `ClickstreamJsonBinder` replaces the default only if it finds a top-level number under the aliases above.
3. Enrichment / writer do not reset bound floats back to `-1`.

Therefore:

- If Kafka JSON contains the flat decimals → data-ingestor keeps them through to ClickHouse binding values.
- If Kafka JSON omits them / already has `-1` / only nests short names under `click` without flat aliases → ClickHouse receives / stores sentinel `-1`.

Given the stated problem (“SDK sends decimals, CH has -1”), and this audit’s proof that correct flat input is preserved, the loss is **upstream of, or outside, a successful flat bind** (missing/incorrect Kafka field presence, or already-default Kafka payload), not a mapper overwrite after deserialize of `0.7946`.

## Conclusion

**B. DATA-INGESTOR CORRECT**

Proof:

Input flat ratios `0.7946 / 0.0732 / 0.7946 / 0.5147` remain identical `Float` values after:

- JSON parse
- `ClickstreamMapper`
- `ClickstreamEnrichmentPipeline`
- `ClickstreamIngestStrategy` output list passed to `ClickstreamWriter.write`

No class/method in the clickstream ingest path was found that replaces those bound floats with `-1`.

Remaining limitations of this audit:

- No Kafka broker message dump was inspected.
- No live ClickHouse SELECT was run.
- Tracker/SDK were read only for contract context; they were not modified and were not fully re-audited here.
