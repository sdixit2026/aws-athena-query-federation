# EventBridge Warm-Up Handling for PostgreSQL Connector

**Date:** 2026-05-18  
**Status:** Approved

## Goal

Prevent Lambda cold starts by allowing an EventBridge (CloudWatch Events) scheduled rule to periodically invoke the PostgreSQL connector Lambda. The Lambda must detect these warm-up events and return immediately without attempting to deserialize them as Athena federation requests (which would fail with a parse error).

## Context

The Athena Federation SDK's `CompositeHandler.handleRequest(InputStream, OutputStream, Context)` is declared `final`. Subclasses cannot override it. EventBridge scheduled events have a completely different JSON shape from Athena `FederationRequest` objects:

```json
{
  "source": "aws.events",
  "detail-type": "Scheduled Event",
  "detail": {}
}
```

Passing this payload through the existing code path causes a Jackson deserialization error. The fix must intercept the raw bytes before the SDK's deserialization runs.

## Architecture

Restructure both composite handlers from **inheritance to composition**.

### Before

```
PostGreSqlCompositeHandler    extends  CompositeHandler
PostGreSqlMuxCompositeHandler extends  MultiplexingJdbcCompositeHandler (→ CompositeHandler)
```

### After

```
PostGreSqlCompositeHandler    implements RequestStreamHandler
                              └── delegate: CompositeHandler

PostGreSqlMuxCompositeHandler implements RequestStreamHandler
                              └── delegate: MultiplexingJdbcCompositeHandler
```

Lambda still registers these classes as the function handler — the runtime interface (`RequestStreamHandler`) is unchanged.

## Request Flow

```
Lambda invocation
     │
     ▼
PostGreSqlCompositeHandler.handleRequest(InputStream, OutputStream, Context)
     │
     ├─ read all bytes (ByteStreams.toByteArray)
     │
     ├─ isEventBridgeWarmUpEvent(bytes)?
     │       │
     │       ├─ YES → log INFO + return   (empty output, no error)
     │       │
     │       └─ NO  → new ByteArrayInputStream(bytes)
     │                         │
     │                         ▼
     │               delegate.handleRequest(...)   [CompositeHandler / MultiplexingJdbcCompositeHandler]
     │                         │
     │                         ▼
     │               normal Athena federation path (PingRequest, MetadataRequest, RecordRequest)
     ▼
```

## Detection Logic

```java
private static final ObjectMapper WARM_UP_MAPPER = new ObjectMapper();

private boolean isEventBridgeWarmUpEvent(byte[] bytes) {
    try {
        JsonNode root = WARM_UP_MAPPER.readTree(bytes);
        JsonNode source = root.get("source");
        return source != null && "aws.events".equals(source.asText());
    } catch (Exception e) {
        return false;  // parse failure → treat as real request
    }
}
```

**Detection key:** `"source": "aws.events"` is the canonical EventBridge identifier present on all scheduled rules and AWS-service bus events.  
**Fail-safe:** Any exception during the warm-up check returns `false`, so the bytes are forwarded to the delegate unchanged. The delegate may then throw its own error, exactly as it does today.

## Files Changed

| File | Change |
|---|---|
| `src/main/java/.../PostGreSqlCompositeHandler.java` | Implements `RequestStreamHandler`, holds `CompositeHandler` delegate, adds warm-up check |
| `src/main/java/.../PostGreSqlMuxCompositeHandler.java` | Implements `RequestStreamHandler`, holds `MultiplexingJdbcCompositeHandler` delegate, adds warm-up check |

No changes to `athena-federation-sdk` or any other connector module.

## Error Handling

| Scenario | Behaviour |
|---|---|
| EventBridge event received | Log INFO, return immediately, empty output |
| Malformed JSON received | `isEventBridgeWarmUpEvent` returns `false`; delegate handles it (same behaviour as today) |
| Valid Athena request | Bytes re-wrapped in `ByteArrayInputStream`, passed to delegate unchanged |
| Delegate throws | Exception propagates normally |

## Testing

- Unit test: `PostGreSqlCompositeHandler` with a mock EventBridge payload returns without calling the delegate
- Unit test: `PostGreSqlCompositeHandler` with a valid `PingRequest` payload delegates correctly
- Unit test: `PostGreSqlMuxCompositeHandler` same two cases
- The existing `PostGreSqlMetadataHandlerTest` and `PostGreSqlRecordHandlerTest` are unaffected (they test the inner handlers directly)

## Infrastructure Note (out of scope for code change)

To actually keep the Lambda warm, an EventBridge rule must be created targeting the Lambda function on a schedule (e.g., every 5 minutes). This is a deployment/IaC concern and is not part of this code change.
