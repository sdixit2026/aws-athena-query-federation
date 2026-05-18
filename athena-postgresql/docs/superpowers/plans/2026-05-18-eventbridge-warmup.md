# EventBridge Warm-Up Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect EventBridge scheduled warm-up events in the PostgreSQL connector and return immediately without attempting Athena federation request deserialization.

**Architecture:** Both composite handlers are restructured from inheritance to composition — each implements `RequestStreamHandler` directly and holds a delegate. The `handleRequest` override reads all bytes first, checks for `"source":"aws.events"` via Jackson `JsonNode`, and returns early on match; otherwise re-wraps the bytes and forwards to the delegate unchanged.

**Tech Stack:** Java 11, JUnit 4, Mockito, Jackson `jackson-databind` (transitive via SDK), Guava `ByteStreams` (transitive via SDK), Maven.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `src/main/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlCompositeHandler.java` | Implements `RequestStreamHandler`, detects warm-up events, delegates to `CompositeHandler` |
| Modify | `src/main/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlMuxCompositeHandler.java` | Same pattern, delegates to `MultiplexingJdbcCompositeHandler` |
| Create | `src/test/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlCompositeHandlerTest.java` | Unit tests for warm-up detection in non-mux handler |
| Create | `src/test/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlMuxCompositeHandlerTest.java` | Unit tests for warm-up detection in mux handler |

---

## Task 1: Refactor PostGreSqlCompositeHandler

**Files:**
- Modify: `src/main/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlCompositeHandler.java`
- Create: `src/test/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlCompositeHandlerTest.java`

- [ ] **Step 1: Write the failing tests**

Create the file `src/test/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlCompositeHandlerTest.java` with this content:

```java
/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class PostGreSqlCompositeHandlerTest
{
    private RequestStreamHandler mockDelegate;
    private PostGreSqlCompositeHandler handler;
    private Context mockContext;

    @Before
    public void setup()
    {
        mockDelegate = Mockito.mock(RequestStreamHandler.class);
        handler = new PostGreSqlCompositeHandler(mockDelegate);
        mockContext = Mockito.mock(Context.class);
    }

    @Test
    public void testEventBridgeWarmUpEvent_returnsEarlyWithoutDelegating() throws Exception
    {
        String warmUpPayload = "{\"source\":\"aws.events\",\"detail-type\":\"Scheduled Event\",\"detail\":{}}";
        InputStream inputStream = new ByteArrayInputStream(warmUpPayload.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();

        handler.handleRequest(inputStream, outputStream, mockContext);

        Mockito.verify(mockDelegate, Mockito.never()).handleRequest(
                Mockito.any(InputStream.class),
                Mockito.any(OutputStream.class),
                Mockito.any(Context.class));
        assertEquals(0, ((ByteArrayOutputStream) outputStream).size());
    }

    @Test
    public void testNonBridgeEvent_delegatesToCompositeHandler() throws Exception
    {
        String athenaPayload = "{\"@type\":\"PingRequest\",\"identity\":{}}";
        InputStream inputStream = new ByteArrayInputStream(athenaPayload.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();

        handler.handleRequest(inputStream, outputStream, mockContext);

        Mockito.verify(mockDelegate, Mockito.times(1)).handleRequest(
                Mockito.any(InputStream.class),
                Mockito.any(OutputStream.class),
                Mockito.eq(mockContext));
    }

    @Test
    public void testMalformedJson_delegatesToCompositeHandler() throws Exception
    {
        InputStream inputStream = new ByteArrayInputStream("not-valid-json".getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();

        handler.handleRequest(inputStream, outputStream, mockContext);

        Mockito.verify(mockDelegate, Mockito.times(1)).handleRequest(
                Mockito.any(InputStream.class),
                Mockito.any(OutputStream.class),
                Mockito.eq(mockContext));
    }

    @Test
    public void testOtherEventSource_delegatesToCompositeHandler() throws Exception
    {
        String otherPayload = "{\"source\":\"custom.myapp\",\"detail-type\":\"SomeEvent\",\"detail\":{}}";
        InputStream inputStream = new ByteArrayInputStream(otherPayload.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();

        handler.handleRequest(inputStream, outputStream, mockContext);

        Mockito.verify(mockDelegate, Mockito.times(1)).handleRequest(
                Mockito.any(InputStream.class),
                Mockito.any(OutputStream.class),
                Mockito.eq(mockContext));
    }
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
cd /Users/shubhamdixit/saptharushi/aws-athena-query-federation/athena-postgresql
mvn test -Dtest=PostGreSqlCompositeHandlerTest -q 2>&1 | tail -20
```

Expected: compilation error — `PostGreSqlCompositeHandler(RequestStreamHandler)` constructor does not exist yet.

- [ ] **Step 3: Implement PostGreSqlCompositeHandler**

Replace the entire content of `src/main/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlCompositeHandler.java` with:

```java
/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose {@link PostGreSqlMetadataHandler} and {@link PostGreSqlRecordHandler}.
 *
 * Handles EventBridge scheduled warm-up events by detecting {@code "source":"aws.events"} and
 * returning immediately, preventing cold-start deserialization errors.
 *
 * Recommend using {@link PostGreSqlMuxCompositeHandler} instead.
 */
public class PostGreSqlCompositeHandler
        implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(PostGreSqlCompositeHandler.class);
    private static final ObjectMapper WARM_UP_MAPPER = new ObjectMapper();

    private final RequestStreamHandler delegate;

    public PostGreSqlCompositeHandler()
    {
        this.delegate = new CompositeHandler(
                new PostGreSqlMetadataHandler(new PostGreSqlEnvironmentProperties().createEnvironment()),
                new PostGreSqlRecordHandler(new PostGreSqlEnvironmentProperties().createEnvironment()));
    }

    PostGreSqlCompositeHandler(RequestStreamHandler delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
            throws IOException
    {
        byte[] bytes = com.google.common.io.ByteStreams.toByteArray(inputStream);
        if (isEventBridgeWarmUpEvent(bytes)) {
            logger.info("Received EventBridge warm-up event, returning early.");
            return;
        }
        delegate.handleRequest(new ByteArrayInputStream(bytes), outputStream, context);
    }

    private static boolean isEventBridgeWarmUpEvent(byte[] bytes)
    {
        try {
            JsonNode root = WARM_UP_MAPPER.readTree(bytes);
            JsonNode source = root.get("source");
            return source != null && "aws.events".equals(source.asText());
        }
        catch (Exception e) {
            return false;
        }
    }
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
cd /Users/shubhamdixit/saptharushi/aws-athena-query-federation/athena-postgresql
mvn test -Dtest=PostGreSqlCompositeHandlerTest -q 2>&1 | tail -20
```

Expected output:
```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlCompositeHandler.java \
        src/test/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlCompositeHandlerTest.java
git commit -m "feat(postgresql): handle EventBridge warm-up events in PostGreSqlCompositeHandler"
```

---

## Task 2: Refactor PostGreSqlMuxCompositeHandler

**Files:**
- Modify: `src/main/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlMuxCompositeHandler.java`
- Create: `src/test/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlMuxCompositeHandlerTest.java`

- [ ] **Step 1: Write the failing tests**

Create the file `src/test/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlMuxCompositeHandlerTest.java` with this content:

```java
/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class PostGreSqlMuxCompositeHandlerTest
{
    private RequestStreamHandler mockDelegate;
    private PostGreSqlMuxCompositeHandler handler;
    private Context mockContext;

    @Before
    public void setup()
    {
        mockDelegate = Mockito.mock(RequestStreamHandler.class);
        handler = new PostGreSqlMuxCompositeHandler(mockDelegate);
        mockContext = Mockito.mock(Context.class);
    }

    @Test
    public void testEventBridgeWarmUpEvent_returnsEarlyWithoutDelegating() throws Exception
    {
        String warmUpPayload = "{\"source\":\"aws.events\",\"detail-type\":\"Scheduled Event\",\"detail\":{}}";
        InputStream inputStream = new ByteArrayInputStream(warmUpPayload.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();

        handler.handleRequest(inputStream, outputStream, mockContext);

        Mockito.verify(mockDelegate, Mockito.never()).handleRequest(
                Mockito.any(InputStream.class),
                Mockito.any(OutputStream.class),
                Mockito.any(Context.class));
        assertEquals(0, ((ByteArrayOutputStream) outputStream).size());
    }

    @Test
    public void testNonBridgeEvent_delegatesToMuxHandler() throws Exception
    {
        String athenaPayload = "{\"@type\":\"PingRequest\",\"identity\":{}}";
        InputStream inputStream = new ByteArrayInputStream(athenaPayload.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();

        handler.handleRequest(inputStream, outputStream, mockContext);

        Mockito.verify(mockDelegate, Mockito.times(1)).handleRequest(
                Mockito.any(InputStream.class),
                Mockito.any(OutputStream.class),
                Mockito.eq(mockContext));
    }

    @Test
    public void testMalformedJson_delegatesToMuxHandler() throws Exception
    {
        InputStream inputStream = new ByteArrayInputStream("not-valid-json".getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();

        handler.handleRequest(inputStream, outputStream, mockContext);

        Mockito.verify(mockDelegate, Mockito.times(1)).handleRequest(
                Mockito.any(InputStream.class),
                Mockito.any(OutputStream.class),
                Mockito.eq(mockContext));
    }

    @Test
    public void testOtherEventSource_delegatesToMuxHandler() throws Exception
    {
        String otherPayload = "{\"source\":\"custom.myapp\",\"detail-type\":\"SomeEvent\",\"detail\":{}}";
        InputStream inputStream = new ByteArrayInputStream(otherPayload.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();

        handler.handleRequest(inputStream, outputStream, mockContext);

        Mockito.verify(mockDelegate, Mockito.times(1)).handleRequest(
                Mockito.any(InputStream.class),
                Mockito.any(OutputStream.class),
                Mockito.eq(mockContext));
    }
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
cd /Users/shubhamdixit/saptharushi/aws-athena-query-federation/athena-postgresql
mvn test -Dtest=PostGreSqlMuxCompositeHandlerTest -q 2>&1 | tail -20
```

Expected: compilation error — `PostGreSqlMuxCompositeHandler(RequestStreamHandler)` constructor does not exist yet.

- [ ] **Step 3: Implement PostGreSqlMuxCompositeHandler**

Replace the entire content of `src/main/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlMuxCompositeHandler.java` with:

```java
/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcCompositeHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose {@link PostGreSqlMuxMetadataHandler} and {@link PostGreSqlMuxRecordHandler}.
 *
 * Handles EventBridge scheduled warm-up events by detecting {@code "source":"aws.events"} and
 * returning immediately, preventing cold-start deserialization errors.
 */
public class PostGreSqlMuxCompositeHandler
        implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(PostGreSqlMuxCompositeHandler.class);
    private static final ObjectMapper WARM_UP_MAPPER = new ObjectMapper();

    private final RequestStreamHandler delegate;

    public PostGreSqlMuxCompositeHandler() throws ReflectiveOperationException
    {
        this.delegate = new MultiplexingJdbcCompositeHandler(
                PostGreSqlMuxMetadataHandler.class,
                PostGreSqlMuxRecordHandler.class,
                PostGreSqlMetadataHandler.class,
                PostGreSqlRecordHandler.class);
    }

    PostGreSqlMuxCompositeHandler(RequestStreamHandler delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
            throws IOException
    {
        byte[] bytes = com.google.common.io.ByteStreams.toByteArray(inputStream);
        if (isEventBridgeWarmUpEvent(bytes)) {
            logger.info("Received EventBridge warm-up event, returning early.");
            return;
        }
        delegate.handleRequest(new ByteArrayInputStream(bytes), outputStream, context);
    }

    private static boolean isEventBridgeWarmUpEvent(byte[] bytes)
    {
        try {
            JsonNode root = WARM_UP_MAPPER.readTree(bytes);
            JsonNode source = root.get("source");
            return source != null && "aws.events".equals(source.asText());
        }
        catch (Exception e) {
            return false;
        }
    }
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
cd /Users/shubhamdixit/saptharushi/aws-athena-query-federation/athena-postgresql
mvn test -Dtest=PostGreSqlMuxCompositeHandlerTest -q 2>&1 | tail -20
```

Expected output:
```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlMuxCompositeHandler.java \
        src/test/java/com/amazonaws/athena/connectors/postgresql/PostGreSqlMuxCompositeHandlerTest.java
git commit -m "feat(postgresql): handle EventBridge warm-up events in PostGreSqlMuxCompositeHandler"
```

---

## Task 3: Full regression run

**Files:** None modified.

- [ ] **Step 1: Run all postgresql module tests**

```bash
cd /Users/shubhamdixit/saptharushi/aws-athena-query-federation/athena-postgresql
mvn test -q 2>&1 | tail -30
```

Expected output:
```
[INFO] Tests run: N, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

If any pre-existing test fails, note the failure — it is unrelated to this change (these tests do not touch the composite handlers) and should be investigated separately.

- [ ] **Step 2: Confirm the two new test classes appear in the summary**

The output should include lines like:
```
[INFO] Running com.amazonaws.athena.connectors.postgresql.PostGreSqlCompositeHandlerTest
[INFO] Tests run: 4, ...
[INFO] Running com.amazonaws.athena.connectors.postgresql.PostGreSqlMuxCompositeHandlerTest
[INFO] Tests run: 4, ...
```
