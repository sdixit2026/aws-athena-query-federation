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
