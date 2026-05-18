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
