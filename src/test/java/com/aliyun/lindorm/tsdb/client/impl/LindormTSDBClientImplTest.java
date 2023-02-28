/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.lindorm.tsdb.client.impl;

import com.alibaba.fastjson.JSON;
import com.aliyun.lindorm.tsdb.client.ClientOptions;
import com.aliyun.lindorm.tsdb.client.LindormTSDBClient;
import com.aliyun.lindorm.tsdb.client.LindormTSDBFactory;
import com.aliyun.lindorm.tsdb.client.SchemaPolicy;
import com.aliyun.lindorm.tsdb.client.TestKit;
import com.aliyun.lindorm.tsdb.client.exception.ClientException;
import com.aliyun.lindorm.tsdb.client.model.Query;
import com.aliyun.lindorm.tsdb.client.model.QueryResult;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.ResultSet;
import com.aliyun.lindorm.tsdb.client.model.VersionInfo;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import com.aliyun.lindorm.tsdb.client.utils.VersionUtils;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.*;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class LindormTSDBClientImplTest {


    public static final String URL = "http://127.0.0.1:7878";
    private MockWebServer server = new MockWebServer();

    @BeforeEach
    public void before() throws IOException {
        final Dispatcher dispatcher = new Dispatcher() {

            @Override
            public MockResponse dispatch(
                    RecordedRequest recordedRequest) throws InterruptedException {
                String userAgent = recordedRequest.getHeader("User-Agent");
                assertEquals("LindormTSDBClient/" + VersionUtils.VERSION, userAgent);
                switch (recordedRequest.getRequestUrl().encodedPath()) {
                    case "/api/write":
                    case "/api/v2/write":
                    case "/api/v3/write":
                        System.out.println(recordedRequest.getRequestUrl());
                        return new MockResponse().setResponseCode(204);
                    case "/api/v2/sql":
                        String db = recordedRequest.getRequestUrl().queryParameter("db");
                        String body = "";
                        if (db == null || db.equals("default")) {
                            body = JSON.toJSONString(TestKit.createQueryResult(5, 5));
                        } else {
                            body = JSON.toJSONString(TestKit.createQueryResult(6, 6));
                        }
                        return new MockResponse()
                                .setResponseCode(200)
                                .setBody(body);
                    case "/api/version":
                        return TestKit.versionMockResponse();
                    case "/api/health":
                        return new MockResponse()
                                .setResponseCode(200)
                                .setBody("OK");
                    default:
                        return new MockResponse().setResponseCode(404);
                }
            }
        };
        server.setDispatcher(dispatcher);
        server.start(7878);
    }

    @AfterEach
    public void after() throws IOException {
        server.shutdown();
    }


    @Test
    public void testWritePoint() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        CompletableFuture<WriteResult> future = client.write(TestKit.createRecord());
        assertTrue(future.join().isSuccessful());
    }


    @Test
    public void testWritePoints() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        CompletableFuture<WriteResult> future = client.write(TestKit.createRecords(5));
        assertTrue(future.join().isSuccessful());
    }

    @Test
    public void testWriteSyncPoints() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        Map<String, String> params = new HashMap<>();
        params.put("cluster_id", "xxxxx");
        WriteResult writeResult = client.writeSync(TestKit.createRecords(5), params);
        assertTrue(writeResult.isSuccessful());
    }

    @Test
    public void testWriteSyncPoints0() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        Map<String, String> params = new HashMap<>();
        params.put("cluster_id", "xxxxx");
        WriteResult writeResult = client.writeSync("test", TestKit.createRecords(5), params);
        assertTrue(writeResult.isSuccessful());
    }

    @Test
    public void testWriteSyncPointsWithEmptyParams() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        Map<String, String> params = new HashMap<>();
        WriteResult writeResult = client.writeSync(TestKit.createRecords(5), params);
        assertTrue(writeResult.isSuccessful());
    }

    @Test
    public void testWritePointWithDB() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        String database = "demo";
        CompletableFuture<WriteResult> future = client.write(database, TestKit.createRecord());
        assertTrue(future.join().isSuccessful());
    }


    @Test
    public void testWritePointsWithDB() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        String database = "demo";
        CompletableFuture<WriteResult> future = client.write(database, TestKit.createRecords(5));
        assertTrue(future.join().isSuccessful());
    }

    @Test
    public void testQueryPoints() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        String command = "select * from demo";
        ResultSet result = client.query(command);
        assertEquals(result.next(), TestKit.createQueryResult(5,5));
        result.close();
    }

    @Test
    public void testQueryPointsWithDB() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        String database = "demo";
        String command = "select * from demo";
        ResultSet result = client.query(database, command);
        assertEquals(result.next(), TestKit.createQueryResult(6,6));
        result.close();
    }

    @Test
    public void testQueryPointsWithDB0() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        String database = "demo";
        String command = "select * from demo";
        ResultSet result = client.query(new Query(database, command));
        assertEquals(result.next(), TestKit.createQueryResult(6, 6));
        result.close();
    }

    @Test
    public void testGetServerVersion() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        VersionInfo versionInfo = client.getServerVersion();

        assertNotNull(versionInfo);
        assertEquals("3.4.10", versionInfo.getVersion());
        assertEquals("2022-02-25T11:56:24+0800", versionInfo.getBuildTime());
        assertEquals("b78f605ad0118bcf94b4b6c56a6468d48212fe91", versionInfo.getFullRevision());

        client.shutdown();
    }

    @Test
    public void testCanFlush() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        client.stopAllSenders();
        int numBatches = 5;
        CompletableFuture[] futures = new CompletableFuture[numBatches];
        for (int i = 0; i < numBatches; i++) {
            futures[i] = client.write("database" + i, TestKit.createRecords(5));
        }

        for (CompletableFuture future : futures) {
            assertFalse(future.isDone());
        }

        client.flush();

        for (CompletableFuture future : futures) {
            assertTrue(future.isDone());
        }
        client.shutdown();
    }

    @Test
    public void testCanWriteAfterFlush() throws InterruptedException {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        int numBatches = 5;
        CompletableFuture[] futures = new CompletableFuture[numBatches];
        for (int i = 0; i < numBatches; i++) {
            futures[i] = client.write("database" + i, TestKit.createRecords(5));
        }

        for (CompletableFuture future : futures) {
            assertFalse(future.isDone());
        }

        CountDownLatch latch = new CountDownLatch(1);

        CompletableFuture[] futures1 = new CompletableFuture[numBatches];

        CountDownLatch latch1 = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                int numBatches = 5;

                for (int i = 0; i < numBatches; i++) {
                    futures1[i] = client.write("database" + i, TestKit.createRecords(5));
                }
                latch1.countDown();
            }
        }).start();

        client.flush();
        latch.countDown();

        for (CompletableFuture future : futures) {
            assertTrue(future.isDone());
        }
        latch1.await();
        TimeUnit.SECONDS.sleep(1);
        for (CompletableFuture future : futures1) {
            assertTrue(future.isDone());
        }
        client.shutdown();
    }

    @Test
    public void testCanFlushWhenShutdown() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        client.stopAllSenders();
        int numBatches = 5;
        CompletableFuture[] futures = new CompletableFuture[numBatches];
        for (int i = 0; i < numBatches; i++) {
            futures[i] = client.write("database" + i, TestKit.createRecords(5));
        }
        for (CompletableFuture future : futures) {
            assertFalse(future.isDone());
        }
        client.shutdown();

        for (CompletableFuture future : futures) {
            assertTrue(future.isDone());
        }
    }
    
    @Test
    public void testConnectionCheck() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        assertNotNull(client);
    }

    @Test
    public void testConnectionCheckWithErrorURL() {
        ClientOptions options = ClientOptions.newBuilder("http://127.0.0.1:7890").build();
        ClientException clientException = Assertions.assertThrows(ClientException.class, () -> {
            LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        });
        assertNotNull(clientException);
    }

    @Test
    public void testHealth() {
        ClientOptions options = ClientOptions.newBuilder(URL).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        boolean status = client.isHealth();
        assertTrue(status);
    }

    @Disabled
    @Test
    public void testSchemaPolicyWithWeak() {
        ClientOptions options = ClientOptions
                .newBuilder("http://127.0.0.1:3002")
                .setSchemaPolicy(SchemaPolicy.WEAK)
                .build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);

        client.write(Record
                .table("test")
                .time(System.currentTimeMillis())
                .addField("f1", 1.0)
                .addTag("t1", "v1")
                .build()).join();

        client.write(Record
                .table("test")
                .time(System.currentTimeMillis())
                .addField("f1", "string")
                .addTag("t1", "v1")
                .build()).join();

        client.shutdown();
    }

    @Disabled
    @Test
    public void testSchemaPolicyWithWeak2() {
        ClientOptions options = ClientOptions
                .newBuilder("http://127.0.0.1:3002")
                .setSchemaPolicy(SchemaPolicy.WEAK)
                .build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);

        client.write(Record
                .table("test1")
                .time(System.currentTimeMillis())

                .addField("f1", "string")
                .addTag("t1", "v1")
                .build()).join();

        client.write(Record
                .table("test1")
                .time(System.currentTimeMillis())
                .addField("f1", 1.0)
                .addTag("t1", "v1")
                .build()).join();

        client.shutdown();
    }

    @Disabled
    @Test
    public void testSchemaPolicyWithWeak3() {
        ClientOptions options = ClientOptions
                .newBuilder("http://127.0.0.1:3002")
                .setSchemaPolicy(SchemaPolicy.WEAK)
                .build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);

        client.write(Record
                .table("test1")
                .time(System.currentTimeMillis())

                .addField("f1", "string")
                .addTag("t1", "v1")
                .build()).join();

        ResultSet resultSet = client.query("select * from test1");
        try {
            System.out.println(resultSet.next());
        } finally {
            resultSet.close();
        }

        client.shutdown();
    }

    @Disabled
    @Test
    public void testGetServerVersion0() {
        ClientOptions options = ClientOptions
                .newBuilder("http://127.0.0.1:3002")
                .setSchemaPolicy(SchemaPolicy.WEAK)
                .build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);

        VersionInfo versionInfo = client.getServerVersion();
        assertNotNull(versionInfo);
        assertNotNull(versionInfo.getBuildTime());
        assertNotNull(versionInfo.getVersion());
        assertNotNull(versionInfo.getFullRevision());
        System.out.println(versionInfo);

        client.shutdown();
    }

    @Test
    @Disabled
    public void testConnectionCheck0() {
        ClientOptions options = ClientOptions.newBuilder("http://127.0.0.1:3002").build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        assertNotNull(client);
    }

    @Test
    @Disabled
    public void testHealth0() {
        ClientOptions options = ClientOptions.newBuilder("http://127.0.0.1:3002").build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        boolean status = client.isHealth();
        assertTrue(status);
    }
}
