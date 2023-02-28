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
import com.aliyun.lindorm.tsdb.client.SchemaPolicy;
import com.aliyun.lindorm.tsdb.client.TestKit;
import com.aliyun.lindorm.tsdb.client.model.Query;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.ResultSet;
import com.aliyun.lindorm.tsdb.client.model.VersionInfo;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import com.aliyun.lindorm.tsdb.client.utils.VersionUtils;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class LindormTSDBClientImplTest0 {


    public static final String URL = "https://127.0.0.1:7878";
    private MockWebServer server = new MockWebServer();


    //Keystore information for https
    private static String KEYSTORE_PASSWORD = "password";
    private static String KEY_PASSWORD = "password";

    private static SSLContext sslContext = null;

    public static SSLContext getSSLContext() {
        if (sslContext != null) {
            return sslContext;
        }
        try {
            KeyStore keystore = KeyStore.getInstance("JKS");
            InputStream inputStream = LindormTSDBClientImplTest0.class.getResourceAsStream("/SslAuthenticationTest.jks");
            keystore.load(inputStream, KEYSTORE_PASSWORD.toCharArray());
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                    .getDefaultAlgorithm());
            kmf.init(keystore, KEY_PASSWORD.toCharArray());
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), null, null);
            return sslContext;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static SSLSocketFactory getSSLSocketFactory() {
        return (getSSLContext() != null) ? getSSLContext().getSocketFactory() : null;
    }

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
                        return new MockResponse()
                                .setResponseCode(200)
                                .setBody("{\"version\":\"3.4.10\",\n" +
                                        "            \"buildTime\":\"2022-02-25T11:56:24+0800\",\n" +
                                        "            \"full_revision\":\"b78f605ad0118bcf94b4b6c56a6468d48212fe91\"}");
                    default:
                        return new MockResponse().setResponseCode(404);
                }
            }
        };

        server.useHttps(getSSLSocketFactory(), false);
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
    public void testWritePointsWithKeepAlive() {
        ClientOptions options = ClientOptions.newBuilder(URL).setConnectionPool(3000, 3).build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        CompletableFuture<WriteResult> future = client.write(TestKit.createRecords(5));
        assertTrue(future.join().isSuccessful());
    }

    @Test
    public void testWritePointsWithSchemaPolicyNone() {
        ClientOptions options = ClientOptions.newBuilder(URL).setSchemaPolicy(SchemaPolicy.NONE).build();
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
}
