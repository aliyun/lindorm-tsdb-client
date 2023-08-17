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
package com.aliyun.lindorm.tsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.lindorm.tsdb.client.exception.ClientException;
import com.aliyun.lindorm.tsdb.client.exception.LindormTSDBException;
import com.aliyun.lindorm.tsdb.client.impl.LindormTSDBClientImpl;
import com.aliyun.lindorm.tsdb.client.model.Query;
import com.aliyun.lindorm.tsdb.client.model.QueryResult;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.Result;
import com.aliyun.lindorm.tsdb.client.model.ResultSet;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import com.aliyun.lindorm.tsdb.client.utils.ExceptionUtils;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class LindormTSDBClientTest {


    private MockWebServer createWriteMockWebServer(int port) throws IOException {
        MockWebServer server = new MockWebServer();
        server.enqueue(TestKit.versionMockResponse());
        server.enqueue(new MockResponse().setResponseCode(204));
        server.start(port);
        return server;
    }


    @Test
    public void testVerifyServerVersion() throws IOException {
        MockWebServer server = new MockWebServer();
        MockResponse mockResponse =  new MockResponse()
                .setResponseCode(200)
                .setBody("{\"version\":\"3.4.3\",\n" +
                        "            \"buildTime\":\"2022-02-25T11:56:24+0800\",\n" +
                        "            \"full_revision\":\"b78f605ad0118bcf94b4b6c56a6468d48212fe91\"}");
        server.enqueue(mockResponse);
        server.enqueue(new MockResponse().setResponseCode(204));
        server.start(7878);
        ClientOptions options = ClientOptions.newBuilder("http://127.0.0.1:7878").build();
        ClientException clientException = Assertions.assertThrows(ClientException.class, () -> {
            LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);
        });
        assertNotNull(clientException);

        server.shutdown();
    }

    @Test
    public void testVerifyServerVersion0() throws IOException {
        MockWebServer server = new MockWebServer();
        MockResponse mockResponse =  new MockResponse()
                .setResponseCode(200)
                .setBody("{\"version\":\"3.4.10\",\n" +
                        "            \"buildTime\":\"2022-02-25T11:56:24+0800\",\n" +
                        "            \"full_revision\":\"b78f605ad0118bcf94b4b6c56a6468d48212fe91\"}");
        server.enqueue(mockResponse);
        server.enqueue(new MockResponse().setResponseCode(204));
        server.start(7878);
        ClientOptions options = ClientOptions.newBuilder("http://127.0.0.1:7878").build();
        LindormTSDBClientImpl client = new LindormTSDBClientImpl(options);

        assertNotNull(client);

        server.shutdown();
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        // 1.创建客户端实例
        String url = "http://127.0.0.1:3002";
        // LindormTSDBClient线程安全，可以重复使用，无需频繁创建和销毁
        ClientOptions options = ClientOptions.newBuilder(url)
                .setUsername("tsdb")
                .setPassword("enxU^gPq#gUqEyM")
                .setMaxRetries(15)
                .setMaxWaitTimeMs(3000)
                .setConnectionPool(1000, 10)
                .build();
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(options);

        String dbname = "testSDK";
        String tablename = "sensor";

        lindormTSDBClient.execute(String.format("drop database if exists %s", dbname));
        lindormTSDBClient.execute(String.format("CREATE DATABASE %s", dbname));
        //lindormTSDBClient.execute("demo","drop table sensor");
        Thread.sleep(1000L);
        lindormTSDBClient.execute(dbname, String.format("CREATE TABLE %s (device_id VARCHAR TAG,region VARCHAR TAG,time BIGINT,temperature DOUBLE,humidity LONG, fielda BIGINT, PRIMARY KEY(device_id))", tablename));

        // 3.写入数据
        int numRecords = 1000;
        List<Record> records = new ArrayList<>(numRecords);
        long currentTime = 1647937300000L;

        for (int j = 0; j < 1; j++) {
            for (int i = 0; i < numRecords; i++) {
                Record record = Record
                        .table(tablename)
                        .time(currentTime + i)
                        .tag("device_id", "F07A")
                        .tag("region", "north-cn")
                        //                        .tag("device_id", "F07A" + String.format("%06d", i))
                        //                        .tag("region", i % 4 == 0 ? "north-cn" : i % 4 == 1 ? "south-cn" : i % 4 == 2 ? "east-cn" : "west-cn")
                        .addField("temperature", 1.0)
                        .addField("humidity", 9L)
                        .addField("fielda", 10L)
                        .build();

                records.add(record);
                if (records.size() % 1000 == 0) {
                    CompletableFuture<WriteResult> future = lindormTSDBClient.write(dbname, records);

                    // 处理异步写入结果
                    future.whenComplete((r, ex) -> {
                        // 处理写入失败
                        if (ex != null) {
                            System.out.println("Failed to write.");
                            Throwable throwable = ExceptionUtils.getRootCause(ex);
                            if (throwable instanceof LindormTSDBException) {
                                LindormTSDBException e = (LindormTSDBException) throwable;
                                System.out.println("Caught an LindormTSDBException, which means your request made it to Lindorm TSDB, "
                                        + "but was rejected with an error response for some reason.");
                                System.out.println("Error Code: " + e.getCode());
                                System.out.println("SQL State:  " + e.getSqlstate());
                                System.out.println("Error Message: " + e.getMessage());
                            }  else if (throwable instanceof ClientException) {
                                System.out.println(throwable.getCause().getMessage());
                            } else {
                                throwable.printStackTrace();
                            }
                        } else  {
                            System.out.println("Write successfully.");
                        }
                    });
                    try {
                        WriteResult write = future.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
//                    System.out.println(future.join());
                    records.clear();
                }
            }

            if (records.size() > 0) {
                CompletableFuture<WriteResult> future = lindormTSDBClient.write(dbname, records);

                // 处理异步写入结果
                future.whenComplete((r, ex) -> {
                    // 处理写入失败
                    if (ex != null) {
                        System.out.println("Failed to write.");
                        Throwable throwable = ExceptionUtils.getRootCause(ex);
                        if (throwable instanceof LindormTSDBException) {
                            LindormTSDBException e = (LindormTSDBException) throwable;
                            System.out.println("Caught an LindormTSDBException, which means your request made it to Lindorm TSDB, "
                                    + "but was rejected with an error response for some reason.");
                            System.out.println("Error Code: " + e.getCode());
                            System.out.println("SQL State:  " + e.getSqlstate());
                            System.out.println("Error Message: " + e.getMessage());
                        }  else {
                            throwable.printStackTrace();
                        }
                    } else  {
                        System.out.println("Write successfully.");
                    }
                });
                if (!future.isCompletedExceptionally()) {
                    System.out.println(future.join());
                }
                records.clear();
            }
        }





        // 4.查询数据
        String sql = "select * from sensor limit 10";
        ResultSet resultSet = lindormTSDBClient.query(dbname, sql);

        try {
            // 处理查询结果
            QueryResult result = null;
            // 查询结果使用分批的方式返回，默认每批1000行
            // 当resultSet的next()方法返回为null，表示已经读取完所有的查询结果
            int indexTemprature = 3, indexHumidity = 4, indexFielda = 5;
            while ((result = resultSet.next()) != null) {
                List<String> columns = result.getColumns();
                System.out.println("columns: " + columns);
                List<String> metadata = result.getMetadata();
                System.out.println("metadata: " + metadata);
                //check temperature, humidity, fielda;
                Assert.assertEquals(metadata.get(indexTemprature), "DOUBLE");
                Assert.assertEquals(metadata.get(indexHumidity), "BIGINT");
                Assert.assertEquals(metadata.get(indexFielda), "BIGINT");
                List<List<Object>> rows = result.getRows();
                for (int i = 0, size = rows.size(); i < size; i++) {
                    List<Object> row = rows.get(i);
                    System.out.println("row #" + i + " : " + row);
                    Assert.assertTrue(row.get(indexTemprature) instanceof Double);
                    Assert.assertTrue(row.get(indexHumidity) instanceof Long);
                    Assert.assertTrue(row.get(indexFielda) instanceof Long);
                }
            }
        } finally {
            // 查询结束后，需确保调用ResultSet的close方法，以释放IO资源
            resultSet.close();
        }

    }

    @Test
    public void testWritePointWithCallback() throws IOException, InterruptedException {
        MockWebServer server = createWriteMockWebServer(7878);
        String url = "http://127.0.0.1:7878";
        String username = "username";
        String password = "password";
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);

        Record record = TestKit.createRecord();
        CountDownLatch latch = new CountDownLatch(1);
        lindormTSDBClient.write(record, (result, points, e) -> {
            try {
                // 同步等待结果
                assertTrue(result.isSuccessful());
                assertEquals(1, points.size());
                assertEquals(record, points.get(0));
                assertNull(e);
                latch.countDown();
            } finally {
                latch.countDown();
            }
        });

        latch.await();

        lindormTSDBClient.shutdown();

        server.shutdown();
    }


    @Test
    public void testWritePointsWithCallback() throws IOException, InterruptedException {
        MockWebServer server = createWriteMockWebServer(7878);
        String url = "http://127.0.0.1:7878";
        String username = "username";
        String password = "password";
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);

        List<Record> records = TestKit.createRecords(100);
        CountDownLatch latch = new CountDownLatch(1);
        lindormTSDBClient.write(records, (result, points1, e) -> {
            try {
                // 同步等待结果
                assertTrue(result.isSuccessful());
                assertEquals(records, points1);
                assertNull(e);
            } finally {
                latch.countDown();
            }
        });

        latch.await();

        lindormTSDBClient.shutdown();

        server.shutdown();
    }

    @Test
    public void testWritePointWithCallback0() throws IOException, InterruptedException {
        MockWebServer server = createWriteMockWebServer(7878);
        String url = "http://127.0.0.1:7878";
        String username = "username";
        String password = "password";
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);

        Record record = TestKit.createRecord();
        CountDownLatch latch = new CountDownLatch(1);
        String database = "demo";
        lindormTSDBClient.write(database, record, (result, points, e) -> {
            try {
                // 同步等待结果
                assertTrue(result.isSuccessful());
                assertEquals(1, points.size());
                assertEquals(record, points.get(0));
                assertNull(e);
                latch.countDown();
            } finally {
                latch.countDown();
            }
        });

        latch.await();

        lindormTSDBClient.shutdown();

        server.shutdown();
    }


    @Test
    public void testWritePointsWithCallback0() throws IOException, InterruptedException {
        MockWebServer server = createWriteMockWebServer(7878);
        String url = "http://127.0.0.1:7878";
        String username = "username";
        String password = "password";
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);

        List<Record> records = TestKit.createRecords(100);
        CountDownLatch latch = new CountDownLatch(1);
        String database = "demo";
        lindormTSDBClient.write(database, records, (result, points1, e) -> {
            try {
                // 同步等待结果
                assertTrue(result.isSuccessful());
                assertEquals(records, points1);
                assertNull(e);
            } finally {
                latch.countDown();
            }
        });

        latch.await();

        lindormTSDBClient.shutdown();

        server.shutdown();
    }

    @Test
    public void testWriteWithACL() throws IOException {
        MockWebServer server = createWriteMockWebServer(7878);
        String url = "http://127.0.0.1:7878";
        String username = "username";
        String password = "password";
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);

        Record record = TestKit.createRecord();
        CompletableFuture<WriteResult> future = lindormTSDBClient.write(record);

        // 同步等待结果
        assertTrue(future.join().isSuccessful());

        lindormTSDBClient.shutdown();

        server.shutdown();
    }

    @Test
    public void testCantWriteWithErrorACL() throws InterruptedException, IOException {
        MockWebServer server = new MockWebServer();
        server.enqueue(TestKit.versionMockResponse());
        server.enqueue(new MockResponse().setResponseCode(401));
        server.start(7878);

        String url = "http://127.0.0.1:7878";
        String username = "username";
        String password = "password";
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);

        // 写入一批数据点，
        int num = 10;
        List<Record> records = TestKit.createRecords(num);

        CountDownLatch latch = new CountDownLatch(1);
        lindormTSDBClient.write(records).whenComplete((r, e) -> {
            try {
                Throwable ex = ExceptionUtils.getRootCause(e);
                if (ex != null) {
                    e = ex;
                }
                assertTrue(e != null && e instanceof LindormTSDBException);
                assertEquals(401, ((LindormTSDBException) e).getCode());
            } finally {
                latch.countDown();
            }
        });

        latch.await();
        lindormTSDBClient.shutdown();
        server.shutdown();
    }


    @Test
    public void testCantWriteWithErrorACL0() throws InterruptedException, IOException {
        MockWebServer server = new MockWebServer();
        server.enqueue(TestKit.versionMockResponse());
        server.enqueue(new MockResponse().setResponseCode(401));
        server.start(7878);

        String url = "http://127.0.0.1:7878";
        String username = "username";
        String password = "password";
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);

        // 写入一批数据点，
        int num = 10;
        List<Record> records = TestKit.createRecords(num);

        CountDownLatch latch = new CountDownLatch(1);
        lindormTSDBClient.write(records, (result, points1, e) -> {
            try {
                assertTrue(e != null && e instanceof LindormTSDBException);
                assertEquals(401, ((LindormTSDBException) e).getCode());
            } finally {
                latch.countDown();
            }
        });

        latch.await();
        lindormTSDBClient.shutdown();
        server.shutdown();
    }


    @Test
    public void testQuery() throws IOException {
        MockWebServer server = createQueryServer();
        try {
            server.start(7878);
            String url = "http://127.0.0.1:7878";
            LindormTSDBClient client = LindormTSDBFactory.connect(url);
            String command = "select * from demo";
            ResultSet result = client.query(command);
            Assertions.assertEquals(result.next(), TestKit.createQueryResult(5,5));
            result.close();
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testHealth() throws IOException {
        MockWebServer server = new MockWebServer();
        try {
            server.enqueue(TestKit.versionMockResponse());
            server.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));
            server.start(7878);
            String url = "http://127.0.0.1:7878";
            LindormTSDBClient client = LindormTSDBFactory.connect(url);
            assertEquals(true, client.isHealth());
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testNotHealth() throws IOException {
        MockWebServer server = new MockWebServer();
        try {
            server.enqueue(TestKit.versionMockResponse());
            server.enqueue(new MockResponse().setResponseCode(500));
            server.start(7878);
            String url = "http://127.0.0.1:7878";
            LindormTSDBClient client = LindormTSDBFactory.connect(url);
            assertEquals(false, client.isHealth());
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testQueryWithDB() throws IOException {
        MockWebServer server = createQueryServer();
        try {
            server.start(7878);
            String url = "http://127.0.0.1:7878";
            LindormTSDBClient client = LindormTSDBFactory.connect(url);
            String database = "demo";
            String command = "select * from demo";
            ResultSet result = client.query(database, command);
            Assertions.assertEquals(result.next(), TestKit.createQueryResult(6, 6));
            result.close();
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testQueryWithDB0() throws IOException {
        MockWebServer server = createQueryServer();
        try {
            server.start(7878);
            String url = "http://127.0.0.1:7878";
            LindormTSDBClient client = LindormTSDBFactory.connect(url);
            String database = "demo";
            String command = "select * from demo";
            ResultSet result = client.query(new Query(database, command));
            Assertions.assertEquals(result.next(), TestKit.createQueryResult(6, 6));
            result.close();
        } finally {
            server.shutdown();
        }
    }

    @Disabled
    @Test
    void testExecute() {
        String url = "http://127.0.0.1:3002";
        LindormTSDBClient client = LindormTSDBFactory.connect(url);
        String sql = "CREATE DATABASE test";
        Result result = client.execute(sql);
        assertTrue(result.isSuccessful());
    }

    @Disabled
    @Test
    void testExecute0() {
        String url = "http://127.0.0.1:3002";
        LindormTSDBClient client = LindormTSDBFactory.connect(url);
        String sql = "SHOW DATABASES";
        Result result = client.execute(sql);
        assertTrue(result.isSuccessful());
        assertTrue(result.getRows().size() > 0);
        System.out.println(result);
        List<List<Object>> databases = result.getRows();
        for (int i = 0, size = databases.size(); i < size; i++) {
            System.out.println("database #" + i + ": " + databases.get(i).get(0));
        }
    }


    @Disabled
    @Test
    void testQueryChunkedWithError() {
        String url = "http://127.0.0.1:3002";
        LindormTSDBClient client = LindormTSDBFactory.connect(url);

        Record record = TestKit.createRecord();
        CompletableFuture<WriteResult> future = client.write(record);
        future.join();

        String sql = "select * from test";

        CountDownLatch latch = new CountDownLatch(1);
        client.query(new Query(sql), 2, new BiConsumer<Cancellable, QueryResult>() {
            @Override
            public void accept(Cancellable cancellable, QueryResult queryResult) {
                System.out.println(queryResult);
                System.out.println(queryResult.getCode() + "  " + queryResult.getSqlstate() + " " + queryResult.getMessage());
            }
        }, new Runnable() {
            @Override
            public void run() {
                latch.countDown();
            }
        }, new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) {
                throwable.printStackTrace();
                System.out.println("Encountered error.");
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Disabled
    @Test
    void testExecute1() {
        String url = "http://127.0.0.1:3002";
        LindormTSDBClient client = LindormTSDBFactory.connect(url);
        String sql = "";
        Result result;
        sql = "Drop table sensor ";
        result = client.execute("test",sql);
        sql = "CREATE TABLE sensor (device_id VARCHAR TAG,region VARCHAR TAG,time BIGINT,temperature DOUBLE,humidity DOUBLE,PRIMARY KEY(device_id))";
        result = client.execute("test",sql);

        assertTrue(result.isSuccessful());
    }

    @Disabled
    @Test
    void testExecute2() {
        String url = "http://127.0.0.1:3002";
        LindormTSDBClient client = LindormTSDBFactory.connect(url);
        String sql = "SHOW tables";
        Result result = client.execute("test", sql);
        assertTrue(result.isSuccessful());
        assertTrue(result.getRows().size() > 0);
        System.out.println(result);
        List<List<Object>> databases = result.getRows();
        for (int i = 0, size = databases.size(); i < size; i++) {
            System.out.println("table #" + i + ": " + databases.get(i).get(0));
        }
    }

    @Test
    public void testQueryWithNoContent() throws IOException {
        MockWebServer server = new MockWebServer();
        try {
            server.enqueue(TestKit.versionMockResponse());
            server.enqueue(new MockResponse().setResponseCode(204));
            server.start(7878);

            String url = "http://127.0.0.1:7878";
            String username = "username";
            String password = "password";
            LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);
            String database = "demo";
            String sql = "select * from demo";
            ResultSet result = lindormTSDBClient.query(new Query(database, sql));
            assertTrue(result != null);
            assertTrue(result.next() == null);
            result.close();

            lindormTSDBClient.shutdown();
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testQueryChunkedWithNoContent() throws Exception {
        MockWebServer server = new MockWebServer();
        try {
            server.enqueue(TestKit.versionMockResponse());
            server.enqueue(new MockResponse().setResponseCode(204));
            server.start(7878);

            String url = "http://127.0.0.1:7878";
            String username = "username";
            String password = "password";
            LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url, username, password);
            String database = "demo";
            String sql = "select * from demo";
            CountDownLatch latch = new CountDownLatch(1);
            lindormTSDBClient.query(new Query(database, sql), 2, new Consumer<QueryResult>() {
                @Override
                public void accept(QueryResult result) {
                    try {
                        assertTrue(result != null);
                        assertTrue(result.getRows().isEmpty());
                        assertTrue(result.getColumns().isEmpty());
                        assertTrue(result.getMetadata().isEmpty());
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
            lindormTSDBClient.shutdown();
        } finally {
            server.shutdown();
        }
    }

//    @Ignore
//    @Test
//    public void testQueryPointsUseChunked() throws IOException {
//        String url = "http://127.0.0.1:3002";
//        LindormTSDBClient client = LindormTSDBFactory.connect(url);
//
//        int numPoints = 100;
//        long startTime = 1640248774257L;
//        List<Record> records = new ArrayList<>(numPoints);
//        for (int i = 0; i < numPoints; i++) {
//            long time = startTime + i * 1000;
//            Record record = Record.table("writeDemo")
//                    .time(time)
//                    .tag("wt", "wv")
//                    .addField("wf", 1.0 * (i + 1))
//                    .build();
//            records.add(record);
//        }
//
//        client.write(records).join();
//
//        String command = "select time, wf from writeDemo where time >= " + startTime + " and time <= " + startTime + numPoints * 1000;
//        int chunkSize = 10;
//        CountDownLatch latch = new CountDownLatch(1);
//        AtomicInteger counter = new AtomicInteger();
//        client.query(new Query(command), chunkSize, (cancellable, queryResult) -> {
//            assertTrue(queryResult.getRows().size() <= 10);
//            System.out.println(queryResult.getRows());
//            counter.addAndGet(queryResult.getRows().size());
//        }, latch::countDown, (r) -> {
//        });
//
//        try {
//            latch.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        assertEquals(numPoints, counter.get());
//    }


    private MockWebServer createQueryServer() {
        MockWebServer server = new MockWebServer();
        final Dispatcher dispatcher = new Dispatcher() {

            @Override
            public MockResponse dispatch(
                    RecordedRequest recordedRequest) throws InterruptedException {
                switch (recordedRequest.getRequestUrl().encodedPath()) {
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
                    default:
                        return new MockResponse().setResponseCode(404);
                }
            }
        };
        server.setDispatcher(dispatcher);

        return server;
    }
}
