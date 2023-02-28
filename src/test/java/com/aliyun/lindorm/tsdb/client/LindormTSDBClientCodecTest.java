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
//
//import com.aliyun.lindorm.tsdb.client.model.Record;
//import com.aliyun.lindorm.tsdb.client.model.WriteResult;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//
///**
// * @author jianhong.hjh
// // */
//@Ignore
//public class LindormTSDBClientCodecTest {
//
//    @Test
//    public void testJSONSerialize() {
//        String url = "http://127.0.0.1:3002";
//
//        ClientOptions options = ClientOptions
//                .newBuilder(url)
//                .setCodecType(CodecType.JSON)
//                .build();
//
//        LindormTSDBClient client = LindormTSDBFactory.connect(options);
//        List<Record> records = TestKit.createRecords(10);
//        CompletableFuture<WriteResult> future = client.write(records);
//        // 同步等待结果
//        System.out.println(future.join());
//    }
//
//
//    @Test
//    public void testLineProtocolSerialize() {
//        String url = "http://127.0.0.1:3002";
//
//        ClientOptions options = ClientOptions
//                .newBuilder(url)
//                .setCodecType(CodecType.BINARY)
//                .build();
//
//        LindormTSDBClient client = LindormTSDBFactory.connect(options);
//        List<Record> records = TestKit.createRecords(10);
//        CompletableFuture<WriteResult> future = client.write(records);
//        // 同步等待结果
//        System.out.println(future.join());
//    }
//
//
//    @Test
//    public void testBinarySerialize() {
//        String url = "http://127.0.0.1:3002";
//
//        ClientOptions options = ClientOptions
//                .newBuilder(url)
//                .setCodecType(CodecType.BINARY)
//                .build();
//
//        LindormTSDBClient client = LindormTSDBFactory.connect(options);
//        List<Record> records = TestKit.createRecords(10);
//        CompletableFuture<WriteResult> future = client.write(records);
//        // 同步等待结果
//        System.out.println(future.join());
//    }
//
//}
