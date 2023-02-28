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
package com.aliyun.lindorm.tsdb.client.example;

import com.aliyun.lindorm.tsdb.client.ClientOptions;
import com.aliyun.lindorm.tsdb.client.LindormTSDBClient;
import com.aliyun.lindorm.tsdb.client.LindormTSDBFactory;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.SchemaPolicy;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;

import java.util.concurrent.CompletableFuture;

/**
 * @author jianhong.hjh
 */
public class WriteDemo1 {

    public static void main(String[] args) {
        String url = "http://127.0.0.1:3002";
        String username = "username";
        String password = "password";
        ClientOptions options = ClientOptions.newBuilder(url)
                .setUsername(username)
                .setPassword(password)
                .setSchemaPolicy(SchemaPolicy.WEAK)
                .build();

        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(options);

        Record record = Record.table("writeDemo1").time(System.currentTimeMillis())
                .tag("wt", "wv").addField("wf", 1.0).build();

        CompletableFuture<WriteResult> future = lindormTSDBClient.write(record);

        // 同步等待结果
        System.out.println(future.join());


        record = Record.table("writeDemo1").time(System.currentTimeMillis())
                .tag("wt", "wv").addField("wf", "stringValue").build();

        future = lindormTSDBClient.write(record);
        // 同步等待结果
        System.out.println(future.join());


        lindormTSDBClient.shutdown();
    }
}
