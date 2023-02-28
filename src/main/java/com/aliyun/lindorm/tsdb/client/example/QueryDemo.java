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

import com.aliyun.lindorm.tsdb.client.LindormTSDBClient;
import com.aliyun.lindorm.tsdb.client.LindormTSDBFactory;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.Query;
import com.aliyun.lindorm.tsdb.client.model.QueryResult;
import com.aliyun.lindorm.tsdb.client.model.ResultSet;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author jianhong.hjh
 */
public class QueryDemo {

    public static void main(String[] args) {
        String url = "http://127.0.0.1:3002";
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(url);

        Record record = Record.table("writeDemo").time(System.currentTimeMillis())
                .tag("wt", "wv").addField("wf", 1.0).build();

        CompletableFuture<WriteResult> future = lindormTSDBClient.write(record);

        // 同步等待结果
        System.out.println("write result: " + future.join());

        Query query = new Query("select * from writeDemo");
        ResultSet resultSet = lindormTSDBClient.query(query);

        try {
            QueryResult queryResult = null;
            while ((queryResult = resultSet.next()) != null) {
                List<String> columns = queryResult.getColumns();
                System.out.println("columns: " + columns);
                List<String> metadata = queryResult.getMetadata();
                System.out.println("metadata: " + metadata);
                List<List<Object>> rows = queryResult.getRows();
                for (int i = 0, size = rows.size(); i < size; i++) {
                    List<Object> row = rows.get(i);
                    System.out.println( "row #" + i + " : " + row);
                }
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }

        String targetDB = "demoDB";
        Record record1 = Record.table("writeDemo").time(System.currentTimeMillis())
                .tag("wt", "wv1").addField("wf1", 1.0).build();

        CompletableFuture<WriteResult> future1 = lindormTSDBClient.write(targetDB, record1);

        // 同步等待结果
        System.out.println("write result: " + future1.join());

        // 向指定的数据库查询数据点
        Query query1 = new Query(targetDB, "select * from writeDemo");
        resultSet = lindormTSDBClient.query(query1);

        try {
            QueryResult queryResult = null;
            while ((queryResult = resultSet.next()) != null) {
                List<String> columns = queryResult.getColumns();
                System.out.println("columns: " + columns);
                List<String> metadata = queryResult.getMetadata();
                System.out.println("metadata: " + metadata);
                List<List<Object>> rows = queryResult.getRows();
                for (int i = 0, size = rows.size(); i < size; i++) {
                    List<Object> row = rows.get(i);
                    System.out.println( "row #" + i + " : " + row);
                }
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }
}
