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

import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.QueryResult;
import okhttp3.mockwebserver.MockResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jianhong.hjh
 */
public class TestKit {


    public static Record createRecord() {
        Record record = Record.table("test")
                .time(System.currentTimeMillis())
                .tag("tk", "tv")
                .addField("f1", 1.0)
                .build();
        return record;
    }

    public static List<Record> createRecords(int numPoints) {
        List<Record> records = new ArrayList<>(numPoints);
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i < numPoints; i++) {
            Record record = Record.table("test")
                    .time(currentTime + i * 1000)
                    .tag("tk", "tv")
                    .addField("f1", 1.0)
                    .build();
            records.add(record);
        }
        return records;
    }

    public static QueryResult createQueryResult() {
        int numRows = 5;
        int numCols = 5;
        return createQueryResult(numRows, numCols);
    }

    public static QueryResult createQueryResult(int numRows, int numCols) {
        QueryResult queryResult = new QueryResult();
        List<String> columns = new ArrayList<>(numCols);
        List<String> metadata = new ArrayList<>(numCols);
        for (int i = 0; i < numCols; i++) {
            columns.add("c" + i);
            metadata.add("VARCHAR");
        }

        List<List<Object>> rows = new ArrayList<>(numRows);
        for (int j = 0; j < numRows; j++) {
            List<Object> row = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                row.add("val_" + i * j);
            }
            rows.add(row);
        }
        queryResult.setColumns(columns);
        queryResult.setMetadata(metadata);
        queryResult.setRows(rows);
        return queryResult;
    }

    public static MockResponse versionMockResponse() {
        return new MockResponse()
                .setResponseCode(200)
                .setBody("{\"version\":\"3.4.10\",\n" +
                        "            \"buildTime\":\"2022-02-25T11:56:24+0800\",\n" +
                        "            \"full_revision\":\"b78f605ad0118bcf94b4b6c56a6468d48212fe91\"}");
    }

}
