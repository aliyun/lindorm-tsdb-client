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

import com.alibaba.fastjson.JSONReader;
import com.aliyun.lindorm.tsdb.client.model.QueryResult;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fastjson.parser.JSONToken.COMMA;
import static com.alibaba.fastjson.parser.JSONToken.EOF;

/**
 * @author yunxing
 */
public class QueryResultAdaptor {
    public static QueryResult parse(JSONReader jsonReader) {
        if (jsonReader.peek() == EOF) {
            return null;
        }

        jsonReader.startObject();
        QueryResult result = new QueryResult();
        List metadata = null;
        while (jsonReader.hasNext()) {
            String field = jsonReader.readString();
            switch (field) {
                case "columns":
                    List columns = jsonReader.readObject(ArrayList.class);
                    result.setColumns(columns);
                    break;
                case "metadata":
                    metadata = jsonReader.readObject(ArrayList.class);
                    result.setMetadata(metadata);
                    break;
                case "rows":
                    jsonReader.startArray();
                    List<List<Object>> rows = new ArrayList<>();
                    while (jsonReader.hasNext()) {
                        List<Object> row;
                        if (metadata != null) {
                            row = new ArrayList<>();
                            jsonReader.startArray();
                            for (int i = 0; i < metadata.size(); i++) {
                                if ("BIGINT".equals(metadata.get(i)) || "TIMESTAMP".equals(metadata.get(i))) {
                                    row.add(jsonReader.readObject(Long.TYPE));
                                } else if ("DOUBLE".equals(metadata.get(i))) {
                                    row.add(jsonReader.readObject(Double.TYPE));
                                } else {
                                    row.add(jsonReader.readObject());
                                }
                            }
                            jsonReader.endArray();
                        } else {
                            row = jsonReader.readObject(ArrayList.class);
                        }
                        rows.add(row);
                        if (jsonReader.peek() != COMMA) {
                            break;
                        }
                    }
                    result.setRows(rows);
                    jsonReader.endArray();
                    break;
                case "error":
                    result.setError(jsonReader.readString());
                    break;
                case "code":
                    result.setCode(jsonReader.readInteger());
                    break;
                case "message":
                    result.setMessage(jsonReader.readString());
                    break;
                case "sqlstate":
                    result.setSqlstate(jsonReader.readString());
                    break;
                default:
                    throw new IllegalStateException("Not support parameter : " + field);
            }
        }
        jsonReader.endObject();

        return result;
    }
}
