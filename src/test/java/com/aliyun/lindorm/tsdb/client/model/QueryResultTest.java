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
package com.aliyun.lindorm.tsdb.client.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONReader;
import com.aliyun.lindorm.tsdb.client.TestKit;
import com.aliyun.lindorm.tsdb.client.impl.QueryResultAdaptor;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class QueryResultTest {


    @Test
    public void testQueryResult() {
        QueryResult queryResult = TestKit.createQueryResult(10,10);
        String s = JSON.toJSONString(queryResult);
        QueryResult target = JSON.parseObject(s, QueryResult.class);
        assertEquals(queryResult.getColumns(), target.getColumns());
        assertEquals(queryResult.getMetadata(), target.getMetadata());
        assertEquals(queryResult.getRows(), queryResult.getRows());
        assertEquals(queryResult, target);
    }

    @Test
    public void testQueryResultAdaptor() {
        QueryResult queryResult = TestKit.createQueryResult(10,10);
        queryResult.setCode(200);
        queryResult.setMessage("ok");
        queryResult.setError("No error");
        queryResult.setSqlstate("Success");
        String s = JSON.toJSONString(queryResult);

        QueryResult result = QueryResultAdaptor.parse(new JSONReader(new StringReader(s)));
        assertEquals(queryResult, result);
        assertEquals(s, JSON.toJSONString(result));
    }

    @Test
    public void testQueryResultAdaptorChunk() {
        QueryResult queryResult1 = TestKit.createQueryResult(5,5);
        queryResult1.setCode(200);
        queryResult1.setMessage("ok");
        queryResult1.setError("No content");
        queryResult1.setSqlstate("Success");
        QueryResult queryResult2 = TestKit.createQueryResult(10,10);
        queryResult2.setMessage("400");
        queryResult2.setError("Not Support");
        queryResult2.setSqlstate("Failed");
        String s = JSON.toJSONString(queryResult1) +  JSON.toJSONString(queryResult2);

        JSONReader jsonReader = new JSONReader(new StringReader(s));
        QueryResult result1 = QueryResultAdaptor.parse(jsonReader);
        QueryResult result2 = QueryResultAdaptor.parse(jsonReader);

        assertEquals(queryResult1, result1);
        assertEquals(queryResult2, result2);
    }
}
