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


import com.aliyun.lindorm.tsdb.client.impl.LindormTSDBClientImpl;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import javax.xml.validation.Schema;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class ClientOptionsTest {


    @Test
    public void testClientOptionsBuilder() {
        String url = "http://ld-xxxx-proxy-tsdb-pub.lindorm.rds.aliyuncs.com:8242";
        String username = "username";
        String password = "password";
        ClientOptions options = ClientOptions.newBuilder(url)
                .setUsername(username)
                .setPassword(password)
                .setNumBatchThreads(2)
                .setMaxRetries(5)
                .setRetryBackoffMs(1)
                .setRequestTimeoutMs(1)
                .setConnectTimeoutMs(1)
                .setMaxWaitTimeMs(1)
                .setBatchSize(5)
//                .setCodecType(CodecType.JSON)
                .setSchemaPolicy(SchemaPolicy.STRONG)
                .setMaxPointBatches(1)
                .setQueryChunkSize(100)
                .setDefaultDatabase("bb")
                .setConnectionPool(5000, 10)
                .build();

        assertEquals(url, options.getUrl());
        assertEquals(username, options.getUsername());
        assertEquals(2, options.getNumBatchThreads());
        assertEquals(5, options.getMaxRetries());
        assertEquals(1, options.getRetryBackoffMs());
        assertEquals(1, options.getRequestTimeoutMs());
        assertEquals(1, options.getConnectTimeoutMs());
        assertEquals(1, options.getMaxWaitTimeMs());
        assertEquals(5, options.getBatchSize());
        assertEquals(1, options.getMaxPointBatches());
//        assertEquals(CodecType.JSON, options.getCodecType());
        assertEquals(SchemaPolicy.STRONG, options.getSchemaPolicy());
        assertEquals(100, options.getQueryChunkSize());
        assertEquals("bb", options.getDefaultDatabase());
        assertEquals(5000, options.getKeepAliveMs());
        assertEquals(10, options.getMaxIdleConn());
    }
}
