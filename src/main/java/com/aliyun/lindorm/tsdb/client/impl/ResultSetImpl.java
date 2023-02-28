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
import com.aliyun.lindorm.tsdb.client.model.ResultSet;
import okhttp3.ResponseBody;

import java.io.InputStreamReader;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jianhong.hjh
 */
public class ResultSetImpl implements ResultSet {

    private final ResponseBody chunkedBody;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final JSONReader jsonReader;

    public ResultSetImpl(ResponseBody chunkedBody) {
        Objects.requireNonNull(chunkedBody);
        this.chunkedBody = chunkedBody;
        this.jsonReader = new JSONReader(new InputStreamReader(chunkedBody.byteStream()));
    }

    @Override
    public QueryResult next() {
        if (!chunkedBody.source().isOpen()) {
            return null;
        }
        QueryResult result = QueryResultAdaptor.parse(jsonReader);
        return result;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            this.chunkedBody.close();
        }
    }
}
