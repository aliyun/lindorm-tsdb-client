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

import java.util.Objects;

/**
 * A Factory to create a instance of Lindorm TSDB client.
 *
 * @author jianhong.hjh
  */
public class LindormTSDBFactory {

    private LindormTSDBFactory() {

    }

    public static LindormTSDBClient connect(String url) {
        return new LindormTSDBClientImpl(ClientOptions.newBuilder(url).build());
    }

    public static LindormTSDBClient connect(String url, String username, String password) {
        Objects.requireNonNull(username);
        ClientOptions options = ClientOptions.newBuilder(url)
                .setUsername(username).setPassword(password).build();
        return new LindormTSDBClientImpl(options);
    }

    public static LindormTSDBClient connect(ClientOptions options) {
        Objects.requireNonNull(options);
        return new LindormTSDBClientImpl(options);
    }

}
