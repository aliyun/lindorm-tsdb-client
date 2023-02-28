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

import com.aliyun.lindorm.tsdb.client.exception.ClientException;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.net.ConnectException;

public class BasicAuthInterceptor implements Interceptor {

    private String credentials;

    public BasicAuthInterceptor(final String user, final String password) {
        credentials = Credentials.basic(user, password);
    }

    @Override
    public Response intercept(final Chain chain) throws IOException {
        Request request = chain.request();
        Request authenticatedRequest = request.newBuilder().header("Authorization", credentials).build();
        try {
            return chain.proceed(authenticatedRequest);
        } catch (ConnectException e) {
            throw new ClientException(e);
        }
    }
}
