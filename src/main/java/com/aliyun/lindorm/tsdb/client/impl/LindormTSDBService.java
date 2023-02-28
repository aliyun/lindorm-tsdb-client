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

import com.aliyun.lindorm.tsdb.client.model.QueryResult;
import com.aliyun.lindorm.tsdb.client.utils.VersionUtils;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.POST;
import retrofit2.http.Query;
import retrofit2.http.QueryMap;
import retrofit2.http.Streaming;

import java.util.Map;

/**
 * @author jianhong.hjh
 */
public interface LindormTSDBService {

    String CHUNK_SIZE = "chunk_size";
    String DB = "db";
    String SCHEMA_POLICY = "schema_policy";
    String CODEC = "codec";

    String USER_AGENT = "User-Agent: LindormTSDBClient/";

    @POST("api/v3/write")
    @Headers(USER_AGENT + VersionUtils.VERSION)
    Call<ResponseBody> write(@Query(DB) String database,
            @Query(CODEC) String codec,
            @Query(SCHEMA_POLICY) String schemaPolicy,
            @Body RequestBody batchPoints);

    @POST("api/v3/write")
    @Headers(USER_AGENT + VersionUtils.VERSION)
    Call<ResponseBody> write(@Query(DB) String database,
            @Query(CODEC) String codec,
            @Query(SCHEMA_POLICY) String schemaPolicy,
            @QueryMap Map<String, String> params,
            @Body RequestBody batchPoints);

    @POST("api/v2/sql")
    @Headers(USER_AGENT + VersionUtils.VERSION)
    Call<QueryResult> query(@Query(DB) String database,
            @Body RequestBody query);

    @Streaming
    @POST("api/v2/sql?chunked=true")
    @Headers(USER_AGENT + VersionUtils.VERSION)
    Call<ResponseBody> query(@Query(DB) String database,
            @Query(CHUNK_SIZE) int chunkSize,
            @Body RequestBody query);


    @GET("api/version")
    @Headers(USER_AGENT + VersionUtils.VERSION)
    Call<ResponseBody> version();

    @GET("api/health")
    @Headers(USER_AGENT + VersionUtils.VERSION)
    Call<ResponseBody> health();
}
