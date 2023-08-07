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

import com.aliyun.lindorm.tsdb.client.model.Query;
import com.aliyun.lindorm.tsdb.client.model.QueryResult;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.Result;
import com.aliyun.lindorm.tsdb.client.model.ResultSet;
import com.aliyun.lindorm.tsdb.client.model.VersionInfo;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import com.aliyun.lindorm.tsdb.client.utils.ExceptionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author jianhong.hjh
 */
public interface LindormTSDBClient {

    CompletableFuture<WriteResult> write(final Record record);

    CompletableFuture<WriteResult> write(final List<Record> records);

    CompletableFuture<WriteResult> write(final String database, final Record record);

    CompletableFuture<WriteResult> write(final String database, final List<Record> records);

    CompletableFuture<WriteResult> write(String database, Record record, List<String> clusterIdList);

    CompletableFuture<WriteResult> write(String database, List<Record> records, List<String> clusterIdList);

    WriteResult writeSync(final List<Record> records);

    WriteResult writeSync(final List<Record> records, final Map<String, String> params);

    WriteResult writeSync(final List<Record> records, final List<String> clusterIdList);

    WriteResult writeSync(List<Record> records, List<String> clusterIdList, Map<String, String> params);

    WriteResult writeSync(final String database, final List<Record> records);

    WriteResult writeSync(final String database, final List<Record> records, final Map<String, String> params);

    void write(final Record record, Callback callback);

    void write(final List<Record> records, Callback callback);

    default void write(final String database, final Record record, Callback callback) {
        transfer(write(database, record), Collections.singletonList(record), callback);
    }

    default void write(final String database, final List<Record> records, Callback callback) {
        transfer(write(database, records), records, callback);
    }

    default void transfer(CompletableFuture<WriteResult> future, List<Record> records,
            Callback callback) {
        future.whenComplete((r, e) -> {
            callback.onCompletion(r, records, ExceptionUtils.getRootCause(e));
        });
    }

    void flush();

    default boolean createDatabase(String database) {
        Result result = execute(String.format("CREATE DATABASE %s", database));
        return result.isSuccessful();
    }

    default List<String> showDatabases() {
        Result result = execute("SHOW DATABASES");
        if (result == null || result.getRows() == null || result.getRows().isEmpty()) {
            return Collections.emptyList();
        }
        return result.getRows().stream().map(e -> (String) e.get(0)).collect(Collectors.toList());
    }

    default Result execute(String sql) {
        return execute(new Query(sql));
    }

    default Result execute(String database, String sql) {
        return execute(new Query(database, sql));
    }

    Result execute(final Query query);


    ResultSet query(String sql);

    default ResultSet query(Query query) {
        return query(query.getDatabase(), query.getCommand());
    }

    ResultSet query(String database, String sql);

    default ResultSet query(Query query, int chunkSize) {
        return query(query.getDatabase(), query.getCommand(), chunkSize);
    }

    ResultSet query(String database, String sql, int chunkSize);


    VersionInfo getServerVersion();

    boolean isHealth();

    /**
     * Execute a query against a database.
     * <p>
     * One of the consumers will be executed.
     *
     * @param query     the query to execute.
     * @param onSuccess the consumer to invoke when result is received
     * @param onFailure the consumer to invoke when error is thrown
     */
    void query(final Query query, final Consumer<QueryResult> onSuccess,
            final Consumer<Throwable> onFailure);

    /**
     * Execute a streaming query against a database.
     *
     * @param query     the query to execute.
     * @param chunkSize the number of QueryResults to process in one chunk.
     * @param onNext    the consumer to invoke for each received QueryResult
     */
    default void query(Query query, int chunkSize, Consumer<QueryResult> onNext) {
        query(query, chunkSize, onNext, () -> {});
    }

    /**
     * Execute a streaming query against a database.
     *
     * @param query     the query to execute.
     * @param chunkSize the number of QueryResults to process in one chunk.
     * @param onNext    the consumer to invoke for each received QueryResult; with capability to
     *                  discontinue a streaming query
     */
    default void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext) {
        query(query, chunkSize, onNext, () -> {});
    }

    /**
     * Execute a streaming query against a database.
     *
     * @param query      the query to execute.
     * @param chunkSize  the number of QueryResults to process in one chunk.
     * @param onNext     the consumer to invoke for each received QueryResult
     * @param onComplete the onComplete to invoke for successfully end of stream
     */
    default void query(Query query, int chunkSize, Consumer<QueryResult> onNext,
            Runnable onComplete) {
        query(query, chunkSize, (c, q) -> onNext.accept(q), onComplete);
    }

    /**
     * Execute a streaming query against a database.
     *
     * @param query      the query to execute.
     * @param chunkSize  the number of QueryResults to process in one chunk.
     * @param onNext     the consumer to invoke for each received QueryResult; with capability to
     *                   discontinue a streaming query
     * @param onComplete the onComplete to invoke for successfully end of stream
     */
    default void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext,
            Runnable onComplete) {
        query(query, chunkSize, onNext, onComplete, null);
    }

    /**
     * Execute a streaming query against a database.
     *
     * @param query      the query to execute.
     * @param chunkSize  the number of QueryResults to process in one chunk.
     * @param onNext     the consumer to invoke for each received QueryResult; with capability to
     *                   discontinue a streaming query
     * @param onComplete the onComplete to invoke for successfully end of stream
     * @param onFailure  the consumer for error handling
     */
    void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext,
            Runnable onComplete,
            Consumer<Throwable> onFailure);

    void shutdown();
}
