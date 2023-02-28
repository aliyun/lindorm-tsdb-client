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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONReader;
import com.aliyun.lindorm.tsdb.client.Cancellable;
import com.aliyun.lindorm.tsdb.client.ClientOptions;
import com.aliyun.lindorm.tsdb.client.CodecType;
import com.aliyun.lindorm.tsdb.client.LindormTSDBClient;
import com.aliyun.lindorm.tsdb.client.codec.WriteCodecFactory;
import com.aliyun.lindorm.tsdb.client.exception.ClientException;
import com.aliyun.lindorm.tsdb.client.exception.LindormTSDBException;
import com.aliyun.lindorm.tsdb.client.model.ErrorResult;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.Query;
import com.aliyun.lindorm.tsdb.client.model.QueryResult;
import com.aliyun.lindorm.tsdb.client.model.Result;
import com.aliyun.lindorm.tsdb.client.model.ResultSet;
import com.aliyun.lindorm.tsdb.client.model.VersionInfo;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import com.aliyun.lindorm.tsdb.client.utils.ComparableVersion;
import com.aliyun.lindorm.tsdb.client.utils.LockedBarrier;

import okhttp3.ConnectionPool;
import okhttp3.ConnectionSpec;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.io.InputStreamReader;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.aliyun.lindorm.tsdb.client.impl.BatchProcessor.MEDIA_TYPE_STRING;

/**
 *
 * @author jianhong.hjh
  */
public class LindormTSDBClientImpl implements LindormTSDBClient {
    private static final Logger LOG = LoggerFactory.getLogger(LindormTSDBClientImpl.class);

    private final ClientOptions options;

    private final Retrofit retrofit;

    private final OkHttpClient client;

    private LindormTSDBService service;

    private BatchProcessor batchProcessor;

    private RecordBatchSender[] batchSenders;

    private AtomicBoolean closed = new AtomicBoolean();

    private final LockedBarrier barrier = new LockedBarrier();

    private final ChunkProccesor chunkProccesor;

    private final int queryChunkSize;

    private final String defaultDatabase;

    private final String schemaPolicy;

    private final CodecType codecType;

    private final static ComparableVersion MIN_SERVER_VERSION = new ComparableVersion("3.4.8");

    public LindormTSDBClientImpl(ClientOptions options) {
        this.options = options;
        String username = options.getUsername();
        String password = options.getPassword();

        this.queryChunkSize = options.getQueryChunkSize();
        this.defaultDatabase = options.getDefaultDatabase();

        this.schemaPolicy = options.getSchemaPolicy().name();
        this.codecType = options.getCodecType();

        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();
        if (username != null && password != null) {
            okHttpClientBuilder.addInterceptor(new BasicAuthInterceptor(username, password));
        }

        // connection specs
        okHttpClientBuilder.connectionSpecs(Arrays.asList(
                // support https
                ConnectionSpec.MODERN_TLS,
                ConnectionSpec.COMPATIBLE_TLS,
                // support http
                ConnectionSpec.CLEARTEXT));
        okHttpClientBuilder.sslSocketFactory(socketFactory(), new TrustManagerImpl());
        okHttpClientBuilder.hostnameVerifier(new HostNameVerifierImpl());

        // timeout
        okHttpClientBuilder.connectTimeout(options.getConnectTimeoutMs(), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.callTimeout(options.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.writeTimeout(options.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.readTimeout(options.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
        if (options.getKeepAliveMs() > 0) {
            okHttpClientBuilder.connectionPool(new ConnectionPool(options.getMaxIdleConn(), options.getKeepAliveMs(), TimeUnit.MILLISECONDS));
        }

        this.client = okHttpClientBuilder.build();

        String url = options.getUrl();

        this.retrofit = new Retrofit.Builder().baseUrl(url).addConverterFactory(GsonConverterFactory.create())
                .client(this.client).build();


        this.chunkProccesor = new JSONChunkProccesor();

        this.service = this.retrofit.create(LindormTSDBService.class);

        this.batchProcessor = new BatchProcessor(options, this.barrier);

        VersionInfo versionInfo = getServerVersion();
        LOG.info("The server version is {}", versionInfo.getVersion());
        verifyServerVersion(versionInfo.getVersion());

        // initialize point batch sender
        int numThreads = this.options.getNumBatchThreads();
        this.batchSenders = new RecordBatchSender[numThreads];
        for (int i = 0; i < numThreads; i++) {
            RecordBatchSender batchSender = new RecordBatchSender(options,
                    this.service, this.batchProcessor, this.barrier);
            this.batchSenders[i] = batchSender;
            String name = "lindorm-tsdb-batch-sender-" + i;
            Thread senderThread = new Thread(batchSender, name);
            senderThread.setDaemon(true);
            senderThread.start();
        }
    }

    private void verifyServerVersion(String version) {
        ComparableVersion thatVersion = new ComparableVersion(version);
        if (MIN_SERVER_VERSION.compareTo(thatVersion) > 0) {
            throw new ClientException("The server version is lower than " + MIN_SERVER_VERSION
                    + ", the client is not supported. Please upgrade the server version.");
        }
    }

    @Override
    public CompletableFuture<WriteResult> write(final Record record) {
        return write(this.defaultDatabase, record);
    }

    @Override
    public CompletableFuture<WriteResult> write(final List<Record> records) {
        return write(this.defaultDatabase, records);
    }

    @Override
    public void write(final Record record, com.aliyun.lindorm.tsdb.client.Callback callback) {
        transfer(write(this.defaultDatabase, record), Collections.singletonList(record), callback);
    }

    @Override
    public void write(final List<Record> records, com.aliyun.lindorm.tsdb.client.Callback callback) {
        transfer(write(this.defaultDatabase, records), records, callback);
    }

    @Override
    public CompletableFuture<WriteResult> write(String database, Record record) {
        Objects.requireNonNull(database);
        Objects.requireNonNull(record);

        long nowMs = System.currentTimeMillis();
        return this.batchProcessor.append(database, record, nowMs);
    }

    @Override
    public CompletableFuture<WriteResult> write(String database, List<Record> records) {
        Objects.requireNonNull(database);
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("The records must be not null or empty.");
        }

        long nowMs = System.currentTimeMillis();
        return this.batchProcessor.append(database, records, nowMs);
    }

    @Override
    public WriteResult writeSync(List<Record> records) {
        return writeSync(this.defaultDatabase, records, Collections.emptyMap());
    }

    @Override
    public WriteResult writeSync(List<Record> records, Map<String, String> params) {
        return writeSync(this.defaultDatabase, records, params);
    }

    @Override
    public WriteResult writeSync(String database, List<Record> records) {
        return writeSync(database, records, Collections.emptyMap());
    }

    @Override
    public WriteResult writeSync(String database, List<Record> records,
            Map<String, String> params) {
        Objects.requireNonNull(database);
        if (params == null) {
            params = Collections.emptyMap();
        }
        byte[] encodedResult = WriteCodecFactory.encode(records, codecType);
        RequestBody requestBody = RequestBody.create(encodedResult, MEDIA_TYPE_STRING);
        Call<ResponseBody> call = this.service.write(database, codecType.name(),
                this.schemaPolicy, params, requestBody);
        try {
            Response<ResponseBody> response = call.execute();
            int code = response.code();
            if (response.isSuccessful()) {
                return WriteResult.success();
            } else if (code >= 400) {
                byte[] body = null;
                try {
                    body = response.errorBody().bytes();
                } catch (Exception e) {
                    LOG.error("Failed to parse response body", e);
                    throw new ClientException(e);
                }

                throw RecordBatchSender.convert(body, codecType);
            } else {
                // Now just ignore it and complete error
                throw new LindormTSDBException(code, "unknown", "Unknown error.");
            }
        } catch (LindormTSDBException e) {
            throw e;
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    @Override
    public void flush() {
        if (this.closed.get()) {
            throw new IllegalStateException("Lindorm TSDB already closed.");
        }
        this.batchProcessor.startForceFlushing();
        try {
            RecordBatchSender recordBatchSender = null;
            for (int i = 0; i < batchSenders.length; i++) {
                if (batchSenders[i] != null) {
                    recordBatchSender = this.batchSenders[i];
                    break;
                }
            }
            if (recordBatchSender == null) {
                throw new IllegalStateException("RecordBatchSender is null, can't flush.");
            }
            doFlushAllBatchesInQueue(recordBatchSender);
        } finally {
            this.batchProcessor.finishForceFlushing();
        }
    }

    private void doFlushAllBatchesInQueue(RecordBatchSender recordBatchSender) {
        Map<String, List<RecordBatch>> batches = this.batchProcessor.drainAll();
        if (batches == null || batches.isEmpty()) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("start to force flush records for {}", batches.keySet());
        }
        recordBatchSender.sendPointRequests(batches, System.currentTimeMillis(), false);
    }

    @Override
    public Result execute(Query query) {
        Objects.requireNonNull(query);
        Objects.requireNonNull(query.getDatabase());
        Objects.requireNonNull(query.getCommand());

        RequestBody requestBody = RequestBody.create(MEDIA_TYPE_STRING, query.getCommand());
        Call<QueryResult> call = service.query(query.getDatabase(), requestBody);
        try {
            Response<QueryResult> response = call.execute();
            if (response.isSuccessful()) {
                QueryResult result = response.body();
                if (result == null) {
                    result = new QueryResult();
                }
                return result;
            } else {
                String message = response.errorBody().string();
                ErrorResult errorResult =  ErrorResult.fromJSON(message);
                LindormTSDBException ex = new LindormTSDBException(errorResult.getCode(),
                        errorResult.getSqlstate(), errorResult.getMessage());
                throw ex;
            }
        } catch (IOException e) {
            throw new ClientException(e);
        }
    }

    @Override
    public void query(Query query, Consumer<QueryResult> onSuccess,
            Consumer<Throwable> onFailure) {
        Objects.requireNonNull(query);
        Objects.requireNonNull(query.getDatabase());
        Objects.requireNonNull(query.getCommand());

        RequestBody requestBody = RequestBody.create(MEDIA_TYPE_STRING, query.getCommand());

        Call<QueryResult> call = service.query(query.getDatabase(), requestBody);
        call.enqueue(new Callback<QueryResult>() {

            @Override
            public void onResponse(Call<QueryResult> call, Response<QueryResult> response) {
                if (response.isSuccessful()) {
                    onSuccess.accept(response.body());
                } else {
                    int code = response.code();
                    Throwable t = null;
                    String message = null;
                    try {
                        message = response.errorBody().string();
                    } catch (IOException e) {
                        t = e;
                    }
                    if (t != null) {
                        onFailure.accept(new ClientException(t));
                    } else {
                        ErrorResult errorResult =  ErrorResult.fromJSON(message);
                        LindormTSDBException ex = new LindormTSDBException(errorResult.getCode(),
                                errorResult.getSqlstate(), errorResult.getMessage());
                        onFailure.accept(ex);
                    }
                }
            }

            @Override
            public void onFailure(Call<QueryResult> call, Throwable throwable) {
                onFailure.accept(new ClientException(throwable));
            }
        });
    }

    @Override
    public ResultSet query(String sql) {
        return query(this.defaultDatabase, sql);
    }

    @Override
    public ResultSet query(String database, String sql) {
        return query(database, sql, this.queryChunkSize);
    }

    @Override
    public ResultSet query(String database, String sql, int chunkSize) {
        RequestBody requestBody = RequestBody.create(MEDIA_TYPE_STRING, sql);
        Call<ResponseBody> call = service.query(database, chunkSize, requestBody);
        try {
            Response<ResponseBody> response = call.execute();
            if (response.isSuccessful()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Read a chunked body");
                }
                if (response.code() == 204) {
                    return ResultSet.EMPTY;
                }
                ResponseBody chunkedBody = response.body();
                if (chunkedBody == null) {
                    return ResultSet.EMPTY;
                }
                return new ResultSetImpl(chunkedBody);
            } else {
                int code = response.code();
                String message = response.errorBody().string();
                if (LOG.isDebugEnabled()) {
                    LOG.error("Encountered error while query points. errorCode: {}, message: {}",
                            code, message);
                }
                ErrorResult errorResult = ErrorResult.fromJSON(message);
                throw new LindormTSDBException(errorResult.getCode(),
                        errorResult.getSqlstate(), errorResult.getMessage());
            }
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    @Override
    public VersionInfo getServerVersion() {
        Call<ResponseBody> call = service.version();
        try {
            Response<ResponseBody> response = call.execute();
                if (response.isSuccessful()) {
                    return JSON.parseObject(response.body().string(), VersionInfo.class);
                } else {
                    int code = response.code();
                    String message = response.errorBody().string();
                    if (LOG.isDebugEnabled()) {
                        LOG.error("Encountered error while get server version. errorCode: {}, message: {}",
                                code, message);
                    }
                    ErrorResult errorResult = ErrorResult.fromJSON(message);
                    throw new LindormTSDBException(errorResult.getCode(),
                            errorResult.getSqlstate(), errorResult.getMessage());
                }
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    @Override
    public boolean isHealth() {
        Call<ResponseBody> call = service.health();
        try {
            Response<ResponseBody> response = call.execute();
            return response.isSuccessful();
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    @Override
    public void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext,
            Runnable onComplete, Consumer<Throwable> onFailure) {

        Objects.requireNonNull(query);
        Objects.requireNonNull(query.getDatabase());
        Objects.requireNonNull(query.getCommand());

        RequestBody requestBody = RequestBody.create(MEDIA_TYPE_STRING, query.getCommand());
        Call<ResponseBody> call = service.query(query.getDatabase(), chunkSize, requestBody);
        call.enqueue(new Callback<ResponseBody>() {

            @Override
            public void onResponse(Call<ResponseBody> call, Response<ResponseBody> response) {
                Cancellable cancellable = new Cancellable() {
                    @Override
                    public void cancel() {
                        call.cancel();
                    }

                    @Override
                    public boolean isCanceled() {
                        return call.isCanceled();
                    }
                };

                try {
                    if (response.isSuccessful()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Read a chunked body");
                        }
                        ResponseBody chunkedBody = response.body();
                        chunkProccesor.process(chunkedBody, cancellable, onNext, onComplete);
                    } else {
                        int code = response.code();
                        String message = response.errorBody().string();
                        if (LOG.isDebugEnabled()) {
                            LOG.error("Encountered error while query points. errorCode: {}, message: {}",
                                    code, message);
                        }
                        ErrorResult errorResult =  ErrorResult.fromJSON(message);
                        LindormTSDBException ex = new LindormTSDBException(errorResult.getCode(),
                                errorResult.getSqlstate(), errorResult.getMessage());
                        if (onFailure == null) {
                            throw ex;
                        } else {
                            onFailure.accept(ex);
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Encountered IO error while query points.", e);
                    QueryResult queryResult = new QueryResult();
                    queryResult.setError(e.toString());
                    onNext.accept(cancellable, queryResult);
                    if (onFailure != null) {
                        onFailure.accept(e);
                    }
                } catch (Exception e) {
                    LOG.error("Encountered error while query points.", e);
                    call.cancel();
                    if (onFailure != null) {
                        onFailure.accept(e);
                    }
                }
            }

            @Override
            public void onFailure(Call<ResponseBody> call, Throwable throwable) {
                if (onFailure == null) {
                    throw new ClientException(throwable);
                } else {
                    onFailure.accept(throwable);
                }
            }
        });
    }

    @Override
    public void shutdown() {
        if (!this.closed.compareAndSet(false, true)) {
            LOG.warn("Lindorm TSDB already closed.");
            return;
        }
        LOG.debug("Beginning shutdown of Lindorm TSDB Client.");
        this.batchProcessor.close();
        for (int i = 0; i < batchSenders.length; i++) {
            RecordBatchSender sender = batchSenders[i];
            if (sender != null) {
                sender.setRunning(false);
            }
        }
        // try to flush all batches remained in the queue.
        for (int i = 0; i < batchSenders.length; i++) {
            if (batchSenders[i] != null) {
                doFlushAllBatchesInQueue(batchSenders[i]);
                break;
            }
        }
        LOG.debug("Shutdown of Lindorm TSDB Client has completed.");
    }


    private interface ChunkProccesor {
        void process(ResponseBody chunkedBody, Cancellable cancellable,
                BiConsumer<Cancellable, QueryResult> consumer, Runnable onComplete) throws IOException;
    }

    private class JSONChunkProccesor implements ChunkProccesor {

        @Override
        public void process(final ResponseBody chunkedBody, final Cancellable cancellable,
                final BiConsumer<Cancellable, QueryResult> consumer, final Runnable onComplete)
                throws IOException {
            if (chunkedBody == null) {
                QueryResult queryResult = new QueryResult();
                consumer.accept(cancellable, queryResult);
                if (!cancellable.isCanceled()) {
                    onComplete.run();
                }
                return;
            }
            try {
                JSONReader jsonReader = new JSONReader(new InputStreamReader(chunkedBody.byteStream()));
                while (!cancellable.isCanceled()) {
                    QueryResult result = QueryResultAdaptor.parse(jsonReader);
                    if (result != null) {
                        if (result.getCode() != 0) {
                            throw new LindormTSDBException(result.getCode(), result.getSqlstate(), result.getMessage());
                        }
                        consumer.accept(cancellable, result);
                    }
                }
            } catch (Exception e) {
                QueryResult queryResult = new QueryResult();
                consumer.accept(cancellable, queryResult);
                if (!cancellable.isCanceled()) {
                    onComplete.run();
                }
            } finally {
                chunkedBody.close();
            }
        }
    }


    public static SSLSocketFactory socketFactory() {
        SSLSocketFactory socketFactory = null;
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[] {new TrustManagerImpl()}, new SecureRandom());
            socketFactory = sc.getSocketFactory();
        } catch (Exception e) {

        }
        return socketFactory;
    }

    public static class HostNameVerifierImpl implements HostnameVerifier {

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    }

    public static class TrustManagerImpl implements X509TrustManager, TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates,
                String s) throws CertificateException {

        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates,
                String s) throws CertificateException {

        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }


    // only for test
    void stopAllSenders() {
        for (int i = 0; i < batchSenders.length; i++) {
            batchSenders[i].setRunning(false);
        }
    }
}
