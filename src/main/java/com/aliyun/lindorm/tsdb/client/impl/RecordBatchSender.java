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

import com.aliyun.lindorm.tsdb.client.ClientOptions;
import com.aliyun.lindorm.tsdb.client.CodecType;
import com.aliyun.lindorm.tsdb.client.codec.WriteCodecFactory;
import com.aliyun.lindorm.tsdb.client.exception.ClientException;
import com.aliyun.lindorm.tsdb.client.exception.LindormTSDBException;
import com.aliyun.lindorm.tsdb.client.model.ErrorResult;
import com.aliyun.lindorm.tsdb.client.utils.LockedBarrier;
import com.aliyun.lindorm.tsdb.client.utils.UnsafeBuffer;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.aliyun.lindorm.tsdb.client.impl.BatchProcessor.MEDIA_TYPE_STRING;

/**
 * @author jianhong.hjh
 */
public class RecordBatchSender implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RecordBatchSender.class);

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeoutMs;

    /* The max time to wait before retrying a request which has failed */
    private final long retryBackoffMs;

    private final BatchProcessor batchProcessor;

    private final LindormTSDBService service;

    private int numEmptyBatches = 0;

    private final LockedBarrier barrier;

    private final long maxWaitTimeMs = 100;

    private final String schemaPolicy;

    private final CodecType codecType;

    public RecordBatchSender(ClientOptions options,
            LindormTSDBService service,
            BatchProcessor batchProcessor,
            LockedBarrier barrier) {
        this.retries = options.getMaxRetries();
        this.requestTimeoutMs = options.getRequestTimeoutMs();
        this.retryBackoffMs = options.getRetryBackoffMs();
        this.schemaPolicy = options.getSchemaPolicy().name();
        this.codecType = options.getCodecType();

        this.service = service;
        this.batchProcessor = batchProcessor;
        this.barrier = barrier;

        this.running = true;
    }


    @Override
    public void run() {
        LOG.debug("Starting Lindorm TSDB sender I/O thread.");

        // main loop, runs until shutdown is called
        while (this.running) {
            try {
                runOnce();
            } catch (Exception e) {
                LOG.error("Uncaught error in Lindorm TSDB sender I/O thread: ", e);
            }
        }

        LOG.debug("Beginning shutdown of Lindorm TSDB sender I/O thread, sending remaining points.");
        while (true) {
            long now = System.currentTimeMillis();
            Map<String, List<RecordBatch>> batches = this.batchProcessor.drain(now);
            if (batches.isEmpty()) {
                break;
            }
            sendPointRequests(batches, now, false);
        }
        LOG.debug("Shutdown of Lindorm TSDB sender I/O thread has completed.");
    }

    private void runOnce() throws InterruptedException {
        long now = System.currentTimeMillis();
        Map<String, List<RecordBatch>> batches = this.batchProcessor.drain(now);
        if (batches.isEmpty()) {
            this.numEmptyBatches++;
            if (this.numEmptyBatches > 3) {
                // Wait until a data point enters or timeout
                this.barrier.await(maxWaitTimeMs);
            }
            return;
        }

        this.numEmptyBatches = 0;
        sendPointRequests(batches, now, true);
    }

    public void sendPointRequests(Map<String, List<RecordBatch>> batches, long now, boolean retry) {
        for (Map.Entry<String, List<RecordBatch>> entry : batches.entrySet()) {
            sendPointRequest(now, entry.getKey(), requestTimeoutMs, entry.getValue(), retry);
        }
    }

    private void sendPointRequest(long now, String database, int requestTimeoutMs,
            List<RecordBatch> batches, boolean retry) {
        for (RecordBatch recordBatch : batches) {
            try {
                byte[] encodedResult = WriteCodecFactory.encode(recordBatch.getRecords(), codecType);
                RequestBody requestBody = RequestBody.create(encodedResult, MEDIA_TYPE_STRING);
                Call<ResponseBody> call = this.service.write(database, codecType.name(),
                        this.schemaPolicy, requestBody);
                try {
                    Response<ResponseBody> response = call.execute();
                    completeBatch(recordBatch, call, response, now, retry, codecType);
                } catch (Exception e) {
                    failBatch(recordBatch, call, new ClientException(e), now);
                }
            } catch (Exception e) {
                LOG.error("Failed to send record batch for {}", recordBatch.getDatabase(), e);
                recordBatch.done(new ClientException(e));
            }
        }
    }

    private void completeBatch(RecordBatch batch, Call<ResponseBody> call,
            Response<ResponseBody> response, long now, boolean retry, CodecType type) {
        int code = response.code();
        if (response.isSuccessful()) {
            batch.done(null);
        } else if (code >= 300 && code < 400) { // http redirect
            // TODO handle http redirect
            // Now just ignore it and complete error
            batch.done(new LindormTSDBException(code, "", "The request redirected."));
        } else if (code >= 400) {
            byte[] body = null;
            try {
                body = response.errorBody().bytes();
            } catch (Exception e) {
                LOG.error("Failed to parse response body", e);
                batch.done(new ClientException(e));
                return;
            }

            // handle error
            failBatch(batch, code, body, now, retry, type);
        }
    }

    private void failBatch(RecordBatch batch, int code, byte[] body, long now, boolean retry, CodecType type) {
        try {
            if (code >= 500) { // Server error
                // check if can retry
                if (retry && canRetry(batch, now)) {
                    LOG.warn("Got error send points on database {}, retrying ({} attempts left). Error: {}",
                            batch.getDatabase(), this.retries - batch.attempts() - 1, body);
                    reenqueueBatch(batch, now);
                } else {
                    batch.done(convert(body, type));
                }
            } else {
                if (code == 400) {
                    batch.done(convert(body, type));
                } else {
                    String msg = new String(body, StandardCharsets.UTF_8);
                    LOG.error("Failed to send points. {}", msg);
                    batch.done(new ClientException("status code : " + code
                            + ", msg: " + msg));
                }
            }
        } catch (Exception e){
            batch.done(new ClientException(e));
        }
    }

    public static LindormTSDBException convert(byte[] body, CodecType type) throws Exception {
        switch (type) {
            case LINE_PROTOCOL:
            case JSON:
                String errorBody = new String(body, StandardCharsets.UTF_8);
                ErrorResult errorResult = ErrorResult.fromJSON(errorBody);
                LOG.error("Failed to send points. {}", errorBody);
                return new LindormTSDBException(errorResult.getCode(),
                        errorResult.getSqlstate(), errorResult.getMessage());
            case BINARY:
                UnsafeBuffer buffer = new UnsafeBuffer(body);
                int code = buffer.readInt();
                String sqlstate = buffer.readString();
                String message = buffer.readString();
                LOG.error("Failed to send points. {} - {}", sqlstate, message);
                return new LindormTSDBException(code, sqlstate, message);
        }
        throw new UnsupportedOperationException("Unsupported codec type: " + type.name());
    }

    private void reenqueueBatch(RecordBatch batch, long now) {
        this.batchProcessor.reenqueue(batch, now);
    }


    private void failBatch(RecordBatch batch, Call<ResponseBody> call, Throwable throwable, long now) {
        LOG.error("Failed to send points.", throwable);
        batch.done(throwable);
    }

    private boolean canRetry(RecordBatch batch, long now) {
        return batch.attempts() < this.retries && !batch.isDone();
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
