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
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import com.aliyun.lindorm.tsdb.client.utils.LockedBarrier;
import okhttp3.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jianhong.hjh
 */
public class BatchProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BatchProcessor.class);

    public static final okhttp3.MediaType MEDIA_TYPE_STRING = MediaType.parse("text/plain");

    private Map<String, BatchQueue> batchQueues = new ConcurrentHashMap<>();

    private final AtomicInteger appendsInProgress;

    private int batchSize = 500;

    private long retryBackOffMs;

    private int maxPointBatches;

    private boolean closed = false;

    private final LockedBarrier barrier;

    private final long maxWaitTimeMs;

    public BatchProcessor(ClientOptions options, LockedBarrier barrier) {
        this.retryBackOffMs = options.getRetryBackoffMs();
        this.maxWaitTimeMs = options.getMaxWaitTimeMs();
        this.batchSize = options.getBatchSize();
        this.maxPointBatches = options.getMaxPointBatches();
        this.barrier = barrier;

        this.appendsInProgress = new AtomicInteger(0);
    }

    public CompletableFuture<WriteResult> append(String database, Record record, long nowMs) {
        return append(database, record, nowMs, null);
    }

    public CompletableFuture<WriteResult> append(String database, Record record, long nowMs, List<String> cluterIdList) {
        verify();
        this.appendsInProgress.incrementAndGet();
        BatchQueue dp = getOrCreateBatchQueue(database);
        dp.lock();
        try {
            CompletableFuture<WriteResult> future = tryAppend(dp, record, nowMs);
            if (future != null) {
                return future;
            }
            // 若队列已经达到最大批次大小，则一直阻塞直到有剩余空间
            while (dp.isReachedMaxPointBatch()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reached max record batch size. Await the queue is has space to append.");
                }
                try {
                    dp.await();
                } catch (InterruptedException e) {
                    CompletableFuture<WriteResult> result = new CompletableFuture<>();
                    result.completeExceptionally(e);
                    return result;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Signaled after await.");
                }
                // 唤醒后，进行重试
                future = tryAppend(dp, record, nowMs);
                if (future != null) {
                    return future;
                }
            }
            // TODO pool batch points
            RecordBatch recordBatch = new RecordBatch(database, batchSize, cluterIdList);
            future = Objects.requireNonNull(recordBatch.tryAppend(record, nowMs));
            dp.addLast(recordBatch);
            return future;
        } finally {
            dp.unlock();
            this.appendsInProgress.decrementAndGet();
        }
    }

    public CompletableFuture<WriteResult> append(String database, List<Record> records, long nowMs) {
        return append(database, records, nowMs, null);
    }

    public CompletableFuture<WriteResult> append(String database, List<Record> records, long nowMs, List<String> clusterIdList) {
        verify();
        BatchQueue dp = getOrCreateBatchQueue(database);
        int numAppendPoints = records.size();
        this.appendsInProgress.addAndGet(numAppendPoints);
        dp.lock();
        try {
            Iterator<Record> pointIt = records.iterator();
            CompletableFuture<WriteResult> firstFuture = tryAppend(dp, pointIt, nowMs);
            if (firstFuture != null && !pointIt.hasNext()) {
                return firstFuture;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Append to multi point batch.");
            }
            CompletableFuture<WriteResult> result = null;
            if (firstFuture != null) {
                result = firstFuture;
            } else {
                result = CompletableFuture.completedFuture(WriteResult.success());
            }
            int numAppenedBatches = firstFuture == null ? 0 : 1;
            // append remain records
            while (pointIt.hasNext()) {
                // 若队列已经达到最大批次大小，则一直阻塞直到有剩余空间
                while (dp.isReachedMaxPointBatch()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Reached max point batch size. Await the queue is has space to append.");
                    }
                    try {
                        dp.await();
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted.", e);
                        CompletableFuture<WriteResult> exFuture = new CompletableFuture<>();
                        exFuture.completeExceptionally(e);
                        return exFuture;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Signaled after await.");
                    }
                }
                // TODO pool batch records
                RecordBatch recordBatch = new RecordBatch(database, batchSize, clusterIdList);
                CompletableFuture<WriteResult> appendedFuture = Objects.requireNonNull(recordBatch.tryAppend(pointIt, nowMs));
                result = result.thenCombine(appendedFuture, (b, r) -> new WriteResult(b.isSuccessful() && r.isSuccessful()));
                dp.addLast(recordBatch);
                numAppenedBatches++;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("The records split into {} batches for database[{}].", numAppenedBatches, database);
            }
            signal();
            return result;
        } finally {
            dp.unlock();
            this.appendsInProgress.addAndGet(-numAppendPoints);
        }
    }

    private void signal() {
        // signal sender to poll points
        this.barrier.signalAll();
    }

    private LockedBarrier forceFlushBarrier = new LockedBarrier();

    private AtomicBoolean forceFlushing = new AtomicBoolean(false);

    private void verify() {
        if (closed) {
            throw new IllegalStateException("Lindorm TSDB closed while send in progress");
        }
        // 若正在强制flush中则阻塞当前的写入线程
        while (this.forceFlushing.get()) {
            LOG.info("Wait for the forced flush to end.");
            // 阻塞写入，直到强制flush结束
            try {
                this.forceFlushBarrier.await(500);
            } catch (InterruptedException ignored) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignored interrupt event.", ignored);
                }
            }
        }
    }

    public void startForceFlushing() {
        this.forceFlushing.set(true);
    }

    public void finishForceFlushing() {
        this.forceFlushing.set(false);
        // 通知被forceFlushing阻塞的线程
        this.forceFlushBarrier.signalAll();
        // 通知被BatchQueue阻塞的线程
        for (Map.Entry<String, BatchQueue> entry : this.batchQueues.entrySet()) {
            BatchQueue bq = entry.getValue();
            bq.lock();
            try {
                // 通知被阻塞的线程
                bq.signalAll();
            } finally {
                bq.unlock();
            }
        }
    }

    private CompletableFuture<WriteResult> tryAppend(BatchQueue queue, Record record,long nowMs) {
        RecordBatch last = queue.peekLast();
        if (last != null) {
            CompletableFuture<WriteResult> future = last.tryAppend(record, nowMs);
            if (future == null) {
                last.closeForPointsAppends();
            } else {
                // signal sender to poll points
                signal();
                return future;
            }
        }
        return null;
    }

    private CompletableFuture<WriteResult> tryAppend(BatchQueue queue, Iterator<Record> it,long nowMs) {
        RecordBatch last = queue.peekLast();
        if (last != null) {
            CompletableFuture<WriteResult> future = last.tryAppend(it, nowMs);
            if (future == null) {
                last.closeForPointsAppends();
            } else {
                return future;
            }
        }
        return null;
    }

    private BatchQueue getOrCreateBatchQueue(String database) {
        return this.batchQueues.computeIfAbsent(database, k -> new BatchQueue(database, maxPointBatches));
    }

    public Map<String, List<RecordBatch>> drain(long now) {
        Map<String, List<RecordBatch>> batches = new HashMap<>();
        for (Map.Entry<String, BatchQueue> entry : this.batchQueues.entrySet()) {
            String database = entry.getKey();
            BatchQueue batchQueue = entry.getValue();
            batchQueue.lock();
            try {
                RecordBatch first = batchQueue.peekFirst();
                if (first == null) {
                    continue;
                }
                boolean backOff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackOffMs;
                if (backOff) {
                    continue;
                }

                if (first.isFrozen() || first.isFull() || isReachedMaxWaitTime(first, now)) {
                    first.closeForPointsAppends();
                    RecordBatch recordBatch = batchQueue.pollFirst();
                    recordBatch.drained(now);
                    batches.put(database, Collections.singletonList(recordBatch));
                    // 通知被阻塞的线程
                    batchQueue.signalAll();
                }
            } finally {
                batchQueue.unlock();
            }

        }
        return batches;
    }

    public Map<String, List<RecordBatch>> drainAll() {
        Map<String, List<RecordBatch>> batches = new HashMap<>();
        for (Map.Entry<String, BatchQueue> entry : this.batchQueues.entrySet()) {
            String database = entry.getKey();
            BatchQueue batchQueue = entry.getValue();
            batchQueue.lock();
            try {
                List<RecordBatch> recordBatches = batchQueue.drainAll();
                if (recordBatches.isEmpty()) {
                    continue;
                }
                batches.put(database, recordBatches);
            } finally {
                batchQueue.unlock();
            }
        }
        return batches;
    }

    private boolean isReachedMaxWaitTime(RecordBatch batch, long now) {
        return now - batch.getCreatedMs() > this.maxWaitTimeMs;
    }

    public void close() {
        this.closed = true;
    }

    public void reenqueue(RecordBatch batch, long now) {
        batch.reenqueued(now);

        BatchQueue batchQueue = getOrCreateBatchQueue(batch.getDatabase());
        batchQueue.lock();
        try {
            batchQueue.addFirst(batch);
        } finally {
            batchQueue.unlock();
        }
    }
}
