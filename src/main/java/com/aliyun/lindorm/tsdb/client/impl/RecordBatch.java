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

import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jianhong.hjh
 */
public class RecordBatch {
    private final long createdMs;
    private String database;
    private int batchSize;
    private List<Record> records;
    private List<CompletableFuture<WriteResult>> futures;

    private AtomicInteger attempts = new AtomicInteger();

    private long lastAttemptMs;
    private long lastAppendTime;
    private long drainedMs;
    private boolean retry;

    private boolean frozen = false;

    private boolean done = false;

    private List<String> clusterIdList = new ArrayList<>();

    public RecordBatch(String database, int batchSize) {
        this(database, batchSize, System.currentTimeMillis());
    }

    public RecordBatch(String database, int batchSize, List<String> clusterIdList) {
        this(database, batchSize, System.currentTimeMillis(), clusterIdList);
    }

    RecordBatch(String database, int batchSize, long createdMs) {
        this.database = database;
        this.batchSize = batchSize;
        this.records = new ArrayList<>(batchSize);
        this.futures = new ArrayList<>(batchSize);
        this.createdMs = createdMs;
        this.lastAttemptMs = this.createdMs;
        this.lastAppendTime = this.createdMs;
        this.retry = false;
    }

    RecordBatch(String database, int batchSize, long createdMs, List<String> clusterIdList) {
        this(database, batchSize, createdMs);
        this.clusterIdList = clusterIdList;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    public List<String> getClusterIdList() {
        return clusterIdList;
    }

    public void setClusterIdList(List<String> clusterIdList) {
        this.clusterIdList = clusterIdList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordBatch)) {
            return false;
        }
        RecordBatch that = (RecordBatch) o;
        return Objects.equals(getDatabase(), that.getDatabase()) &&
                Objects.equals(getRecords(), that.getRecords()) &&
                Objects.equals(getClusterIdList(), that.getClusterIdList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDatabase(), getRecords());
    }

    @Override
    public String toString() {
        return "RecordBatch{" +
                "database='" + database + '\'' +
                "clusterIdList='" + clusterIdList + '\'' +
                ", records=" + records +
                '}';
    }

    public CompletableFuture<WriteResult> tryAppend(Record record, long nowMs) {
        if (isFrozen() || isFull()) {
            return null;
        }
        this.records.add(record);
        return appendFuture();
    }


    public CompletableFuture<WriteResult> tryAppend(Iterator<Record> it, long nowMs) {
        if (isFrozen() || isFull()) {
            return null;
        }
        while (this.records.size() < this.batchSize && it.hasNext()) {
            this.records.add(it.next());
        }
        return appendFuture();
    }

    private CompletableFuture<WriteResult> appendFuture() {
        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        this.futures.add(future);
        return future;
    }

    public void closeForPointsAppends() {
        this.frozen = true;
    }

    public boolean isFull() {
        return this.records.size() >= this.batchSize;
    }

    public boolean isFrozen() {
        return this.frozen;
    }


    public boolean done(Throwable throwable) {
        if (throwable == null) {
            for (CompletableFuture<WriteResult> future : this.futures) {
                future.complete(WriteResult.success());
            }
        } else {
            for (CompletableFuture<WriteResult> future : this.futures) {
                future.completeExceptionally(throwable);
            }
        }
        this.done = true;
        return true;
    }

    public boolean isDone() {
        return this.done;
    }

    public int attempts() {
        return this.attempts.get();
    }

    public long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - lastAttemptMs);
    }

    public void drained(long nowMs) {
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    public void reenqueued(long now) {
        this.attempts.incrementAndGet();
        this.lastAttemptMs = Math.max(lastAppendTime, now);
        this.lastAppendTime = Math.max(lastAppendTime, now);
        this.retry = true;
    }

    public long getCreatedMs() {
        return this.createdMs;
    }

    public long getLastAttemptMs() {
        return lastAttemptMs;
    }


    public long getLastAppendTime() {
        return lastAppendTime;
    }


    public long getDrainedMs() {
        return drainedMs;
    }
}
