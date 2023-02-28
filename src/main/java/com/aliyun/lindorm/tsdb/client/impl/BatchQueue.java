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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jianhong.hjh
 */
public class BatchQueue {
    private final String database;

    private final ArrayDeque<RecordBatch> queue;

    private final int maxPointBatches;

    private ReentrantLock lock = new ReentrantLock();

    private final Condition wait = lock.newCondition();

    public BatchQueue(String database, int maxPointBatches) {
        this.database = database;
        this.queue = new ArrayDeque<>();
        this.maxPointBatches = maxPointBatches;
    }

    public void lock() {
        this.lock.lock();
    }

    public void unlock() {
        this.lock.unlock();
    }

    public RecordBatch peekLast() {
        return this.queue.peekLast();
    }

    public void addLast(RecordBatch recordBatch) {
        this.queue.addLast(recordBatch);
    }

    public RecordBatch getFirst() {
        return this.queue.getFirst();
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public RecordBatch poll() {
        return this.queue.poll();
    }

    public RecordBatch pollLast() {
        return this.queue.pollLast();
    }

    public String getDatabase() {
        return database;
    }

    public RecordBatch pollFirst() {
        return this.queue.pollFirst();
    }

    public RecordBatch peekFirst() {
        return this.queue.peekFirst();
    }

    public List<RecordBatch> drainAll() {
        List<RecordBatch> recordBatches = new ArrayList<>(queue.size());
        while (!queue.isEmpty()) {
            RecordBatch recordBatch = queue.poll();
            if (recordBatch != null) {
                recordBatch.closeForPointsAppends();
                recordBatch.drained(System.currentTimeMillis());
                recordBatches.add(recordBatch);
            }
        }
        return recordBatches;
    }

    public void addFirst(RecordBatch batch) {
        this.queue.addFirst(batch);
    }

    public void await() throws InterruptedException {
        this.wait.await();
    }

    public void signal() {
        this.wait.signal();
    }

    public void signalAll() {
        this.wait.signalAll();
    }

    public boolean isReachedMaxPointBatch() {
        return this.queue.size() >= maxPointBatches;
    }

    public int size() {
        return this.queue.size();
    }
}
