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
import com.aliyun.lindorm.tsdb.client.TestKit;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import com.aliyun.lindorm.tsdb.client.utils.LockedBarrier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class BatchProcessorTest {
    private static final Logger LOG = LoggerFactory.getLogger(BatchProcessorTest.class);

    @Test
    public void testAppendPoint() {
        ClientOptions clientOptions = ClientOptions.newBuilder("xx").build();
        BatchProcessor batchProcessor = new BatchProcessor(clientOptions, new LockedBarrier());

        Record record = Record.table("test")
                .time(System.currentTimeMillis())
                .tag("tk", "tv")
                .addField("f1", 1.0)
                .build();
        CompletableFuture<WriteResult> result = batchProcessor.append("db", record, System.currentTimeMillis());

        assertNotNull(result);
    }

    @Test
    public void testCanBlockWhenReachedMaxPointBatches() {
        int maxPointBatches = 1;
        int batchSize = 10;
        ClientOptions clientOptions = ClientOptions
                .newBuilder("xx")
                .setMaxPointBatches(maxPointBatches)
                .setBatchSize(batchSize)
                .build();
        BatchProcessor batchProcessor = new BatchProcessor(clientOptions, new LockedBarrier());

        Record record = TestKit.createRecord();
        for (int i = 0; i < batchSize; i++) {
            CompletableFuture<WriteResult> result = batchProcessor.append("db", record, System.currentTimeMillis());
            assertNotNull(result);
        }

        AtomicInteger atomicInteger = new AtomicInteger();
        new Thread(() -> {
            assertEquals(0, atomicInteger.get());
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            atomicInteger.incrementAndGet();
            batchProcessor.drain(System.currentTimeMillis());

        }).start();

        CompletableFuture<WriteResult> result = batchProcessor.append("db", record, System.currentTimeMillis());
        assertEquals(1, atomicInteger.get());
        assertNotNull(result);
    }


    @Test
    public void testCanBlockWhenReachedMaxPointBatches1() throws InterruptedException {
        int maxPointBatches = 1;
        int batchSize = 10;
        int maxWaitTimeMs = 1;
        ClientOptions clientOptions = ClientOptions
                .newBuilder("xx")
                .setMaxPointBatches(maxPointBatches)
                .setBatchSize(batchSize)
                .setMaxWaitTimeMs(maxWaitTimeMs)
                .build();

        BatchProcessor batchProcessor = new BatchProcessor(clientOptions, new LockedBarrier());

        Record record = TestKit.createRecord();
        for (int i = 0; i < 10; i++) {
            CompletableFuture<WriteResult> result = batchProcessor.append("db", record, System.currentTimeMillis());
            assertNotNull(result);
        }

        AtomicBoolean atomicBoolean = new AtomicBoolean();
        AtomicBoolean status = new AtomicBoolean(false);
        AtomicReference<CompletableFuture<WriteResult>> reference = new AtomicReference<>();
        new Thread(() -> {
            List<Record> records = TestKit.createRecords(5);
            atomicBoolean.set(false);
            status.set(true);
            CompletableFuture<WriteResult> result = batchProcessor.append("db", records, System.currentTimeMillis());
            reference.set(result);
            atomicBoolean.set(true);
        }).start();

        while (!status.get()) ;
        assertFalse(atomicBoolean.get());
        TimeUnit.SECONDS.sleep(2);
        assertFalse(atomicBoolean.get());
        while (reference.get() == null) {
            batchProcessor.drain(System.currentTimeMillis());
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertTrue(atomicBoolean.get());
    }

    @Test
    public void testCompletedBatch() {
        int maxPointBatches = 1;
        int batchSize = 10;
        int maxWaitTimeMs = 20;
        ClientOptions clientOptions = ClientOptions
                .newBuilder("xx")
                .setMaxPointBatches(maxPointBatches)
                .setBatchSize(batchSize)
                .setMaxWaitTimeMs(maxWaitTimeMs)
                .build();
        BatchProcessor batchProcessor = new BatchProcessor(clientOptions, new LockedBarrier());

        Record record = TestKit.createRecord();

        for (int i = 0; i < 5; i++) {
            CompletableFuture<WriteResult> result = batchProcessor.append("db", record, System.currentTimeMillis());
            assertNotNull(result);
        }

        AtomicInteger atomicInteger = new AtomicInteger();
        AtomicBoolean atomicBoolean = new AtomicBoolean();
        new Thread(() -> {
            assertEquals(0, atomicInteger.get());
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (!atomicBoolean.get()) {
                Map<String, List<RecordBatch>> batches = batchProcessor.drain(System.currentTimeMillis());
                if (!batches.isEmpty()) {
                    batches.values().stream().flatMap(e -> e.stream()).forEach(e -> e.done(null));
                    atomicInteger.incrementAndGet();
                }
            }
        }).start();

        int numPoints = 40;
        List<Record> records = TestKit.createRecords(numPoints);
        CompletableFuture<WriteResult> result = batchProcessor.append("db", records, System.currentTimeMillis());
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int expect = numPoints / batchSize + 1;
        assertEquals(expect, atomicInteger.get());
        assertNotNull(result);
        assertEquals(true, result.join().isSuccessful());
        atomicBoolean.set(true);
    }

    @Test
    public void testFailedBatch() {
        int maxPointBatches = 1;
        int batchSize = 10;
        int maxWaitTimeMs = 20;
        ClientOptions clientOptions = ClientOptions
                .newBuilder("xx")
                .setMaxPointBatches(maxPointBatches)
                .setBatchSize(batchSize)
                .setMaxWaitTimeMs(maxWaitTimeMs)
                .build();
        BatchProcessor batchProcessor = new BatchProcessor(clientOptions, new LockedBarrier());

        Record record = TestKit.createRecord();

        for (int i = 0; i < 5; i++) {
            CompletableFuture<WriteResult> result = batchProcessor.append("db", record, System.currentTimeMillis());
            assertNotNull(result);
        }

        AtomicInteger atomicInteger = new AtomicInteger();
        AtomicBoolean atomicBoolean = new AtomicBoolean();
        new Thread(() -> {
            try {
                assertEquals(0, atomicInteger.get());
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                while (!atomicBoolean.get()) {
                    Map<String, List<RecordBatch>> batches = batchProcessor.drain(System.currentTimeMillis());
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Drained {} Batches", atomicInteger.get());
                    }
                    if (!batches.isEmpty()) {
                        if ((atomicInteger.get() & 01) == 0) {
                            batches.values().stream().flatMap(e -> e.stream()).forEach(e -> e.done(new RuntimeException()));
                        } else {
                            batches.values().stream().flatMap(e -> e.stream()).forEach(e -> e.done(null));
                        }
                        atomicInteger.incrementAndGet();
                        LOG.info("Drained {} Batches", atomicInteger.get());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Finished.");
                }
            }
        }).start();

        int numPoints = 40;
        List<Record> records = TestKit.createRecords(numPoints);
        CompletableFuture<WriteResult> result = batchProcessor.append("db", records, System.currentTimeMillis());
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int expect = numPoints / batchSize + 1;
        assertEquals(expect, atomicInteger.get());
        assertNotNull(result);
        LOG.info("Appended records.");
        CountDownLatch latch = new CountDownLatch(1);

        LOG.info("Wait append complete.");
        result.whenComplete((r, e) -> {
            try {
                assertEquals(null, r);
                assertTrue(e != null && e instanceof RuntimeException);
            } finally {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        atomicBoolean.set(true);
    }


    @Test
    public void testCanDrainAfterReachedMaxWaitTime() {
        int maxPointBatches = 2;
        int batchSize = 10;
        int maxWaitTimeMs = 200;
        ClientOptions clientOptions = ClientOptions
                .newBuilder("xx")
                .setMaxPointBatches(maxPointBatches)
                .setBatchSize(batchSize)
                .setMaxWaitTimeMs(maxWaitTimeMs)
                .build();
        BatchProcessor batchProcessor = new BatchProcessor(clientOptions, new LockedBarrier());
        List<Record> records = TestKit.createRecords(5);
        long start = System.currentTimeMillis();
        CompletableFuture<WriteResult> future = batchProcessor.append("db", records, start);
        long appendedTime = System.currentTimeMillis();
        assertNotNull(future);

        Map<String, List<RecordBatch>> batches = batchProcessor.drain(start);
        assertTrue(batches.isEmpty());

        batches = batchProcessor.drain(start + maxWaitTimeMs);
        assertTrue(batches.isEmpty());

        batches = batchProcessor.drain(appendedTime + maxWaitTimeMs + 1);
        assertTrue(!batches.isEmpty());
    }

    @Test
    public void testCannotDrainBeforePointBatchNotFull() {
        int maxPointBatches = 2;
        int batchSize = 10;
        int maxWaitTimeMs = 300;
        ClientOptions clientOptions = ClientOptions
                .newBuilder("xx")
                .setMaxPointBatches(maxPointBatches)
                .setBatchSize(batchSize)
                .setMaxWaitTimeMs(maxWaitTimeMs)
                .build();
        BatchProcessor batchProcessor = new BatchProcessor(clientOptions, new LockedBarrier());
        List<Record> records = TestKit.createRecords(5);
        CompletableFuture<WriteResult> future = batchProcessor.append("db", records, System.currentTimeMillis());
        assertNotNull(future);

        Map<String, List<RecordBatch>> batches = batchProcessor.drain(System.currentTimeMillis());
        assertTrue(batches.isEmpty());
    }

    @Test
    public void testCanDrainAll() {
        int maxPointBatches = 2;
        int batchSize = 10;
        int maxWaitTimeMs = 300;
        ClientOptions clientOptions = ClientOptions
                .newBuilder("xx")
                .setMaxPointBatches(maxPointBatches)
                .setBatchSize(batchSize)
                .setMaxWaitTimeMs(maxWaitTimeMs)
                .build();
        BatchProcessor batchProcessor = new BatchProcessor(clientOptions, new LockedBarrier());
        List<Record> records = TestKit.createRecords(5);
        batchProcessor.append("db", records, System.currentTimeMillis());

        Map<String, List<RecordBatch>> batches = batchProcessor.drainAll();

        assertNotNull(batches);
        assertEquals(1, batches.size());
        assertEquals(5, batches.entrySet().iterator().next().getValue().iterator().next().getRecords().size());

        batches = batchProcessor.drainAll();
        assertEquals(0, batches.size());
    }
}
