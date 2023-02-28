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

import com.aliyun.lindorm.tsdb.client.TestKit;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author jianhong.hjh
 */
public class RecordBatchTest {

    private String database = "db";
    private int batchSize = 10;

    @Test
    public void testAppendPointsWhenNotFull() {
        RecordBatch recordBatch = new RecordBatch(database, batchSize);
        Record record = TestKit.createRecord();
        CompletableFuture<WriteResult> future = recordBatch.tryAppend(record, System.currentTimeMillis());
        assertNotNull(future);
        assertFalse(recordBatch.isFull());
    }

    @Test
    public void testCanNotAppendPointsWhenFull() {
        RecordBatch recordBatch = new RecordBatch(database, batchSize);
        List<Record> records = TestKit.createRecords(batchSize);

        CompletableFuture<WriteResult> future = recordBatch.tryAppend(records.iterator(), System.currentTimeMillis());
        assertNotNull(future);
        assertTrue(recordBatch.isFull());

        Record record = TestKit.createRecord();
        future = recordBatch.tryAppend(record, System.currentTimeMillis());
        assertNull(future);
    }

    @Test
    public void testCanNotAppendPointsWhenFrozen() {
        RecordBatch recordBatch = new RecordBatch(database, batchSize);
        recordBatch.closeForPointsAppends();
        assertTrue(recordBatch.isFrozen());
        Record record = TestKit.createRecord();
        CompletableFuture<WriteResult> future = recordBatch.tryAppend(record, System.currentTimeMillis());
        assertNull(future);
    }

    @Test
    public void testIsFrozenAfterClosed() {
        RecordBatch recordBatch = new RecordBatch(database, batchSize);
        recordBatch.closeForPointsAppends();
        Record record = TestKit.createRecord();
        CompletableFuture<WriteResult> future = recordBatch.tryAppend(record, System.currentTimeMillis());
        assertNull(future);
    }

    @Test
    public void testReenqueued() {
        RecordBatch recordBatch = new RecordBatch(database, batchSize);
        assertEquals(0, recordBatch.attempts());
        long now = System.currentTimeMillis();
        recordBatch.reenqueued(now);
        assertEquals(1, recordBatch.attempts());
        assertEquals(now, recordBatch.getLastAppendTime());
        assertEquals(now, recordBatch.getLastAttemptMs());
    }
}
