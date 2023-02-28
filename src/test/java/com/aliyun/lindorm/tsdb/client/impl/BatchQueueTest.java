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
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class BatchQueueTest {

    private String database = "db";
    private int maxPointBatches = 2;
    private int batchSize = 5;

    @Test
    public void testReachedMaxBatchQueue() {
        BatchQueue queue = new BatchQueue(database, maxPointBatches);
        RecordBatch pb = mock(RecordBatch.class);
        queue.addLast(pb);
        assertFalse(queue.isReachedMaxPointBatch());

        pb = mock(RecordBatch.class);
        queue.addLast(pb);
        assertTrue(queue.isReachedMaxPointBatch());
    }

    @Test
    public void testPeekAndPollFirst() {
        BatchQueue queue = new BatchQueue(database, maxPointBatches);
        RecordBatch pb1 = new RecordBatch(database, batchSize);
        pb1.tryAppend(TestKit.createRecord(), System.currentTimeMillis());
        queue.addLast(pb1);

        RecordBatch pb2 = new RecordBatch(database, batchSize);
        pb2.tryAppend(TestKit.createRecord(), System.currentTimeMillis());
        queue.addLast(pb2);

        RecordBatch pb3 = new RecordBatch(database, batchSize);
        pb3.tryAppend(TestKit.createRecord(), System.currentTimeMillis());
        queue.addLast(pb3);

        assertEquals(pb1, queue.peekFirst());
        assertEquals(pb1, queue.pollFirst());

        assertEquals(pb2, queue.peekFirst());
        assertEquals(pb2, queue.pollFirst());

        assertEquals(pb3, queue.peekFirst());
        assertEquals(pb3, queue.pollFirst());
    }


    @Test
    public void testPeekAndPollHybrid() {
        BatchQueue queue = new BatchQueue(database, maxPointBatches);
        RecordBatch pb1 = new RecordBatch(database, batchSize);
        pb1.tryAppend(TestKit.createRecord(), System.currentTimeMillis());
        queue.addLast(pb1);

        RecordBatch pb2 = new RecordBatch(database, batchSize);
        pb2.tryAppend(TestKit.createRecord(), System.currentTimeMillis());
        queue.addFirst(pb2);

        RecordBatch pb3 = new RecordBatch(database, batchSize);
        pb3.tryAppend(TestKit.createRecord(), System.currentTimeMillis());
        queue.addLast(pb3);

        assertEquals(pb3, queue.peekLast());
        assertEquals(pb3, queue.pollLast());

        assertEquals(pb2, queue.peekFirst());
        assertEquals(pb2, queue.pollFirst());

        assertEquals(pb1, queue.peekFirst());
        assertEquals(pb1, queue.pollFirst());
    }
}
