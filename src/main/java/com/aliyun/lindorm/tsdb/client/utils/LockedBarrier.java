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
package com.aliyun.lindorm.tsdb.client.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LockedBarrier {

    private final ReentrantLock lock;

    private final Condition condition;

    public LockedBarrier() {
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
    }

    public void await() throws InterruptedException {
        this.lock.lock();
        try {
            this.condition.await();
        } finally {
            this.lock.unlock();
        }
    }

    public void await(long maxWaitTimeMs) throws InterruptedException {
        this.lock.lock();
        try {
            this.condition.await(maxWaitTimeMs, TimeUnit.MILLISECONDS);
        } finally {
            this.lock.unlock();
        }
    }

    public void signalAll() {
        this.lock.lock();
        try {
            this.condition.signalAll();
        } finally {
            this.lock.unlock();
        }
    }
}
