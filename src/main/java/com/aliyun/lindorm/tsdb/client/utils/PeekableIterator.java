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

import java.util.Iterator;
import java.util.List;

/**
 * @author jianhong.hjh
 */
public class PeekableIterator<T> {

    private Iterator<T> iterator;

    private T holder;

    public PeekableIterator(List<T> src) {
        this.iterator = src.iterator();
        this.holder = null;
    }

    public T peek() {
        if (this.holder == null) {
            this.holder = iterator.next();
        }
        return this.holder;
    }

    public boolean hasNext() {
        return this.holder != null ? true : this.iterator.hasNext();
    }

    public T next() {
        if (this.holder != null) {
            T res = this.holder;
            this.holder = null;
            return res;
        }
        return iterator.next();
    }

}
