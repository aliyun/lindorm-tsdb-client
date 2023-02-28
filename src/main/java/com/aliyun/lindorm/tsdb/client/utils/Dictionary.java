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

import java.util.HashMap;

public class Dictionary extends HashMap<String, Integer> {

    private int value = 0;

    public int getOrCreate(String key) {
        Integer id = get(key);
        if (id == null) {
            id = (++value);
            this.put(key, id);
        }
        return id;
    }

    public int getValue() {
        return this.value;
    }
}
