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


import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;


/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class PeekableIteratorTest {


    @Test
    public void testIteratableWithPeek() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add("s" + i);
        }
        PeekableIterator<String> pit = new PeekableIterator<>(list);
        for (int i = 0; i < 100; i++) {

            for (int j = 0; j < 5; j++) {
                assertEquals("s" + i, pit.peek());
            }

            assertTrue(pit.hasNext());

            for (int j = 0; j < 5; j++) {
                assertEquals("s" + i, pit.peek());
            }
            assertEquals("s" + i, pit.next());
        }
    }
}
