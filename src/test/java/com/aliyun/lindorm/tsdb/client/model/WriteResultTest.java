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
package com.aliyun.lindorm.tsdb.client.model;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class WriteResultTest {


    @Test
    public void testWriteResult() {
        WriteResult writeResult = new WriteResult(true);
        assertTrue(writeResult.isSuccessful());
    }


    @Test
    public void testWriteResult0() {
        WriteResult writeResult = WriteResult.success();
        assertTrue(writeResult.isSuccessful());
    }

    @Test
    public void testWriteResult1() {
        WriteResult writeResult = new WriteResult(false);
        assertFalse(writeResult.isSuccessful());
    }
}
