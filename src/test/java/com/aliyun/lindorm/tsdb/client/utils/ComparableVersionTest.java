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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class ComparableVersionTest {


    @Test
    public void compareMinVersion() {
        ComparableVersion minVersion = new ComparableVersion("3.4.7");
        assertEquals(1, minVersion.compareTo(new ComparableVersion("3.4.3")));
    }

    @Test
    public void compareMaxVersion() {
        ComparableVersion maxVersion = new ComparableVersion("3.4.3");
        assertEquals(-1, maxVersion.compareTo(new ComparableVersion("3.4.7")));
    }

}
