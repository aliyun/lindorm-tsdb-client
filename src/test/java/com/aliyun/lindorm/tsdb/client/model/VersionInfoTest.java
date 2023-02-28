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

import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author jianhong.hjh
 */
@RunWith(JUnitPlatform.class)
public class VersionInfoTest {


    @Test
    public void testSerializeAndDeserialize() {
        String versionString = "{\"version\":\"3.4.10\",\n" +
                "            \"buildTime\":\"2022-02-25T11:56:24+0800\",\n" +
                "            \"full_revision\":\"b78f605ad0118bcf94b4b6c56a6468d48212fe91\"}";

        VersionInfo versionInfo = JSON.parseObject(versionString, VersionInfo.class);

        assertEquals("3.4.10", versionInfo.getVersion());
        assertEquals("2022-02-25T11:56:24+0800", versionInfo.getBuildTime());
        assertEquals("b78f605ad0118bcf94b4b6c56a6468d48212fe91", versionInfo.getFullRevision());

        versionInfo = JSON.parseObject(JSON.toJSONString(versionInfo), VersionInfo.class);

        assertEquals("3.4.10", versionInfo.getVersion());
        assertEquals("2022-02-25T11:56:24+0800", versionInfo.getBuildTime());
        assertEquals("b78f605ad0118bcf94b4b6c56a6468d48212fe91", versionInfo.getFullRevision());
    }
}
