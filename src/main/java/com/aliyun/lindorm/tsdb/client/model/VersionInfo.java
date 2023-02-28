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

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Objects;

/**
 * @author jianhong.hjh
 */
public class VersionInfo {
    private String version;

    private String buildTime;

    @JSONField(name = "full_revision")
    private String fullRevision;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBuildTime() {
        return buildTime;
    }

    public void setBuildTime(String buildTime) {
        this.buildTime = buildTime;
    }

    public String getFullRevision() {
        return fullRevision;
    }

    public void setFullRevision(String fullRevision) {
        this.fullRevision = fullRevision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VersionInfo)) {
            return false;
        }
        VersionInfo versionInfo1 = (VersionInfo) o;
        return Objects.equals(getVersion(), versionInfo1.getVersion()) &&
                Objects.equals(getBuildTime(), versionInfo1.getBuildTime()) &&
                Objects.equals(getFullRevision(), versionInfo1.getFullRevision());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getVersion(), getBuildTime(), getFullRevision());
    }

    @Override
    public String toString() {
        return "Version{" +
                "version='" + version + '\'' +
                ", buildTime='" + buildTime + '\'' +
                ", fullRevision='" + fullRevision + '\'' +
                '}';
    }
}
