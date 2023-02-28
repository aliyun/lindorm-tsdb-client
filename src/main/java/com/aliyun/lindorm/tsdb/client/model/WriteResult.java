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

import java.util.Objects;

/**
 * @author jianhong.hjh
 */
public class WriteResult {
    private final boolean successful;

    // append others field if necessary

    public WriteResult(boolean successful) {
        this.successful = successful;
    }

    public boolean isSuccessful() {
        return this.successful;
    }

    public static WriteResult success() {
        return new WriteResult(true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WriteResult)) {
            return false;
        }
        WriteResult that = (WriteResult) o;
        return isSuccessful() == that.isSuccessful();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isSuccessful());
    }

    @Override
    public String toString() {
        return "WriteResult{" +
                "successful=" + successful +
                '}';
    }
}
