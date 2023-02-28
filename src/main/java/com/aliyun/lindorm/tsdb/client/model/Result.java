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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author jianhong.hjh
 */
public class Result {
    private List<String> columns;
    private List<String> metadata;
    private List<List<Object>> rows;

    private String error;

    public Result() {
        this(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    public Result(List<String> columns, List<String> metadata,
            List<List<Object>> rows) {
        this.columns = columns;
        this.metadata = metadata;
        this.rows = rows;
        this.error = null;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<String> metadata) {
        this.metadata = metadata;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public void setRows(List<List<Object>> rows) {
        this.rows = rows;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getError() {
        return this.error;
    }

    @JSONField(serialize = false)
    public boolean isSuccessful() {
        return this.error == null || this.error.length() == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryResult)) {
            return false;
        }
        Result that = (Result) o;
        return Objects.equals(getColumns(), that.getColumns()) &&
                Objects.equals(getMetadata(), that.getMetadata()) &&
                Objects.equals(getRows(), that.getRows()) &&
                Objects.equals(getError(), that.getError());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getColumns(), getMetadata(), getRows(), getError());
    }

    @Override
    public String toString() {
        return "Result{" +
                "columns=" + columns +
                ", metadata=" + metadata +
                ", rows=" + rows +
                ", error='" + error + '\'' +
                '}';
    }
}
