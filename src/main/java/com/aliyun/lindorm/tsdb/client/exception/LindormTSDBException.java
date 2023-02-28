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
package com.aliyun.lindorm.tsdb.client.exception;

import java.util.Objects;

/**
 * @author jianhong.hjh
 */
public class LindormTSDBException extends RuntimeException {
    private final int code;
    private final String sqlstate;
    private final String message;

    public LindormTSDBException(int code, String sqlstate, String s) {
        super(s);
        this.code = code;
        this.sqlstate = sqlstate;
        this.message = s;
    }

    public int getCode() {
        return code;
    }

    public String getSqlstate() {
        return sqlstate;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LindormTSDBException)) {
            return false;
        }
        LindormTSDBException that = (LindormTSDBException) o;
        return getCode() == that.getCode() &&
                Objects.equals(getSqlstate(), that.getSqlstate()) &&
                Objects.equals(getMessage(), that.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCode(), getSqlstate(), getMessage());
    }

    @Override
    public String toString() {
        return "LindormTSDBException{" +
                "code=" + code +
                ", sqlstate='" + sqlstate + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
