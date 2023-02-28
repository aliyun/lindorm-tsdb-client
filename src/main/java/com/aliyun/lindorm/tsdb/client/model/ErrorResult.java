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

import com.alibaba.fastjson.JSONObject;

import java.util.Objects;

/**
 * @author jianhong.hjh
 */
public class ErrorResult {
    private int code;
    private String sqlstate;
    private String message;

    public ErrorResult() {
    }

    public ErrorResult(int code, String sqlstate, String message) {
        this.code = code;
        this.sqlstate = sqlstate;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getSqlstate() {
        return sqlstate;
    }

    public void setSqlstate(String sqlstate) {
        this.sqlstate = sqlstate;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ErrorResult)) {
            return false;
        }
        ErrorResult that = (ErrorResult) o;
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
        return "ErrorResult{" +
                "code=" + code +
                ", sqlstate='" + sqlstate + '\'' +
                ", message='" + message + '\'' +
                '}';
    }


    public static ErrorResult fromJSON(String string) {
        return JSONObject.parseObject(string, ErrorResult.class);
    }
}
