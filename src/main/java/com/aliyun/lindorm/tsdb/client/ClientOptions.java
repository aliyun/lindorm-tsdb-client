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
package com.aliyun.lindorm.tsdb.client;


import java.util.Objects;

/**
 * @author jianhong.hjh
 */
public class ClientOptions {

    private final String url;

    private final String username;

    private final String password;

    private int numBatchThreads = 1;

    private long connectTimeoutMs;

    private int maxRetries;

    private int requestTimeoutMs = 30_000;

    /* The max time to wait before retrying a request which has failed */
    private long retryBackoffMs = 100;

    private long maxWaitTimeMs = 300;

    private int keepAliveMs = -1;

    private int maxIdleConn = 5;

    private int batchSize = 500;

    private int maxPointBatches = 256;

    private SchemaPolicy schemaPolicy;

    private CodecType codecType;

    private int queryChunkSize = Consts.DEFAULT_CHUNK_SIZE;

    private String defaultDatabase = Consts.DEFAULT_DB;

    public ClientOptions(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getNumBatchThreads() {
        return numBatchThreads;
    }

    public static Builder newBuilder(String url) {
        return new Builder(url);
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    public int getRequestTimeoutMs() {
        return this.requestTimeoutMs;
    }

    public long getRetryBackoffMs() {
        return this.retryBackoffMs;
    }

    public long getConnectTimeoutMs() {
        return this.connectTimeoutMs;
    }

    public long getMaxWaitTimeMs() {
        return this.maxWaitTimeMs;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public int getMaxPointBatches() {
        return this.maxPointBatches;
    }

    public SchemaPolicy getSchemaPolicy() {
        return this.schemaPolicy;
    }

    public CodecType getCodecType() {
        return this.codecType;
    }

    public int getQueryChunkSize() {
        return queryChunkSize;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public int getKeepAliveMs() {
        return keepAliveMs;
    }

    public int getMaxIdleConn() {
        return maxIdleConn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientOptions)) {
            return false;
        }
        ClientOptions options = (ClientOptions) o;
        return getNumBatchThreads() == options.getNumBatchThreads() &&
                getConnectTimeoutMs() == options.getConnectTimeoutMs() &&
                getMaxRetries() == options.getMaxRetries() &&
                getRequestTimeoutMs() == options.getRequestTimeoutMs() &&
                getRetryBackoffMs() == options.getRetryBackoffMs() &&
                getMaxWaitTimeMs() == options.getMaxWaitTimeMs() &&
                getKeepAliveMs() == options.getKeepAliveMs() &&
                getMaxIdleConn() == options.getMaxIdleConn() &&
                getBatchSize() == options.getBatchSize() &&
                getMaxPointBatches() == options.getMaxPointBatches() &&
                getQueryChunkSize() == options.getQueryChunkSize() &&
                Objects.equals(getUrl(), options.getUrl()) &&
                Objects.equals(getUsername(), options.getUsername()) &&
                Objects.equals(getPassword(), options.getPassword()) &&
                getSchemaPolicy() == options.getSchemaPolicy() &&
                getCodecType() == options.getCodecType() &&
                Objects.equals(getDefaultDatabase(), options.getDefaultDatabase());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUrl(), getUsername(), getPassword(), getNumBatchThreads(), getConnectTimeoutMs(), getMaxRetries(), getRequestTimeoutMs(), getRetryBackoffMs(),
                getMaxWaitTimeMs(), getKeepAliveMs(), getMaxIdleConn(), getBatchSize(), getMaxPointBatches(), getSchemaPolicy(), getCodecType(), getQueryChunkSize(), getDefaultDatabase());
    }

    @Override
    public String toString() {
        return "ClientOptions{" +
                "url='" + url + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", numBatchThreads=" + numBatchThreads +
                ", connectTimeoutMs=" + connectTimeoutMs +
                ", maxRetries=" + maxRetries +
                ", requestTimeoutMs=" + requestTimeoutMs +
                ", retryBackoffMs=" + retryBackoffMs +
                ", maxWaitTimeMs=" + maxWaitTimeMs +
                ", keepAliveMs=" + keepAliveMs +
                ", maxIdleConn=" + maxIdleConn +
                ", batchSize=" + batchSize +
                ", maxPointBatches=" + maxPointBatches +
                ", schemaPolicy=" + schemaPolicy +
                ", codecType=" + codecType +
                ", queryChunkSize=" + queryChunkSize +
                ", defaultDatabase='" + defaultDatabase + '\'' +
                '}';
    }

    public static class Builder {

        private String url;

        private String username;

        private String password;

        private int numBatchThreads = 1;

        private int maxRetries = 3;

        private int requestTimeoutMs = 30_000;

        private long retryBackoffMs = 1000;

        private long connectTimeoutMs = 30_000;

        private long maxWaitTimeMs = 300;

        private int keepAliveMs = -1;

        private int maxIdleConn = 5;

        private int batchSize = 500;

        private int maxPointBatches = 256;

        private SchemaPolicy schemaPolicy = SchemaPolicy.STRONG;

        private CodecType codecType = CodecType.BINARY;

        private int queryChunkSize = Consts.DEFAULT_CHUNK_SIZE;

        private String defaultDatabase = Consts.DEFAULT_DB;

        public Builder(String url) {
            if (url == null || url.trim().isEmpty()) {
                throw new IllegalArgumentException("The url must not be empty!");
            }
            this.url = url;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setNumBatchThreads(int numBatchThreads) {
            this.numBatchThreads = numBatchThreads;
            return this;
        }

        public Builder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder setRetryBackoffMs(int retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder setRequestTimeoutMs(int requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        public Builder setConnectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder setMaxWaitTimeMs(int maxWaitTimeMs) {
            this.maxWaitTimeMs = maxWaitTimeMs;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setMaxPointBatches(int maxPointBatches) {
            this.maxPointBatches = maxPointBatches;
            return this;
        }

        public Builder setSchemaPolicy(SchemaPolicy schemaPolicy) {
            Objects.requireNonNull(schemaPolicy);
            this.schemaPolicy = schemaPolicy;
            return this;
        }

//        public Builder setCodecType(CodecType codecType) {
//            Objects.requireNonNull(codecType);
//            this.codecType = codecType;
//            return this;
//        }

        public Builder setQueryChunkSize(int queryChunkSize) {
            this.queryChunkSize = queryChunkSize;
            return this;
        }

        public Builder setDefaultDatabase(String defaultDatabase) {
            Objects.requireNonNull(defaultDatabase);
            this.defaultDatabase = defaultDatabase;
            return this;
        }

        public Builder setConnectionPool(int keepAliveMs, int maxIdleConn) {
            this.keepAliveMs = keepAliveMs;
            this.maxIdleConn = maxIdleConn;
            return this;
        }

        public ClientOptions build() {
            ClientOptions options = new ClientOptions(url, username, password);
            options.numBatchThreads = numBatchThreads;
            options.maxRetries = maxRetries;
            options.retryBackoffMs = retryBackoffMs;
            options.requestTimeoutMs = requestTimeoutMs;
            options.connectTimeoutMs = connectTimeoutMs;
            options.maxWaitTimeMs = maxWaitTimeMs;
            options.batchSize = batchSize;
            options.maxPointBatches = maxPointBatches;
            options.schemaPolicy = schemaPolicy;
            options.codecType = codecType;
            options.queryChunkSize = queryChunkSize;
            options.defaultDatabase = defaultDatabase;
            options.keepAliveMs = keepAliveMs;
            options.maxIdleConn = maxIdleConn;
            return options;
        }
    }
}
