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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * @author jianhong.hjh
 */
public class Record {
    private final String table;

    private final long timestamp;

    private final Map<String, String> tags;

    private final Map<String, Field> fields;

    Record(String table, long timestamp,
            Map<String, String> tags, Map<String, Field> fields) {
        this.table = table;
        this.timestamp = timestamp;
        this.tags = tags;
        this.fields = fields;
    }

    public String getTable() {
        return table;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @JSONField(serialize = false)
    public Map<String, Field> getFields() {
        return fields;
    }

    @JSONField(serialize =  true, name = "fields")
    public Map<String, Object> getUnwrapFields() {
        Map<String, Object> result = new HashMap<>(fields.size());
        for (Map.Entry<String, Field> entry : fields.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getValue());
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Record)) {
            return false;
        }
        Record record = (Record) o;
        return getTimestamp() == record.getTimestamp() &&
                Objects.equals(getTable(), record.getTable()) &&
                Objects.equals(getTags(), record.getTags()) &&
                Objects.equals(getFields(), record.getFields());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTable(), getTimestamp(), getTags(), getFields());
    }

    @Override
    public String toString() {
        return "Record{" +
                "table='" + table + '\'' +
                ", timestamp=" + timestamp +
                ", tags=" + tags +
                ", fields=" + fields +
                '}';
    }

    public static Builder table(String tableName) {
        return new Builder(tableName);
    }

    public static final class Builder {

        private String tableName;

        private long timestamp;

        private Map<String, String> tags = new TreeMap<>();

        private Map<String, Field> fields = new HashMap<>();

        public Builder(String tableName) {
            this.tableName = tableName;
        }


        public Builder time(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder addTag(String tagName, String tagValue) {
            return tag(tagName, tagValue);
        }

        public Builder addTag(Map<String, String> tags) {
            return tag(tags);
        }

        public Builder tag(String tagName, String tagValue) {
            Objects.requireNonNull(tagName);
            Objects.requireNonNull(tagValue);
            if (!tagName.isEmpty()) {
                this.tags.put(tagName, tagValue); 
            }
            return this;
        }

        public Builder tag(Map<String, String> tags) {
            Objects.requireNonNull(tags);
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                tag(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public Builder addField(final String field, final boolean value) {
            fields.put(field, Field.of(field, value));
            return this;
        }

        public Builder addField(final String field, final long value) {
            fields.put(field, Field.of(field, value));
            return this;
        }

        public Builder addField(final String field, final double value) {
            fields.put(field, Field.of(field, value));
            return this;
        }

//        public Builder addField(final String field, final int value) {
//            fields.put(field, Field.of(field, value));
//            return this;
//        }

        public Builder addField(final String field, final float value) {
            fields.put(field, Field.of(field, value));
            return this;
        }

//        public Builder addField(final String field, final short value) {
//            fields.put(field, Field.of(field, value));
//            return this;
//        }

        public Builder addField(final String field, final String value) {
            Objects.requireNonNull(value, "value");

            fields.put(field, Field.of(field, value));
            return this;
        }

//        public Builder addField(final String field, final byte[] value) {
//            Objects.requireNonNull(value, "value");
//
//            fields.put(field, Field.of(field, value));
//            return this;
//        }

        public Builder addField(final Field field) {
            Objects.requireNonNull(field, "field");

            fields.put(field.getName(), field);
            return this;
        }

        public boolean hasFields() {
            return fields.size() > 0;
        }

        public Record build() {
            return build(true);
        }

        public Record build(boolean check) {
            if (check) {
                checkRecord();
            }
            return new Record(this.tableName, this.timestamp,
                    this.tags, this.fields);
        }

        private void checkRecord() {
            if (this.tableName == null || tableName.length() == 0) {
                throw new IllegalArgumentException("The tableName can't be empty");
            }

            if (this.fields == null || this.fields.isEmpty()) {
                throw new IllegalArgumentException("The fields can't be empty");
            }

            if (timestamp <= 0) {
                throw new IllegalArgumentException("The timestamp must be set and bigger than zero");
            }
            for (int i = 0; i < tableName.length(); i++) {
                final char c = tableName.charAt(i);
                if (!checkChar(c)) {
                    throw new IllegalArgumentException("There is an invalid character in tableName. the char is '" + c + "'");
                }
            }
            if (tags == null || tags.isEmpty()) {
                return;
            }
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String tagkey = entry.getKey();
                String tagvalue = entry.getValue();

                for (int i = 0; i < tagkey.length(); i++) {
                    final char c = tagkey.charAt(i);
                    if (!checkChar(c)) {
                        throw new IllegalArgumentException("There is an invalid character in tag key. the tag key is + "
                                + tagkey + ", the char is '" + c + "'");
                    }
                }

                for (int i = 0; i < tagvalue.length(); i++) {
                    final char c = tagvalue.charAt(i);
                    if (!checkChar(c)) {
                        throw new IllegalArgumentException("There is an invalid character in tag value. the tag is + <"
                                + tagkey + ":" + tagvalue + "> , the char is '" + c + "'");
                    }
                }
            }
        }

        public static boolean checkChar(char c) {
            return c >= 0x20 && c != 0x7F;
        }
    }
}
