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
import com.aliyun.lindorm.tsdb.client.utils.Bytes;

import java.util.Objects;

public class Field {
    private String name;
    private DataType dataType;
    private Object value;

    public Field(String name, Object value, DataType type) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        Objects.requireNonNull(type);
        this.name = name;
        this.dataType = type;
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        Objects.requireNonNull(value);
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JSONField(serialize =  false)
    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Field)) {
            return false;
        }
        Field field = (Field) o;
        return Objects.equals(getName(), field.getName()) &&
                Objects.equals(getValue(), field.getValue()) &&
                getDataType() == field.getDataType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getValue(), getDataType());
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + getName() + '\'' +
                ", value=" + value +
                ", type=" + getDataType() +
                '}';
    }

    @JSONField(serialize =  false)
    public long getLong() {
        return ((Number) value).longValue();
    }

    @JSONField(serialize =  false)
    public int getInt() {
        return (int) value;
    }

    @JSONField(serialize =  false)
    public boolean getBoolean() {
        return (boolean) value;
    }

    @JSONField(serialize =  false)
    public double getDouble() {
        return ((Number) value).doubleValue();
    }

    @JSONField(serialize =  false)
    public String getString() {
        return (String) value;
    }

    @JSONField(serialize =  false)
    public byte[] getBytes() {
        return (byte[]) value;
    }

    public byte[] stringValueBytes() {
        if (value instanceof String) {
            return Bytes.toUTF8Bytes((String) value);
        } else if (value instanceof byte[]) {
            return (byte[]) value;
        } else {
            throw new IllegalStateException("unsupported data type: " + getDataType());
        }
    }

    public byte[] valueBytes() {
        switch (getDataType()) {
//            case INTEGER:
//                return Bytes.toBytes(getInt());
            case LONG:
                return Bytes.toBytes(getLong());
            case DOUBLE:
                return Bytes.toBytes(getDouble());
//            case BYTES:
            case STRING:
                return stringValueBytes();
            case BOOL:
                return Bytes.toBytes(getBoolean());
            default:
                throw new IllegalStateException("unsupported data type: " + getDataType());
        }
    }

    public static Field of(String name, Object value, DataType dataType) {
        return new Field(name, value, dataType);
    }

    public static Field of(String name, long value) {
        return new Field(name, value, DataType.LONG);
    }

//    public static Field of(String name, int value) {
//        return new Field(name, value, DataType.INTEGER);
//    }

    public static Field of(String name, double value) {
        return new Field(name, value, DataType.DOUBLE);
    }

    public static Field of(String name, boolean value) {
        return new Field(name, value, DataType.BOOL);
    }

//    public static Field of(String name, byte[] value) {
//        return new Field(name, value, DataType.BYTES);
//    }

    public static Field of(String name, String value) {
        return new Field(name, value, DataType.STRING);
    }
}
