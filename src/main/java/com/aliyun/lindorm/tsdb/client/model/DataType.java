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

/**
 * 数据类型的ID不可随意修改，必须和服务端保持一致
 */
public enum DataType {

    // INTEGER((byte) 0x00, 4),

    LONG((byte) 0x01, 8),

    DOUBLE((byte) 0x02, 8),

   // BYTES((byte) 0x03, -1),

    BOOL((byte) 0x04, 1),

    STRING((byte) 0x08, -1);

    private final byte id;
    private final int length;

    DataType(byte id, int length) {
        this.id = id;
        this.length = length;
    }

    public byte getId() {
        return id;
    }

    public int getLength() {
        return length;
    }

    public static DataType of(byte id) {
        if (id == 0x01) {
            return LONG;
//        } else if (id == 0x00) {
//            return INTEGER;
        } else if (id == 0x02) {
            return DOUBLE;
//        } else if (id == 0x03) {
//            return BYTES;
        } else if (id == 0x04) {
            return BOOL;
        } else if (id == 0x08) {
            return STRING;
//        } else if (id == 0x09) {
//            throw new IllegalStateException("should not be data type LATEST" );
//        } else if (id == 0x0a) {
//            throw new IllegalStateException("should not be data type ALL" );
        }
        throw new IllegalStateException("unknown data type " + id);
    }
}
