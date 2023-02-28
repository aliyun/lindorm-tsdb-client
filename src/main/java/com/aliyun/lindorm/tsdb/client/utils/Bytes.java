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
package com.aliyun.lindorm.tsdb.client.utils;

import java.nio.charset.StandardCharsets;

public class Bytes {

    public static byte[] toBytes(int value) {
        return new byte[]{(byte) ((value >> 24) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) (value & 0xFF)};
    }

    public static byte[] toBytes(double value) {
        return toBytes(Double.doubleToLongBits(value));
    }

    public static byte[] toBytes(boolean value) {
        return value ? new byte[]{1} : new byte[]{0};
    }

    public static byte[] toBytes(long value) {
        return new byte[]{(byte) ((int) (value >> 56)),
                (byte) ((int) (value >> 48)),
                (byte) ((int) (value >> 40)),
                (byte) ((int) (value >> 32)),
                (byte) ((int) (value >> 24)),
                (byte) ((int) (value >> 16)),
                (byte) ((int) (value >> 8)),
                (byte) ((int) value)};
    }

    public static String toString(byte[] bytes) {
        return toStringBinary(bytes, 0, bytes.length);
    }

    public static byte[] toUTF8Bytes(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String toStringBinary(byte[] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        if (off >= b.length) {
            return result.toString();
        } else {
            if (off + len > b.length) {
                len = b.length - off;
            }

            for (int i = off; i < off + len; ++i) {
                int ch = b[i] & 255;
                if ((ch < 48 || ch > 57) && (ch < 65 || ch > 90) && (ch < 97 || ch > 122) && " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) < 0) {
                    result.append(String.format("\\x%02X", ch));
                } else {
                    result.append((char) ch);
                }
            }

            return result.toString();
        }
    }
}
