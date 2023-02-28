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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UnsafeBuffer {

    /**
     * Maximum encoded size of 32-bit positive integers (in bytes)
     */
    public static final int MAX_VAR_INT_SIZE = 5;

    /**
     * maximum encoded size of 64-bit longs, and negative 32-bit ints (in bytes)
     */
    public static final int MAX_VAR_LONG_SIZE = 10;

    // 4 MiB page
    private static final int CALCULATE_THRESHOLD = 4 * 1024 * 1024;

    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private static final int DEFAULT_MAX_CAPACITY = 16 * 1024 * 1024;

    private ByteBuffer buffer;

    private int maxCapacity = DEFAULT_MAX_CAPACITY;

    public UnsafeBuffer() {
        this(false);
    }

    public UnsafeBuffer(boolean direct) {
        this(direct, DEFAULT_INITIAL_CAPACITY);
    }

    public UnsafeBuffer(boolean direct, int initialCapacity) {
        if (direct) {
            this.buffer = ByteBuffer.allocateDirect(initialCapacity);
        } else {
            this.buffer = ByteBuffer.allocate(initialCapacity);
        }
    }

    public UnsafeBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public UnsafeBuffer(byte[] array) {
        this(array, 0, array.length);
    }

    public UnsafeBuffer(byte[] array, int offset, int len) {
        this.buffer = ByteBuffer.wrap(array, offset, len);
    }

    public void wrap(byte[] array) {
        wrap(array, 0, array.length);
    }

    public void wrap(byte[] array, int offset, int len) {
        this.buffer = ByteBuffer.wrap(array, offset, len);
    }

    public void wrap(ByteBuffer payload) {
        this.buffer = payload;
    }

    public void writeByte(byte value) {
        ensureWritable(1);
        this.buffer.put(value);
    }

    public void writeByte(int value) {
        ensureWritable(1);
        this.buffer.put((byte) (value & 0xFF));
    }


    public void writeBytes(byte[] bytes) {
        writeBytes(bytes, 0, bytes.length);
    }

    public void writeBytes(byte[] bytes, int offset, int len) {
        ensureWritable(len);
        System.arraycopy(bytes, offset, buffer.array(), buffer.position(), len);
        this.buffer.position(this.buffer.position() + len);
    }

    public void writeSignedVarInt(int value) {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        writeUnsignedVarInt((value << 1) ^ (value >> 31));
    }

    public void writeUnsignedVarInt(int value) {
        ensureWritable(MAX_VAR_INT_SIZE);
        while ((value & 0xFF_FF_FF_80) != 0) {
            writeByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        writeByte((byte) (value & 0x7F));
    }

    public void writeSignedVarLong(long value) {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        writeUnsignedVarLong((value << 1) ^ (value >> 63));
    }

    public void writeUnsignedVarLong(long value) {
        ensureWritable(MAX_VAR_LONG_SIZE);
        while ((value & 0xFF_FF_FF_FF_FF_FF_FF_80L) != 0) {
            writeByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        writeByte((byte) (value & 0x7F));
    }

    public long writeString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        this.writeUnsignedVarInt(bytes.length);
        this.writeBytes(bytes, 0, bytes.length);
        return bytes.length;
    }

    public byte readByte() {
        return this.buffer.get();
    }

    public int readBytes(byte[] bytes) {
        if (buffer.remaining() >= bytes.length) {
            buffer.get(bytes);
            return bytes.length;
        }
        int numReads = buffer.remaining();
        buffer.get(bytes, 0, numReads);
        return numReads;
    }

    public UnsafeBuffer slice(int numBytes) {
        ByteBuffer byteBuffer = buffer.slice();
        byteBuffer.limit(numBytes);
        return new UnsafeBuffer(byteBuffer);
    }

    public void skipBytes(int numBytes) {
        this.buffer.position(buffer.position() + numBytes);
    }

    public int readSignedVarInt() {
        int raw = readUnsignedVarInt();
        // This undoes the trick in writeSignedVarInt()
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1 << 31));

    }

    public int readUnsignedVarInt() {
        int value = 0;
        int i = 0;
        int b;
        while (((b = readByte()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    private byte[] cache = new byte[1024];

    public String readString() {
        int len = this.readUnsignedVarInt();
        byte[] bytes;
        if (len > this.cache.length) {
            bytes = new byte[len];
        } else {
            bytes = this.cache;
        }
        this.buffer.get(bytes, 0, len);
        return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }

    public long readSignedVarLong() {
        long raw = readUnsignedVarLong();
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    public long readUnsignedVarLong() {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = readByte()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    public int remaining() {
        return this.buffer.remaining();
    }

    public boolean hasRemaining() {
        return this.buffer.hasRemaining();
    }

    public boolean isDirect() {
        return this.buffer.isDirect();
    }

    public void clear() {
        this.buffer.clear();
    }

    public ByteBuffer unsafeBuffer() {
        ByteBuffer newBuf = this.buffer.duplicate();
        newBuf.flip();
        return newBuf;
    }

    public ByteBuffer safeBuffer() {
        int len = this.buffer.position();
        ByteBuffer newBuf = ByteBuffer.allocate(len);
        System.arraycopy(this.buffer.array(), 0, newBuf.array(), 0, len);
        newBuf.position(0).limit(len);
        return newBuf;
    }

    private void ensureWritable(int minWritableBytes) {
        if (minWritableBytes <= remaining()) {
            return;
        }
        int writeIndex = this.buffer.position();
        // Normalize the current capacity to the power of 2.
        int minNewCapacity = writeIndex + minWritableBytes;
        int newCapacity = calculateNewCapacity(minNewCapacity);
        // Adjust to the new capacity.
        capacity(newCapacity);
    }

    private void capacity(int newCapacity) {
        ByteBuffer newBuf;
        int len = this.buffer.position();
        newBuf = ByteBuffer.allocate(newCapacity);
        System.arraycopy(this.buffer.array(), 0, newBuf.array(), 0, len);
        newBuf.position(len);
        this.buffer = newBuf;
    }

    private int calculateNewCapacity(int minNewCapacity) {
        // 4 MiB page
        final int threshold = CALCULATE_THRESHOLD;
        if (minNewCapacity == threshold) {
            return threshold;
        }

        // If over threshold, do not double but just increase by threshold.
        if (minNewCapacity > threshold) {
            return (minNewCapacity / threshold + 1) * threshold;
        }

        // Not over threshold. Double up to 4 MiB, starting from 64.
        int newCapacity = 64;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        return Math.min(newCapacity, maxCapacity);
    }

    public int position() {
        return this.buffer.position();
    }

    public void position(int position) {
        this.buffer.position(position);
    }

    public void writeLong(long value) {
        ensureWritable(Long.BYTES);
        this.buffer.putLong(value);
    }

    public void writeDouble(double value) {
        ensureWritable(Double.BYTES);
        this.buffer.putDouble(value);
    }

    public long readLong() {
        return this.buffer.getLong();
    }

    public double readDouble() {
        return this.buffer.getDouble();
    }

    public long readLong(int index) {
        return this.buffer.getLong(index);
    }

    public double readDouble(int index) {
        return this.buffer.getDouble(index);
    }

    public void writeLong(int index, long value) {
        this.buffer.putLong(index, value);
    }

    public void writeDouble(int index, double value) {
        this.buffer.putDouble(index, value);
    }

    public int readInt() {
        return this.buffer.getInt();
    }

    public int readInt(int index) {
        return this.buffer.getInt(index);
    }

    public void writeInt(int value) {
        ensureWritable(Integer.BYTES);
        this.buffer.putInt(value);
    }
}
