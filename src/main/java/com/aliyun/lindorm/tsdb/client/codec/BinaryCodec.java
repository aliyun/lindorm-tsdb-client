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
package com.aliyun.lindorm.tsdb.client.codec;

import com.aliyun.lindorm.tsdb.client.model.Field;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.utils.Dictionary;
import com.aliyun.lindorm.tsdb.client.utils.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author jianhong.hjh
 */
public class BinaryCodec implements WriteCodec {

    private static final ThreadLocal<UnsafeBuffer> THREAD_LOCAL = ThreadLocal.withInitial(() -> new UnsafeBuffer());

    @Override
    public byte[] encode(List<Record> records) {
        UnsafeBuffer unsafeBuffer = THREAD_LOCAL.get();
        unsafeBuffer.clear();

        // version
        unsafeBuffer.writeByte(1);
        // compress
        unsafeBuffer.writeByte(0);

        unsafeBuffer.writeUnsignedVarInt(records.size());
        int id = 0;
        Dictionary dictionary = new Dictionary();
        for (Record record : records) {
            String table = record.getTable();
            id = dictionary.getOrCreate(table);
            unsafeBuffer.writeUnsignedVarInt(id);

            // encode tags
            Map<String, String> tags = record.getTags();
            int tagSize = tags.size();
            unsafeBuffer.writeUnsignedVarInt(tagSize);
            for (Map.Entry<String, String> entry: tags.entrySet()) {
                id = dictionary.getOrCreate(entry.getKey());
                unsafeBuffer.writeUnsignedVarInt(id);
                id = dictionary.getOrCreate(entry.getValue());
                unsafeBuffer.writeUnsignedVarInt(id);
            }
            long timestamp = record.getTimestamp();
            // encode timestamp
            unsafeBuffer.writeUnsignedVarLong(timestamp);

            // encode fields
            Map<String, Field> fields = record.getFields();
            unsafeBuffer.writeUnsignedVarInt(fields.size());
            for (Map.Entry<String, Field> entry : record.getFields().entrySet()) {
                Field field = entry.getValue();
                // encode field name
                id = dictionary.getOrCreate(field.getName());
                unsafeBuffer.writeUnsignedVarInt(id);
                // encode field data type
                unsafeBuffer.writeByte(field.getDataType().getId());

                // encode field data value
                byte[] val = field.valueBytes();
                unsafeBuffer.writeUnsignedVarInt(val.length);
                unsafeBuffer.writeBytes(val);
            }
        }

        // encode dict
        int dictPos = unsafeBuffer.position();
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(dictionary.entrySet());
        Collections.sort(entries, Comparator.comparingInt(Map.Entry::getValue));
        unsafeBuffer.writeUnsignedVarInt(entries.size());
        for (Map.Entry<String, Integer> entry : entries) {
            unsafeBuffer.writeString(entry.getKey());
        }
        unsafeBuffer.writeInt(dictPos);

        return unsafeBuffer.safeBuffer().array();
    }
}
