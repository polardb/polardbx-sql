/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.json;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.optimizer.json.exception.JsonKeyTooBigException;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;
import com.alibaba.polardbx.optimizer.json.exception.JsonTooLargeException;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonBinaryUtil {
    private final static long INT_MIN16 = (~0x7FFF);
    private final static long INT_MAX16 = 0x7FFF;
    private final static long INT_MIN32 = (~0x7FFFFFFFL);
    private final static long INT_MAX32 = 0x7FFFFFFFL;

    private final static int UINT_MAX16 = 0xFFFF;

    private static final byte JSONB_TYPE_SMALL_OBJECT = 0x0;
    private static final byte JSONB_TYPE_LARGE_OBJECT = 0x1;
    private static final byte JSONB_TYPE_SMALL_ARRAY = 0x2;
    private static final byte JSONB_TYPE_LARGE_ARRAY = 0x3;
    private static final byte JSONB_TYPE_LITERAL = 0x4;
    private static final byte JSONB_TYPE_INT16 = 0x5;
    private static final byte JSONB_TYPE_UINT16 = 0x6;
    private static final byte JSONB_TYPE_INT32 = 0x7;
    private static final byte JSONB_TYPE_UINT32 = 0x8;
    private static final byte JSONB_TYPE_INT64 = 0x9;
    private static final byte JSONB_TYPE_UINT64 = 0xA;
    private static final byte JSONB_TYPE_DOUBLE = 0xB;
    private static final byte JSONB_TYPE_STRING = 0xC;
    private static final byte JSONB_TYPE_OPAQUE = 0xF;
    private static final byte JSONB_NULL_LITERAL = 0x0;
    private static final byte JSONB_TRUE_LITERAL = 0x1;
    private static final byte JSONB_FALSE_LITERAL = 0x2;

    private static final int SMALL_OFFSET_SIZE = 2;
    private static final int LARGE_OFFSET_SIZE = 4;
    private static final int KEY_ENTRY_SIZE_SMALL = 2 + SMALL_OFFSET_SIZE;
    private static final int KEY_ENTRY_SIZE_LARGE = 2 + LARGE_OFFSET_SIZE;
    private static final int VALUE_ENTRY_SIZE_SMALL = 1 + SMALL_OFFSET_SIZE;
    private static final int VALUE_ENTRY_SIZE_LARGE = 1 + LARGE_OFFSET_SIZE;

    /**
     * JSON嵌套最大深度
     */
    private final static int JSON_MAX_DEPTH = 100;

    public static byte[] toBytes(Object jsonObj) throws JsonTooLargeException {
        return toBytes(jsonObj, 128);
    }

    public static byte[] toBytes(Object jsonObj, int initLen) throws JsonTooLargeException {
        WriteOnlyByteBuffer byteBuffer = new WriteOnlyByteBuffer(initLen);

        OffsetRecorder offsetRecorder = new OffsetRecorder();
        innerToBytes(jsonObj, byteBuffer, offsetRecorder, 1, false);

        return byteBuffer.toBytes();
    }

    private static void innerToBytes(Object jsonObj, WriteOnlyByteBuffer buffer,
                                     OffsetRecorder offsetRecorder, int depth,
                                     boolean isParentSmall)
        throws JsonTooLargeException {
        checkJsonDepth(depth);
        if (jsonObj instanceof JSONObject) {
            serializeJsonObject((JSONObject) jsonObj, buffer, offsetRecorder, depth + 1, isParentSmall);
        } else if (jsonObj instanceof JSONArray) {
            serializeJsonArray((JSONArray) jsonObj, buffer, offsetRecorder, depth + 1, isParentSmall);
        } else {
            serializeJsonScalar(jsonObj, buffer, offsetRecorder, depth + 1);
        }
    }

    private static void serializeJsonScalar(Object jsonObj, WriteOnlyByteBuffer buffer,
                                            OffsetRecorder offsetRecorder, int depth) {
        checkJsonDepth(depth);
        byte scalarType = getTypeByObject(jsonObj);
        long value;
        switch (scalarType) {
        case JSONB_TYPE_STRING:
            buffer.put(offsetRecorder.typePos, scalarType);
            byte[] bytes = ((String) jsonObj).getBytes(StandardCharsets.UTF_8);
            // 先字符串长度
            int len = bytes.length;
            do {
                byte ch = (byte) (len & 0x7F);
                len >>= 7;
                if (len != 0) {
                    ch |= 0x80;
                }
                buffer.append(ch);
            } while (len != 0);
            // 再字符串byte数组
            buffer.append(bytes);
            break;
        case JSONB_TYPE_INT16:
            value = ((Number) jsonObj).longValue();
            buffer.put(offsetRecorder.typePos, scalarType);
            buffer.appendInt16(value);
            break;
        case JSONB_TYPE_INT32:
            value = ((Number) jsonObj).longValue();
            buffer.put(offsetRecorder.typePos, scalarType);
            buffer.appendInt32(value);
            break;
        case JSONB_TYPE_INT64:
            value = ((Number) jsonObj).longValue();
            buffer.put(offsetRecorder.typePos, scalarType);
            buffer.appendInt64(value);
            break;
        case JSONB_TYPE_DOUBLE:
            value = Double.doubleToRawLongBits((Double) jsonObj);
            buffer.put(offsetRecorder.typePos, scalarType);
            buffer.appendInt64(value);
            break;
        case JSONB_TRUE_LITERAL:
        case JSONB_FALSE_LITERAL:
        case JSONB_NULL_LITERAL:
            buffer.put(offsetRecorder.typePos, JSONB_TYPE_LITERAL);
            // 实际数据写入
            buffer.append(scalarType);
            break;
        default:
            throw new UnsupportedOperationException("Unsupported JSON scalar type: " + scalarType);
        }
    }

    private static void serializeJsonObject(JSONObject jsonObj, WriteOnlyByteBuffer buffer,
                                            OffsetRecorder offsetRecorder, int depth,
                                            boolean isParentSmall) {
        checkJsonDepth(depth);
        final int curLen = buffer.length;
        try {
            serializeObject(jsonObj, buffer, offsetRecorder, depth, false);
        } catch (JsonTooLargeException e) {
            if (isParentSmall) {
                // 父节点也需要调整为大存储格式
                // 故交给上层处理
                throw e;
            } else {
                buffer.reset(curLen);
                serializeObject(jsonObj, buffer, offsetRecorder, depth, true);
            }
        }
    }

    private static void serializeObject(JSONObject jsonObj, WriteOnlyByteBuffer buffer,
                                        OffsetRecorder offsetRecorder, int depth, boolean large) {
        if (!large && jsonObj.size() > UINT_MAX16) {
            throw new JsonTooLargeException();
        }
        buffer.put(offsetRecorder.typePos, large ? JSONB_TYPE_LARGE_OBJECT : JSONB_TYPE_SMALL_OBJECT);
        final int curStartPos = buffer.length;
        offsetRecorder.startPos = curStartPos;
        int size = jsonObj.size();
        buffer.appendInt(size, large ? 4 : 2);

        // 预留存储整个对象内存大小的位置
        int objMemSizePos = buffer.length;
        buffer.reserveBytes(large ? 4 : 2);

        final int keyEntrySize = large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
        final int valueEntrySize = large ? VALUE_ENTRY_SIZE_SMALL : VALUE_ENTRY_SIZE_SMALL;

        offsetRecorder.offsetFromStart =
            buffer.length + size * (keyEntrySize + valueEntrySize) - offsetRecorder.startPos;

        // 预留key entry的空间
        int keyEntryStart = buffer.length;
        buffer.reserveBytes((2 + (large ? 4 : 2)) * size);

        // 预留value的空间
        final int valueEntryStart = buffer.length;
        buffer.reserveBytes(size * valueEntrySize);

        // 先写入key的offset和长度
        for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {

            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            int len = keyBytes.length;
            if (len > UINT_MAX16) {
                // 限制键的byte长度只能是2字节
                throw new JsonKeyTooBigException();
            }
            if (!large && isTooBigForJson(offsetRecorder.offsetFromStart)) {
                throw new JsonTooLargeException();
            }
            putOffsetOrSize(buffer, keyEntryStart, offsetRecorder.offsetFromStart, large);
            keyEntryStart += large ? 4 : 2;
            buffer.putInt16(keyEntryStart, len);
            keyEntryStart += 2;
            // 先写入key 避免getBytes重复计算
            buffer.append(keyBytes);
            offsetRecorder.offsetFromStart += len;
        }

        // 最后写入value
        offsetRecorder.typePos = valueEntryStart;
        for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
            // 非inline存储位置的起始
            offsetRecorder.offsetFromStart = buffer.length - offsetRecorder.startPos;
            addEntryAndValue(entry.getValue(), buffer, offsetRecorder, large, depth);
            offsetRecorder.typePos += valueEntrySize;
        }
        buffer.putInt(objMemSizePos, buffer.length - curStartPos, large ? 4 : 2);
    }

    private static void serializeJsonArray(JSONArray jsonArr, WriteOnlyByteBuffer buffer,
                                           OffsetRecorder offsetRecorder, int depth,
                                           boolean isParentSmall) throws JsonTooLargeException {
        checkJsonDepth(depth);
        final int curLen = buffer.length;
        try {
            serializeArray(jsonArr, buffer, offsetRecorder, depth, false);
        } catch (JsonTooLargeException e) {
            if (isParentSmall) {
                // 父节点也需要调整为大存储格式
                // 故交给上层处理
                throw e;
            } else {
                buffer.reset(curLen);
                serializeArray(jsonArr, buffer, offsetRecorder, depth, true);
            }
        }
    }

    private static void serializeArray(JSONArray jsonArr, WriteOnlyByteBuffer buffer,
                                       OffsetRecorder offsetRecorder, int depth, boolean large)
        throws JsonTooLargeException {
        if (!large && isTooBigForJson(jsonArr.size())) {
            throw new JsonTooLargeException();
        }
        buffer.put(offsetRecorder.typePos,
            large ? JSONB_TYPE_LARGE_ARRAY : JSONB_TYPE_SMALL_ARRAY);
        final int curStartPos = buffer.length;
        offsetRecorder.startPos = curStartPos;
        int arrLen = jsonArr.size();
        buffer.appendInt(arrLen, large ? 4 : 2);

        // 预留存储整个数组内存大小的位置
        int arrayMemSizePos = buffer.length;
        buffer.reserveBytes(large ? 4 : 2);

        int arrayEntryStartPos = buffer.length;
        // 预留entry大小
        int entrySize = large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL;
        buffer.reserveBytes(arrLen * entrySize);

        offsetRecorder.typePos = arrayEntryStartPos;
        for (Object obj : jsonArr) {
            // 非inline存储位置的起始
            offsetRecorder.offsetFromStart = buffer.length - offsetRecorder.startPos;
            addEntryAndValue(obj, buffer, offsetRecorder, large, depth + 1);
            offsetRecorder.typePos += entrySize;
        }
        buffer.putInt(arrayMemSizePos, buffer.length - curStartPos, large ? 4 : 2);
    }

    private static void addEntryAndValue(Object obj, WriteOnlyByteBuffer buffer,
                                         OffsetRecorder offsetRecorder,
                                         boolean large, int depth) throws JsonTooLargeException {
        checkJsonDepth(depth);
        SmallEntry inlineEntry = new SmallEntry();
        if (shouldInlineValue(obj, large, inlineEntry)) {
            buffer.put(offsetRecorder.typePos, inlineEntry.type);
            putOffsetOrSize(buffer, offsetRecorder.typePos + 1, inlineEntry.value, large);
            return;
        }

        // 填入指针位置
        putOffsetOrSize(buffer, offsetRecorder.typePos + 1, offsetRecorder.offsetFromStart, large);
        // 写入实际数据
        innerToBytes(obj, buffer, offsetRecorder.copy(), depth, !large);
    }

    private static void putOffsetOrSize(WriteOnlyByteBuffer buffer, int pos, long value, boolean large) {
        if (large) {
            buffer.putInt32(pos, value);
        } else {
            buffer.putInt16(pos, value);
        }
    }

    private static boolean shouldInlineValue(Object obj, boolean large, SmallEntry inlineEntry) {
        if (obj == null) {
            inlineEntry.type = JSONB_TYPE_LITERAL;
            inlineEntry.value = JSONB_NULL_LITERAL;
            return true;
        }
        if (obj instanceof Boolean) {
            inlineEntry.type = JSONB_TYPE_LITERAL;
            if ((Boolean) obj) {
                inlineEntry.value = JSONB_TRUE_LITERAL;
            } else {
                inlineEntry.value = JSONB_FALSE_LITERAL;
            }

            return true;
        }
        if (obj instanceof Integer) {
            long value = ((Number) obj).longValue();
            boolean is16Bit;
            if ((is16Bit = can16BitHold(value)) || (large && can32BitHold(value))) {
                inlineEntry.value = (int) value;
                inlineEntry.type = is16Bit ? JSONB_TYPE_INT16 : JSONB_TYPE_INT32;
                return true;
            }
            return false;
        }
        return false;
    }

    private static byte getTypeByObject(Object o) {
        if (o instanceof JSONObject) {
            return JSONB_TYPE_SMALL_OBJECT;
        }
        if (o instanceof JSONArray) {
            return JSONB_TYPE_SMALL_ARRAY;
        }
        if (o instanceof String) {
            return JSONB_TYPE_STRING;
        }
        if (o instanceof Integer || o instanceof Long) {
            long value = ((Number) o).longValue();
            if (can16BitHold(value)) {
                return JSONB_TYPE_INT16;
            } else if (can32BitHold(value)) {
                return JSONB_TYPE_INT32;
            } else {
                return JSONB_TYPE_INT64;
            }
        }
        if (o instanceof Double) {
            return JSONB_TYPE_DOUBLE;
        }
        if (o instanceof Boolean) {
            if ((Boolean) o) {
                return JSONB_TRUE_LITERAL;
            } else {
                return JSONB_FALSE_LITERAL;
            }
        }
        if (o == null) {
            return JSONB_NULL_LITERAL;
        }

        throw new UnsupportedOperationException(o + " is not supported to "
            + "be serialized to binary format.");
    }

    /**
     * 如果长度大于UINT_MAX16, 则应尝试使用大存储格式
     */
    private static boolean isTooBigForJson(int size) {
        return size > UINT_MAX16;
    }

    /**
     * 数值能否用16bit表示
     */
    private static boolean can16BitHold(long number) {
        return number >= INT_MIN16 && number <= INT_MAX16;
    }

    /**
     * 数值能否用32bit表示
     */
    private static boolean can32BitHold(long number) {
        return number >= INT_MIN32 && number <= INT_MAX32;
    }

    private static void checkJsonDepth(int depth) {
        if (depth > JSON_MAX_DEPTH) {
            throw new JsonParserException("JSON exceeds max depth.");
        }
    }

    static class SmallEntry {
        byte type;
        int value;
    }

    static class OffsetRecorder {
        int typePos;
        int startPos;
        int offsetFromStart;

        OffsetRecorder copy() {
            OffsetRecorder copy = new OffsetRecorder();
            copy.typePos = this.typePos;
            copy.startPos = this.startPos;
            copy.offsetFromStart = this.offsetFromStart;
            return copy;
        }
    }

    /**
     * Not thread safe
     */
    static class WriteOnlyByteBuffer {
        byte[] buffer;
        int length;

        public WriteOnlyByteBuffer() {
            this(64);
        }

        public WriteOnlyByteBuffer(int initialCapacity) {
            buffer = new byte[initialCapacity];
            length = 0;
        }

        public void put(int pos, byte value) {
            ensureCapacity(pos);
            buffer[pos] = value;
            updateLength(pos);
        }

        public void append(byte value) {
            ensureCapacity(length);
            buffer[length] = value;
            updateLength(length);
        }

        private void ensureCapacity(int len) {
            if (buffer.length <= len) {
                int newSize = getAlignedSize(len + 1);
                byte[] tmp = new byte[newSize];
                System.arraycopy(buffer, 0, tmp, 0, buffer.length);
                buffer = tmp;
            }
        }

        /**
         * 更新写入的长度
         */
        private void updateLength(int pos) {
            if (pos >= length) {
                length = pos + 1;
            }
        }

        private int getAlignedSize(int pos) {
            final int align = 15; // 16 - 1
            return (pos + align) & ~align;
        }

        public void append(byte[] value) {
            put(length, value);
        }

        public void put(int pos, byte[] value) {
            int endPos = pos + value.length - 1;
            ensureCapacity(endPos);
            System.arraycopy(value, 0, buffer, pos, value.length);
            updateLength(endPos);
        }

        public void appendInt(long value, int bytes) {
            if (bytes == 2) {
                appendInt16(value);
            } else if (bytes == 4) {
                appendInt32(value);
            } else {
                throw new UnsupportedOperationException("Cannot append Int in bytes: " + bytes);
            }
        }

        public void appendInt16(long value) {
            putInt16(length, value);
        }

        public void appendInt32(long value) {
            putInt32(length, value);
        }

        public void appendInt64(long value) {
            putInt64(length, value);
        }

        public void putInt(int pos, long value, int bytes) {
            if (bytes == 2) {
                putInt16(pos, value);
            } else if (bytes == 4) {
                putInt32(pos, value);
            } else {
                throw new UnsupportedOperationException("Cannot put Int in bytes: " + bytes);
            }
        }

        public void putInt16(int pos, long value) {
            ensureCapacity(pos + 1);
            buffer[pos] = (byte) (0x00FF & value);
            buffer[pos + 1] = (byte) (0x00FF & (value >> 8));
            updateLength(pos + 1);
        }

        public void putInt32(int pos, long value) {
            ensureCapacity(pos + 3);
            buffer[pos] = (byte) (0x00FF & value);
            buffer[pos + 1] = (byte) (0x00FF & (value >> 8));
            buffer[pos + 2] = (byte) (0x00FF & (value >> 16));
            buffer[pos + 3] = (byte) (0x00FF & (value >> 24));
            updateLength(pos + 3);
        }

        public void putInt64(int pos, long value) {
            ensureCapacity(pos + 7);
            for (int i = 0; i < 8; i++) {
                buffer[pos + i] = (byte) (0x00FF & (value >> (i * 8)));
            }
            updateLength(pos + 7);
        }

        public byte[] toBytes() {
            byte[] res = new byte[length];
            System.arraycopy(buffer, 0, res, 0, length);
            return res;
        }

        public void reserveBytes(int len) {
            ensureCapacity(length + len - 1);
            updateLength(length + len - 1);
        }

        /**
         * 重置到下一个写入的位置
         */
        public void reset(int pos) {
            length = pos;
        }
    }
}
