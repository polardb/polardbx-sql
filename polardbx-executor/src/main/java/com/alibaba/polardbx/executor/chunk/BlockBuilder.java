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

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.datatype.Decimal;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Block Builder
 *
 *
 */
public interface BlockBuilder extends Block {

    Block build();

    default void writeByte(byte value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeShort(short value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeInt(int value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeLong(long value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeDouble(double value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeFloat(float value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeTimestamp(Timestamp value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeDate(Date value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeTime(Time value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeDatetimeRawLong(long val) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeString(String value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeDecimal(Decimal value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeBigInteger(BigInteger value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeBoolean(boolean value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeByteArray(byte[] value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeByteArray(byte[] value, int offset, int length) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeBlob(Blob value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void writeClob(Clob value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void ensureCapacity(int capacity) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write an object
     * <p>
     * Just to keep compatible with legacy code. Do NOT use this method as far as possible.
     */
    void writeObject(Object value);

    /**
     * Appends a null value to the block.
     */
    void appendNull();

    /**
     * Creates a new block builder of the same type with current block size as initial capacity
     */
    BlockBuilder newBlockBuilder();
}
