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
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Block Builders
 *
 *
 */
public abstract class BlockBuilders {

    private static final int INITIAL_BLOCK_LEN = 4; // initial/max chunk size should be power of 2

    private static final int EXPECTED_STRING_LEN = 20;
    private static final int EXPECTED_BYTE_ARRAY_LEN = 50;

    public static BlockBuilder create(DataType type, ExecutionContext context) {
        return create(type, context, context.getBlockBuilderCapacity());
    }

    public static BlockBuilder create(DataType type, ExecutionContext context, int initialCapacity) {
        // Very special cases e.g. compound type
        if (type == null) {
            return new ObjectBlockBuilder(initialCapacity);
        }

        Class clazz = type.getDataClass();
        if (clazz == Integer.class) {
            return new IntegerBlockBuilder(initialCapacity);
        } else if (clazz == Long.class) {
            return new LongBlockBuilder(initialCapacity);
        } else if (clazz == Short.class) {
            return new ShortBlockBuilder(initialCapacity);
        } else if (clazz == Byte.class) {
            return new ByteBlockBuilder(initialCapacity);
        } else if (clazz == Double.class) {
            return new DoubleBlockBuilder(initialCapacity);
        } else if (clazz == Float.class) {
            return new FloatBlockBuilder(initialCapacity);
        } else if (clazz == String.class) {
            return new StringBlockBuilder(type, initialCapacity, EXPECTED_STRING_LEN);
        } else if (clazz == Enum.class) {
            return new EnumBlockBuilder(initialCapacity, EXPECTED_STRING_LEN, (((EnumType) type).getEnumValues()));
        } else if (clazz == Timestamp.class) {
            return new TimestampBlockBuilder(initialCapacity, type, context);
        } else if (clazz == Date.class) {
            return new DateBlockBuilder(initialCapacity, new DateType(), context);
        } else if (clazz == Time.class) {
            return new TimeBlockBuilder(initialCapacity, type, context);
        } else if (clazz == Decimal.class) {
            return new DecimalBlockBuilder(initialCapacity, type);
        } else if (clazz == BigInteger.class) {
            return new BigIntegerBlockBuilder(initialCapacity);
        } else if (clazz == byte[].class) {
            return new ByteArrayBlockBuilder(initialCapacity, EXPECTED_BYTE_ARRAY_LEN);
        } else if (clazz == Blob.class) {
            return new BlobBlockBuilder(initialCapacity);
        } else if (clazz == Clob.class) {
            return new ClobBlockBuilder(initialCapacity);
        } else if (clazz == Slice.class) {
            return new SliceBlockBuilder(type, initialCapacity, context, context.isEnableOssCompatible());
        } else if (clazz == UInt64.class) {
            return new ULongBlockBuilder(initialCapacity);
        }
        throw new AssertionError("data block not implemented");
    }

}
