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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public abstract class BlockEncodingBuilders {

    public static List<BlockEncoding> create(List<DataType> types) {
        // Very special cases e.g. compound type
        if (types == null || types.isEmpty()) {
            throw new IllegalArgumentException("types is empty!");
        }
        List<BlockEncoding> blockEncodingList = new ArrayList<>();
        for (DataType type : types) {
            Class clazz = type.getDataClass();
            if (clazz == Integer.class) {
                blockEncodingList.add(new IntegerBlockEncoding());
            } else if (clazz == Long.class) {
                blockEncodingList.add(new LongBlockEncoding());
            } else if (clazz == Short.class) {
                blockEncodingList.add(new ShortBlockEncoding());
            } else if (clazz == Byte.class) {
                blockEncodingList.add(new ByteBlockEncoding());
            } else if (clazz == Double.class) {
                blockEncodingList.add(new DoubleBlockEncoding());
            } else if (clazz == Float.class) {
                blockEncodingList.add(new FloatBlockEncoding());
            } else if (clazz == String.class) {
                blockEncodingList.add(new StringBlockEncoding());
            } else if (clazz == Timestamp.class) {
                blockEncodingList.add(new TimestampBlockEncoding());
            } else if (clazz == Date.class) {
                blockEncodingList.add(new DateBlockEncoding());
            } else if (clazz == Time.class) {
                blockEncodingList.add(new TimeBlockEncoding());
            } else if (clazz == Decimal.class) {
                blockEncodingList.add(new DecimalBlockEncoding());
            } else if (clazz == BigInteger.class) {
                blockEncodingList.add(new BigIntegerBlockEncoding());
            } else if (clazz == byte[].class) {
                blockEncodingList.add(new ByteArrayBlockEncoding());
            } else if (clazz == Blob.class) {
                blockEncodingList.add(new BlobBlockEncoding());
            } else if (clazz == Clob.class) {
                blockEncodingList.add(new ClobBlockEncoding());
            } else if (clazz == Enum.class) {
                blockEncodingList.add(new EnumBlockEncoding(((EnumType) type).getEnumValues()));
            } else if (clazz == Slice.class) {
                blockEncodingList.add(new SliceBlockEncoding(type));
            } else if (clazz == UInt64.class) {
                blockEncodingList.add(new ULongBlockEncoding());
            } else {
                throw new AssertionError("data block not implemented for serializer!");
            }
        }
        return blockEncodingList;
    }

}
