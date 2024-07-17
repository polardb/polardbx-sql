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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import io.airlift.slice.Slice;
import org.apache.orc.impl.TypeUtils;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public abstract class BlockEncodingBuilders {

    public static List<BlockEncoding> create(List<DataType> types, ExecutionContext context) {
        if (null != context && context.isEnableOrcRawTypeBlock()) {
            // Special encoding for raw orc block builder.
            // Only Long/Double/ByteArray blocks are created.
            // Normal query should not get there.
            return createBlockEncodingForRawOrcType(types);
        }

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

    private static List<BlockEncoding> createBlockEncodingForRawOrcType(List<DataType> types) {
        if (types == null || types.isEmpty()) {
            throw new IllegalArgumentException("types is empty!");
        }
        List<BlockEncoding> blockEncodingList = new ArrayList<>();
        for (DataType type : types) {
            switch (type.fieldType()) {
            case MYSQL_TYPE_LONGLONG:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_YEAR:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATETIME2:
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TIMESTAMP2:
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_TIME2:
            case MYSQL_TYPE_BIT: {
                // Long block encoder.
                blockEncodingList.add(new LongBlockEncoding());
                break;
            }
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE: {
                // Double block encoder.
                blockEncodingList.add(new DoubleBlockEncoding());
                break;
            }
            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL: {
                // Long or ByteArray block encoder
                if (TypeUtils.isDecimal64Precision(type.getPrecision())) {
                    blockEncodingList.add(new LongBlockEncoding());
                } else {
                    blockEncodingList.add(new ByteArrayBlockEncoding());
                }
                break;
            }
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_JSON: {
                // ByteArray block encoder
                blockEncodingList.add(new ByteArrayBlockEncoding());
                break;
            }

            default:
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Found invalid type " + type.fieldType() + " when creating block builder for orc raw type");
            }
        }
        return blockEncodingList;
    }

}
