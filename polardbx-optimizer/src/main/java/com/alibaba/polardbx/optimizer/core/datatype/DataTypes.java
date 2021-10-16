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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;

public abstract class DataTypes {

    /**
     * The old style String Type used just by the internal, with UTF-8 charset and UTF8_GENERAL_CI collation.
     * The StringType cannot be converted from RelDataType, which means that no user SQL can access to this type.
     */
    public static final DataType<String> StringType = new StringType();
    public static final DataType<String> SensitiveStringType = new SensitiveStringType();
    public static final DataType<String> BinaryStringType = new SensitiveStringType();

    // utf8, utf8_general_ci
    public static final DataType<Slice> VarcharType = new VarcharType();

    @SuppressWarnings("unused")
    public static final DataType<Slice> CharType = new CharType();

    public static final DataType<Double> DoubleType = new DoubleType();
    public static final DataType<Float> FloatType = new FloatType();
    public static final DataType<Integer> BooleanType = new BooleanType();
    public static final DataType<Decimal> DecimalType = new DecimalType();

    public static final DataType<Short> TinyIntType = new TinyIntType();
    public static final DataType<Short> UTinyIntType = new UTinyIntType();
    public static final DataType<Short> SmallIntType = new SmallIntType();
    public static final DataType<Integer> USmallIntType = new USmallIntType();
    public static final DataType<Integer> MediumIntType = new MediumIntType();
    public static final DataType<Integer> UMediumIntType = new UMediumIntType();
    public static final DataType<Integer> IntegerType = new IntegerType();
    public static final DataType<Long> UIntegerType = new UIntegerType();
    public static final DataType<Long> LongType = new LongType();
    public static final DataType<UInt64> ULongType = new ULongType();
    public static final DataType<Short> ShortType = new ShortType();

    public static final DataType<java.sql.Date> DateType = new DateType();
    public static final DataType<java.sql.Timestamp> TimestampType = new TimestampType();
    public static final DataType<java.sql.Timestamp> DatetimeType = new DateTimeType();
    public static final DataType<java.sql.Time> TimeType = new TimeType();

    public static final DataType<Long> YearType = new YearType();
    public static final DataType<Blob> BlobType = new BlobType();
    public static final DataType<Clob> ClobType = new ClobType();
    public static final DataType<Integer> BitType = new BitType();
    public static final DataType<BigInteger> BigBitType = new BigBitType();
    public static final DataType<byte[]> BytesType = new BytesType();
    public static final DataType<Byte> ByteType = new ByteType();

    public static final DataType UndecidedType = new UndecidedStringType();
    public static final DataType NullType = UndecidedType;
    public static final DataType IntervalType = new IntervalType();
    public static final DataType<byte[]> BinaryType = new BinaryType();
    public static final DataType<String> JsonType = new JsonType();
}
