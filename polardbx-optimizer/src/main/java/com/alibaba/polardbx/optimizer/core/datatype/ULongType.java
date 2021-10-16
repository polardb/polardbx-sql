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
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalRoundMod;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.row.Row;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * unsigned bigint type, 64-bit implementation.
 */
public class ULongType extends AbstractDataType<UInt64> {

    public static final ULongType instance = new ULongType();
    public static final UInt64 MAX_VALUE = UInt64.MAX_UINT64;
    public static final UInt64 MIN_VALUE = UInt64.UINT64_ZERO;
    private static final BigDecimal MIN_VALUE_TO_DECIMAL = new BigDecimal(MIN_VALUE.toString());
    private static final BigDecimal MAX_VALUE_TO_DECIMAL = new BigDecimal(MAX_VALUE.toString());

    private static final BigIntegerType BIG_INTEGER_TYPE_INTERNAL = new BigIntegerType();

    @Override
    public int getSqlType() {
        return Types.BIGINT;
    }

    @Override
    public boolean isUnsigned() {
        return true;
    }

    @Override
    public ResultGetter getResultGetter() {
        // JDBC result set getter
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getBigDecimal(index).toBigInteger();
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }

        };
    }

    @Override
    public UInt64 getMaxValue() {
        return MAX_VALUE;
    }

    @Override
    public UInt64 getMinValue() {
        return MIN_VALUE;
    }

    @Override
    public Class getDataClass() {
        return UInt64.class;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    protected BigDecimal getMaxValueToDecimal() {
        return MAX_VALUE_TO_DECIMAL;
    }

    protected BigDecimal getMinValueToDecimal() {
        return MIN_VALUE_TO_DECIMAL;
    }

    @Override
    public String getStringSqlType() {
        return "UNSIGNED";
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        if (o1 instanceof UInt64 && o2 instanceof UInt64) {
            return ((UInt64) o1).compareTo((UInt64) o2);
        }
        // Use BigInteger because we can't differ unsigned long from minus.
        BigInteger a = BIG_INTEGER_TYPE_INTERNAL.convertFrom(o1);
        BigInteger b = BIG_INTEGER_TYPE_INTERNAL.convertFrom(o2);

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        // unsigned int64 comparison.
        return a.compareTo(b);
    }

    @Override
    public UInt64 convertFrom(Object value) {
        // base case.
        if (value == null) {
            return null;
        } else if (value instanceof UInt64) {
            return (UInt64) value;
        }

        // numeric:
        // approx: double, float
        // exact: decimal, bigDecimal (old), bigInteger (old), long, int, short, byte
        if (value instanceof Double || value instanceof Float) {
            // round double value.
            double rounded = Math.rint(((Number) value).doubleValue());
            return UInt64.fromLong((long) rounded);
        } else if (value instanceof Decimal) {
            // round decimal value.
            DecimalStructure fromValue = ((Decimal) value).getDecimalStructure();
            DecimalStructure tmpDecimal = new DecimalStructure();
            FastDecimalUtils.round(fromValue, tmpDecimal, 0, DecimalRoundMod.HALF_UP);
            long longValue = DecimalConverter.decimalToULong(tmpDecimal)[0];
            return UInt64.fromLong(longValue);
        } else if (value instanceof BigDecimal) {
            // round big decimal value.
            // will remove it in future version.
            BigDecimal rounded = ((BigDecimal) value).setScale(0, BigDecimal.ROUND_HALF_UP);
            return UInt64.fromString(rounded.toPlainString());
        } else if (value instanceof BigInteger) {
            // parse big integer string.
            return UInt64.fromBigInteger((BigInteger) value);
        } else if (value instanceof Number) {
            // wrap 64bit value.
            return UInt64.fromLong(((Number) value).longValue());
        }

        // string:
        // string, slice, byte[]
        if (value instanceof String) {
            return UInt64.fromString((String) value);
        } else if (value instanceof Slice) {
            return UInt64.fromBytes(((Slice) value).getBytes());
        } else if (value instanceof byte[]) {
            // byte[] for hex string literals.
            byte[] hex = (byte[]) value;
            if (hex.length > 8) {
                // Too many bytes for long value.
                return UInt64.MAX_UINT64;
            }
            long longValue = 0L;
            for (int i = 0; i < hex.length; i++) {
                longValue = (longValue << 8) + (Byte.toUnsignedInt(hex[i]));
            }
            return UInt64.fromLong(longValue);
        }

        // temporal:
        // originalTimestamp / originalTime / originalDate
        // java.util.Date (old)
        if (value instanceof OriginalTemporalValue
            || value instanceof java.util.Date) {

            int sqlType = value instanceof Timestamp
                ? Types.TIMESTAMP : (value instanceof Date
                ? Types.DATE : (value instanceof Time
                ? Types.TIME : Types.OTHER));

            // Try to get mysql datetime structure
            MysqlDateTime mysqlDateTime = DataTypeUtil.toMySQLDatetimeByFlags(
                value,
                sqlType,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE | TimeParserFlags.FLAG_TIME_NO_DATE_FRAC_WARN);
            Number converted = MySQLTimeConverter.convertTemporalToNumeric(
                mysqlDateTime,
                mysqlDateTime.getSqlType(),
                Types.BIGINT);
            return UInt64.fromLong(converted.longValue());
        }

        // misc:
        // boolean / EnumValue
        if (value instanceof Boolean) {
            return UInt64.fromLong(((Boolean) value) ? 1L : 0L);
        } else if (value instanceof EnumValue) {
            Object enumVal = ((EnumValue) value).type.convertTo(this, (EnumValue) value);
            return this.convertFrom(enumVal);
        }

        // other: cast as string
        Slice slice = DataTypes.VarcharType.convertFrom(value);
        return UInt64.fromBytes(slice.getBytes());
    }

    @Override
    public BigInteger convertJavaFrom(Object value) {
        UInt64 uint64 = convertFrom(value);
        if (uint64 == null) {
            return null;
        } else {
            return uint64.toBigInteger();
        }
    }

    /**
     * Calculator for scalar functions.
     */
    private final Calculator calculator = new AbstractCalculator() {

        @Override
        public Object doAdd(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return a.add(b);
        }

        @Override
        public Object doSub(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return a.sub(b);
        }

        @Override
        public Object doMultiply(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return a.multi(b);
        }

        @Override
        public Object doDivide(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            if (b.longValue() == 0L) {
                return null;
            }
            return a.divide(b);
        }

        @Override
        public Object doMod(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            if (b.longValue() == 0L) {
                return null;
            }
            return a.mod(b);
        }

        @Override
        public Object doAnd(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return a.longValue() != 0 && b.longValue() != 0;
        }

        @Override
        public Object doOr(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return a.longValue() != 0 || b.longValue() != 0;
        }

        @Override
        public Object doXor(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return (a.longValue() != 0) ^ (b.longValue() != 0);
        }

        @Override
        public Object doNot(Object v1) {
            UInt64 a = convertFrom(v1);
            return a.longValue() == 0;
        }

        @Override
        public Object doBitAnd(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return UInt64.fromLong(a.longValue() & b.longValue());
        }

        @Override
        public Object doBitOr(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return UInt64.fromLong(a.longValue() | b.longValue());
        }

        @Override
        public Object doBitXor(Object v1, Object v2) {
            UInt64 a = convertFrom(v1);
            UInt64 b = convertFrom(v2);
            return UInt64.fromLong(a.longValue() ^ b.longValue());
        }

        @Override
        public Object doBitNot(Object v1) {
            UInt64 a = convertFrom(v1);
            return UInt64.fromLong(~a.longValue());
        }
    };

    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_LONGLONG;
    }
}
