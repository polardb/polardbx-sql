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
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.google.common.primitives.UnsignedLong;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * {@linkplain Long}类型
 *
 * @author jianghang 2014-1-22 上午10:41:28
 * @since 5.0.0
 */
public class LongType extends NumberType<Long> {

    private static final long MIN_VALUE = Long.MIN_VALUE;
    private static final long MAX_VALUE = Long.MAX_VALUE;
    private static final BigDecimal MIN_VALUE_TO_DECIMAL = new BigDecimal(MIN_VALUE);
    private static final BigDecimal MAX_VALUE_TO_DECIMAL = new BigDecimal(MAX_VALUE);

    private final Calculator calculator = new AbstractCalculator() {

        @Override
        public Object doAdd(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return i1 + i2;
        }

        @Override
        public Object doSub(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return i1 - i2;
        }

        @Override
        public Object doMultiply(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return i1 * i2;
        }

        @Override
        public Object doDivide(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            if (i2 == 0L) {
                return null;
            }
            return i1 / i2;
        }

        @Override
        public Object doMod(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            if (i2 == 0L) {
                return null;
            }
            return i1 % i2;
        }

        @Override
        public Object doAnd(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return (i1 != 0) && (i2 != 0);
        }

        @Override
        public Object doOr(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return (i1 != 0) || (i2 != 0);
        }

        @Override
        public Object doNot(Object v1) {
            Long i1 = convertToLong(v1);

            return i1 == 0;
        }

        @Override
        public Object doBitAnd(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return i1 & i2;
        }

        @Override
        public Object doBitOr(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return i1 | i2;
        }

        @Override
        public Object doBitNot(Object v1) {
            Long i1 = convertToLong(v1);
            String bits = Long.toBinaryString(~i1);
            UnsignedLong unsignedInteger = UnsignedLong.valueOf(bits, 2);
            return unsignedInteger.bigIntegerValue();
        }

        @Override
        public Object doXor(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return (i1 != 0) ^ (i2 != 0);
        }

        @Override
        public Object doBitXor(Object v1, Object v2) {
            Long i1 = convertToLong(v1);
            Long i2 = convertToLong(v2);
            return i1 ^ i2;
        }
    };

    protected Long convertToLong(Object value) {
        return convertFrom(value);
    }

    @Override
    public Long convertFrom(Object value) {
        // base case.
        if (value == null) {
            return null;
        } else if (value instanceof Long) {
            return (Long) value;
        }

        // numeric:
        // approx: double, float
        // exact: decimal, bigDecimal (old), bigInteger (old), long, int, short, byte
        if (value instanceof Double || value instanceof Float) {
            // round double value.
            double rounded = Math.rint(((Number) value).doubleValue());
            return (long) rounded;
        } else if (value instanceof Decimal) {
            // round decimal value.
            DecimalStructure fromValue = ((Decimal) value).getDecimalStructure();
            DecimalStructure tmpDecimal = new DecimalStructure();
            FastDecimalUtils.round(fromValue, tmpDecimal, 0, DecimalRoundMod.HALF_UP);
            long longValue = DecimalConverter.decimal2Long(tmpDecimal)[0];
            return longValue;
        } else if (value instanceof BigDecimal) {
            // round big decimal value.
            // will remove it in future version.
            BigDecimal rounded = ((BigDecimal) value).setScale(0, BigDecimal.ROUND_HALF_UP);
            long[] results = StringNumericParser.parseString(rounded.toPlainString().getBytes());
            return results[StringNumericParser.NUMERIC_INDEX];
        } else if (value instanceof BigInteger) {
            // parse big integer string.
            // will remove it in future version.
            BigInteger b = (BigInteger) value;
            long[] results = StringNumericParser.parseString(b.toString().getBytes());
            return results[StringNumericParser.NUMERIC_INDEX];
        } else if (value instanceof Number) {
            // wrap 64bit value.
            return ((Number) value).longValue();
        }

        // string:
        // string, slice, byte[]
        if (value instanceof String) {
            long[] results = StringNumericParser.parseString(((String) value).getBytes());
            return results[StringNumericParser.NUMERIC_INDEX];
        } else if (value instanceof Slice) {
            long[] results = StringNumericParser.parseString((Slice) value);
            return results[StringNumericParser.NUMERIC_INDEX];
        } else if (value instanceof byte[]) {
            // byte[] for hex string literals.
            byte[] hex = (byte[]) value;
            if (hex.length > 8) {
                // Too many bytes for long value.
                return -1L;
            }
            long longValue = 0L;
            for (int i = 0; i < hex.length; i++) {
                longValue = (longValue << 8) + (Byte.toUnsignedInt(hex[i]));
            }
            return longValue;
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
            return converted.longValue();
        }

        // misc:
        // boolean / enum val
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1L : 0L;
        } else if (value instanceof EnumValue) {
            Object enumVal = ((EnumValue) value).type.convertTo(this, (EnumValue) value);
            return this.convertFrom(enumVal);
        }

        // other: cast as string
        Slice slice = DataTypes.VarcharType.convertFrom(value);
        long[] results = StringNumericParser.parseString(slice);
        return results[StringNumericParser.NUMERIC_INDEX];
    }

    @Override
    public Long getMaxValue() {
        return MAX_VALUE;
    }

    @Override
    public Long getMinValue() {
        return MIN_VALUE;
    }

    @Override
    protected BigDecimal getMaxValueToDecimal() {
        return MAX_VALUE_TO_DECIMAL;
    }

    @Override
    protected BigDecimal getMinValueToDecimal() {
        return MIN_VALUE_TO_DECIMAL;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.BIGINT;
    }

    @Override
    public String getStringSqlType() {
        return "BIGINT";
    }

    @Override
    public Class getDataClass() {
        return Long.class;
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_LONGLONG;
    }
}
