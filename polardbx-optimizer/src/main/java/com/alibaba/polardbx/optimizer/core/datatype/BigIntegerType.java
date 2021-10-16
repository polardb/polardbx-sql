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

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * {@link BigInteger}类型
 *
 * @author jianghang 2014-1-21 下午1:49:09
 * @since 5.0.0
 */
public class BigIntegerType extends NumberType<BigInteger> {

    public static final BigInteger ZERO_VALUE = BigInteger.valueOf(0);
    private static final BigInteger MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigDecimal MIN_VALUE_TO_DECIMAL = new BigDecimal(MIN_VALUE);
    private static final BigDecimal MAX_VALUE_TO_DECIMAL = new BigDecimal(MAX_VALUE);

    private final Calculator calculator = new AbstractCalculator() {

        @Override
        public Object doAdd(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return i1.add(i2);
        }

        @Override
        public Object doSub(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return i1.subtract(i2);
        }

        @Override
        public Object doMultiply(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return i1.multiply(i2);
        }

        @Override
        public Object doDivide(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);

            if (i2.equals(BigInteger.ZERO)) {
                return null;
            }

            return i1.divide(i2);
        }

        @Override
        public Object doMod(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);

            if (i2.equals(BigInteger.ZERO)) {
                return null;
            }

            return i1.remainder(i2);
        }

        @Override
        public Object doAnd(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return (i1.compareTo(ZERO_VALUE) != 0)
                && (i2.compareTo(ZERO_VALUE) != 0);
        }

        @Override
        public Object doOr(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return (i1.compareTo(ZERO_VALUE) != 0)
                || (i2.compareTo(ZERO_VALUE) != 0);
        }

        @Override
        public Object doNot(Object v1) {
            BigInteger i1 = convertToBigInteger(v1);

            return (i1.compareTo(ZERO_VALUE) == 0);
        }

        @Override
        public Object doBitAnd(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return i1.and(i2);
        }

        @Override
        public Object doBitOr(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return i1.or(i2);
        }

        @Override
        public Object doBitNot(Object v1) {
            BigInteger i1 = convertToBigInteger(v1);
            return i1.not();
        }

        @Override
        public Object doXor(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return (i1.compareTo(ZERO_VALUE) != 0)
                ^ (i2.compareTo(ZERO_VALUE) != 0);
        }

        @Override
        public Object doBitXor(Object v1, Object v2) {
            BigInteger i1 = convertToBigInteger(v1);
            BigInteger i2 = convertToBigInteger(v2);
            return i1.xor(i2);
        }
    };

    protected BigInteger convertToBigInteger(Object value) {
        return convertFrom(value);
    }

    @Override
    public BigInteger getMaxValue() {
        return MAX_VALUE;
    }

    @Override
    public BigInteger getMinValue() {
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
        // could not change to java.sql.Types.BIGINT, since it can't represent
        // unsigned type
        return java.sql.Types.NUMERIC;
    }

    @Override
    public boolean isUnsigned() {
        return true;
    }

    @Override
    public String getStringSqlType() {
        return "BIGINT";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_LONGLONG;
    }

    @Override
    public Class getDataClass() {
        return BigInteger.class;
    }
}
