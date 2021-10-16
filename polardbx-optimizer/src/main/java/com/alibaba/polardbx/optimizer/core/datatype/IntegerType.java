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

/**
 * int/Integer类型
 *
 * @since 5.0.0
 */
public class IntegerType extends NumberType<Integer> {

    private static final int MIN_VALUE = Integer.MIN_VALUE;
    private static final int MAX_VALUE = Integer.MAX_VALUE;
    private static final BigDecimal MIN_VALUE_TO_DECIMAL = new BigDecimal(MIN_VALUE);
    private static final BigDecimal MAX_VALUE_TO_DECIMAL = new BigDecimal(MAX_VALUE);

    private final Calculator calculator = new AbstractCalculator() {

        @Override
        public Object doAdd(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return i1 + i2;
        }

        @Override
        public Object doSub(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return i1 - i2;
        }

        @Override
        public Object doMultiply(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return i1 * (long) i2;
        }

        @Override
        public Object doDivide(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);

            if (i2 == 0) {
                return null;
            }
            return i1 / i2;
        }

        @Override
        public Object doMod(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);

            if (i2 == 0) {
                return null;
            }

            return i1 % i2;
        }

        @Override
        public Object doAnd(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return (i1 != 0) && (i2 != 0);
        }

        @Override
        public Object doOr(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return (i1 != 0) || (i2 != 0);
        }

        @Override
        public Object doNot(Object v1) {
            Integer i1 = convertToInt(v1);

            return i1 == 0;
        }

        @Override
        public Object doBitAnd(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return i1 & i2;
        }

        @Override
        public Object doBitOr(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return i1 | i2;
        }

        @Override
        public Object doBitNot(Object v1) {
            Integer i1 = convertToInt(v1);
            return ~i1;
        }

        @Override
        public Object doXor(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return (i1 != 0) ^ (i2 != 0);
        }

        @Override
        public Object doBitXor(Object v1, Object v2) {
            Integer i1 = convertToInt(v1);
            Integer i2 = convertToInt(v2);
            return i1 ^ i2;
        }
    };

    protected Integer convertToInt(Object value) {
        return convertFrom(value);
    }

    @Override
    public Integer getMaxValue() {
        return MAX_VALUE;
    }

    @Override
    public Integer getMinValue() {
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
        return java.sql.Types.INTEGER;
    }

    @Override
    public String getStringSqlType() {
        return "INTEGER";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_LONG;
    }

    @Override
    public Class getDataClass() {
        return Integer.class;
    }
}
