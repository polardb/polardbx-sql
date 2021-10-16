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

public class ShortType extends NumberType<Short> {

    private static final short MIN_VALUE = Short.MIN_VALUE;
    private static final short MAX_VALUE = Short.MAX_VALUE;
    private static final BigDecimal MIN_VALUE_TO_DECIMAL = new BigDecimal(MIN_VALUE);
    private static final BigDecimal MAX_VALUE_TO_DECIMAL = new BigDecimal(MAX_VALUE);

    private final Calculator calculator = new AbstractCalculator() {

        @Override
        public Object doAdd(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return i1 + i2;
        }

        @Override
        public Object doSub(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return i1 - i2;
        }

        @Override
        public Object doMultiply(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return i1 * i2;
        }

        @Override
        public Object doDivide(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);

            if (i2 == 0) {
                return null;
            }

            return i1 / i2;
        }

        @Override
        public Object doMod(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);

            if (i2 == 0) {
                return null;
            }
            return i1 % i2;
        }

        @Override
        public Object doAnd(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return (i1 != 0) && (i2 != 0);
        }

        @Override
        public Object doOr(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return (i1 != 0) || (i2 != 0);
        }

        @Override
        public Object doNot(Object v1) {
            Short i1 = convertToShort(v1);

            return i1 == 0;
        }

        @Override
        public Object doBitAnd(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return i1 & i2;
        }

        @Override
        public Object doBitOr(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return i1 | i2;
        }

        @Override
        public Object doBitNot(Object v1) {
            Short i1 = convertToShort(v1);
            return ~i1;
        }

        @Override
        public Object doXor(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return (i1 != 0) ^ (i2 != 0);
        }

        @Override
        public Object doBitXor(Object v1, Object v2) {
            Short i1 = convertToShort(v1);
            Short i2 = convertToShort(v2);
            return i1 ^ i2;
        }
    };

    protected Short convertToShort(Object value) {
        return convertFrom(value);
    }

    @Override
    public Short getMaxValue() {
        return MAX_VALUE;
    }

    @Override
    public Short getMinValue() {
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
        return java.sql.Types.SMALLINT;
    }

    @Override
    public String getStringSqlType() {
        return "SMALLINT";
    }

    @Override
    public Class getDataClass() {
        return Short.class;
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_SHORT;
    }
}
