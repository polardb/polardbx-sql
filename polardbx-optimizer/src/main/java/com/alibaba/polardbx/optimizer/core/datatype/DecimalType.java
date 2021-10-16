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
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;

import java.math.BigDecimal;

/**
 * Decimal Type
 */
public class DecimalType extends NumberType<Decimal> {

    private static final Decimal MAX_VALUE = Decimal.fromString("1E66");
    private static final Decimal MIN_VALUE = Decimal.fromString("-1E66");
    private static final Decimal ZERO_VALUE = Decimal.ZERO;

    private int precision;
    private int scale;

    public DecimalType() {
        this(0, -1);
    }

    public DecimalType(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    private final Calculator calculator = new AbstractDecimalCalculator() {
        @Override
        public Decimal convertToDecimal(Object v) {
            return convertFrom(v);
        }
    };

    @Override
    public Decimal getMaxValue() {
        return MAX_VALUE;
    }

    @Override
    public Decimal getMinValue() {
        return MIN_VALUE;
    }

    @Override
    protected BigDecimal getMaxValueToDecimal() {
        return getMaxValue().toBigDecimal();
    }

    @Override
    protected BigDecimal getMinValueToDecimal() {
        return getMinValue().toBigDecimal();
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.DECIMAL;
    }

    @Override
    public String getStringSqlType() {
        return "DECIMAL";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_NEWDECIMAL;
    }

    @Override
    public Class getDataClass() {
        return Decimal.class;
    }

    @Override
    public Object convertJavaFrom(Object value) {
        if (value == null || value instanceof NullValue) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return value;
        }
        Decimal d = convertFrom(value);
        return d.toBigDecimal();
    }

    @Override
    public Class getJavaClass() {
        return BigDecimal.class;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getScale() {
        return scale;
    }
}
