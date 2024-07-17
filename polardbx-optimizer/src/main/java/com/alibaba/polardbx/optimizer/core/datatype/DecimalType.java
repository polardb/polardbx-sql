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
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;

import java.math.BigDecimal;

import static com.alibaba.polardbx.common.datatype.Decimal.MAX_128_BIT_PRECISION;
import static com.alibaba.polardbx.common.datatype.Decimal.MAX_64_BIT_PRECISION;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DEFAULT_SCALE;

/**
 * Decimal Type
 */
public class DecimalType extends NumberType<Decimal> {

    static final Decimal MAX_VALUE = Decimal.fromString("1E66");
    static final Decimal MIN_VALUE = Decimal.fromString("-1E66");

    private final int precision;
    private final int scale;

    public static DecimalType decimal64WithScale(int scale) {
        return new DecimalType(MAX_64_BIT_PRECISION, scale);
    }

    public static DecimalType decimal128WithScale(int scale) {
        return new DecimalType(MAX_128_BIT_PRECISION, scale);
    }

    public DecimalType() {
        this(0, DEFAULT_SCALE);
    }

    public DecimalType(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    public boolean isDecimal64() {
        return DecimalConverter.isDecimal64(precision) && scale >= 0;
    }

    public boolean isDecimal128() {
        return DecimalConverter.isDecimal128(precision) && scale >= 0;
    }

    public boolean isDefaultScale() {
        return precision == DecimalTypeBase.MAX_DECIMAL_PRECISION && scale == 0;
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
