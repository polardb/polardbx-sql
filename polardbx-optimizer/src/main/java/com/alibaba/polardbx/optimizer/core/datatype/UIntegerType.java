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
 * unsigned int类型,计算精度需要依赖LongType
 *
 * @author agapple 2014年11月21日 上午10:57:18
 * @since 5.1.14
 */
public class UIntegerType extends LongType {

    private static final long MIN_VALUE = 0L;
    private static final long MAX_VALUE = (long) Integer.MAX_VALUE - (long) Integer.MIN_VALUE;
    private static final BigDecimal MIN_VALUE_TO_DECIMAL = new BigDecimal(MIN_VALUE);
    private static final BigDecimal MAX_VALUE_TO_DECIMAL = new BigDecimal(MAX_VALUE);

    @Override
    public int getSqlType() {
        return java.sql.Types.INTEGER;
    }

    @Override
    public boolean isUnsigned() {
        return true;
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
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_LONG;
    }

    @Override
    protected Long convertToLong(Object value) {
        Long val = convertFrom(value);
        val = val >= 0 ? val : val + MAX_VALUE + 1;
        return val;
    }
}
