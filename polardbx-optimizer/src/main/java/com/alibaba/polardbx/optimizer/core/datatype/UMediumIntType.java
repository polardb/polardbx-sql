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
 * unsigned medium类型, 可基于IntegerType做计算
 *
 * @author agapple 2014年11月21日 上午11:19:51
 * @since 5.1.14
 */
public class UMediumIntType extends IntegerType {

    private static final int MIN_VALUE = 0;
    private static final int MAX_VALUE = (1 << 24) - 1;
    private static final BigDecimal MIN_VALUE_TO_DECIMAL = new BigDecimal(MIN_VALUE);
    private static final BigDecimal MAX_VALUE_TO_DECIMAL = new BigDecimal(MAX_VALUE);

    @Override
    public int getSqlType() {
        return MEDIUMINT_SQL_TYPE;
    }

    @Override
    public boolean isUnsigned() {
        return true;
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
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_INT24;
    }

    @Override
    protected Integer convertToInt(Object value) {
        int i1 = convertFrom(value);
        i1 = i1 >= 0 ? i1 : i1 + MAX_VALUE + 1;
        return i1;
    }
}
