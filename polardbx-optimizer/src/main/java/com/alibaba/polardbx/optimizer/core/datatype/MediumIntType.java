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

public class MediumIntType extends IntegerType {

    private static final int MIN_VALUE = -1 << 23;
    private static final int MAX_VALUE = (1 << 23) - 1;
    private static final BigDecimal MIN_VALUE_TO_DECIMAL = new BigDecimal(MIN_VALUE);
    private static final BigDecimal MAX_VALUE_TO_DECIMAL = new BigDecimal(MAX_VALUE);

    @Override
    public int getSqlType() {
        return MEDIUMINT_SQL_TYPE;
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
}
