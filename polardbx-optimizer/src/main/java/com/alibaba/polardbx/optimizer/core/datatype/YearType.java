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

/**
 * 针对mysql中特殊的year type
 *
 * @author jianghang 2014-5-23 下午5:22:28
 * @since 5.1.0
 */
public class YearType extends LongType {
    static long SHORT_VALUE_LOWER_BOUND = 1;
    static long SHORT_VALUE_UPPER_BOUND = 99;
    static long SHORT_VALUE_SPLIT_POINT = 69;

    @Override
    public int getSqlType() {
        return YEAR_SQL_TYPE;
    }

    @Override
    public String getStringSqlType() {
        return "YEAR";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_YEAR;
    }

    @Override
    public Long convertFrom(Object value) {
        Long convert = super.convertFrom(value);
        if (convert == null) {
            return null;
        }
        if (convert >= SHORT_VALUE_LOWER_BOUND && convert <= SHORT_VALUE_UPPER_BOUND) {
            convert = fixedReturnValue(convert);
        }
        return convert;
    }

    private Long fixedReturnValue(Long value) {
        if (value >= SHORT_VALUE_LOWER_BOUND && value <= SHORT_VALUE_SPLIT_POINT) {
            return value + 2000;
        } else if (value > SHORT_VALUE_SPLIT_POINT && value <= SHORT_VALUE_UPPER_BOUND) {
            return value + 1900;
        } else {
            return value;
        }
    }
}
