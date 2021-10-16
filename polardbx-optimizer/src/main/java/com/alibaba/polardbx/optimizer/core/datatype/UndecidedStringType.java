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
import com.alibaba.polardbx.common.utils.CaseInsensitive;

/**
 * 未决类型
 *
 * @author jianghang 2014-5-23 下午8:27:59
 * @since 5.1.0
 */
public class UndecidedStringType extends AbstractUTF8StringType {

    @Override
    public int getSqlType() {
        return UNDECIDED_SQL_TYPE;
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_VAR_STRING;
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        // follow MySQL behavior
        if (o1 instanceof Number || o2 instanceof Number) {
            return DataTypes.DecimalType.compare(o1, o2);
        }
        String no1 = convertFrom(o1);
        String no2 = convertFrom(o2);

        if (no1 == null) {
            return -1;
        }

        if (no2 == null) {
            return 1;
        }
        /**
         * mysql 默认不区分大小写
         */
        return CaseInsensitive.compareToIgnoreCase(no1, no2);
    }
}
