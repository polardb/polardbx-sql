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

import java.sql.Timestamp;

/**
 * {@link Timestamp}类型
 *
 * @author jianghang 2014-1-21 下午5:36:26
 * @since 5.0.0
 */
public class DateTimeType extends TimestampType {
    public static final DateTimeType DATE_TIME_TYPE_0 = new DateTimeType(0);
    public static final DateTimeType DATE_TIME_TYPE_1 = new DateTimeType(1);
    public static final DateTimeType DATE_TIME_TYPE_2 = new DateTimeType(2);
    public static final DateTimeType DATE_TIME_TYPE_3 = new DateTimeType(3);
    public static final DateTimeType DATE_TIME_TYPE_4 = new DateTimeType(4);
    public static final DateTimeType DATE_TIME_TYPE_5 = new DateTimeType(5);
    public static final DateTimeType DATE_TIME_TYPE_6 = new DateTimeType(6);

    public DateTimeType() {
        this(0);
    }

    public DateTimeType(int scale) {
        super(scale);
    }

    @Override
    public int getSqlType() {

        return DATETIME_SQL_TYPE;
    }

    @Override
    public String getStringSqlType() {
        return "DATETIME";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_DATETIME;
    }
}
