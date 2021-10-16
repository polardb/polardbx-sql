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

package com.alibaba.polardbx.common.type;

import static com.alibaba.polardbx.common.type.MySQLResultType.*;

public enum MySQLStandardFieldType {

    MYSQL_TYPE_DECIMAL(0),

    MYSQL_TYPE_TINY(1),

    MYSQL_TYPE_SHORT(2),

    MYSQL_TYPE_LONG(3),

    MYSQL_TYPE_FLOAT(4),

    MYSQL_TYPE_DOUBLE(5),

    MYSQL_TYPE_NULL(6),

    MYSQL_TYPE_TIMESTAMP(7),

    MYSQL_TYPE_LONGLONG(8),

    MYSQL_TYPE_INT24(9),

    MYSQL_TYPE_DATE(10),

    MYSQL_TYPE_TIME(11),

    MYSQL_TYPE_DATETIME(12),

    MYSQL_TYPE_YEAR(13),

    MYSQL_TYPE_NEWDATE(14),

    MYSQL_TYPE_VARCHAR(15),

    MYSQL_TYPE_BIT(16),

    MYSQL_TYPE_TIMESTAMP2(17),

    MYSQL_TYPE_DATETIME2(18),

    MYSQL_TYPE_TIME2(19),

    MYSQL_TYPE_JSON(245),

    MYSQL_TYPE_NEWDECIMAL(246),

    MYSQL_TYPE_ENUM(247),

    MYSQL_TYPE_SET(248),

    MYSQL_TYPE_TINY_BLOB(249),

    MYSQL_TYPE_MEDIUM_BLOB(250),

    MYSQL_TYPE_LONG_BLOB(251),

    MYSQL_TYPE_BLOB(252),

    MYSQL_TYPE_VAR_STRING(253),

    MYSQL_TYPE_STRING(254),

    MYSQL_TYPE_GEOMETRY(255);

    static MySQLResultType[] FIELD_TYPES_TO_REAL_TYPES = {

        DECIMAL_RESULT,

        INT_RESULT,

        INT_RESULT,

        INT_RESULT,

        REAL_RESULT,

        REAL_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        INT_RESULT,

        INT_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        INT_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        DECIMAL_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT,

        STRING_RESULT
    };

    private final int id;

    MySQLStandardFieldType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    private int fieldTypeIndex() {
        if (id <= MYSQL_TYPE_BIT.id) {
            return id;
        } else if (id <= MYSQL_TYPE_TIME2.id) {
            return toOldType().id;
        }
        return id - MYSQL_TYPE_JSON.id + MYSQL_TYPE_BIT.id + 1;
    }

    public MySQLStandardFieldType toOldType() {
        switch (this) {
        case MYSQL_TYPE_TIME2:
            return MYSQL_TYPE_TIME;
        case MYSQL_TYPE_DATETIME2:
            return MYSQL_TYPE_DATETIME;
        case MYSQL_TYPE_TIMESTAMP2:
            return MYSQL_TYPE_TIMESTAMP;
        case MYSQL_TYPE_NEWDATE:
            return MYSQL_TYPE_DATE;

        default:
            return this;
        }
    }

    public MySQLResultType toResultType() {
        return FIELD_TYPES_TO_REAL_TYPES[fieldTypeIndex()];
    }

    public static boolean isTemporalType(MySQLStandardFieldType fieldType) {
        switch (fieldType) {
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATE:
            return true;
        default:
            return false;
        }
    }

    public static boolean isTemporalTypeWithDate(MySQLStandardFieldType fieldType) {
        switch (fieldType) {
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATE:
            return true;
        default:
            return false;
        }
    }
}
