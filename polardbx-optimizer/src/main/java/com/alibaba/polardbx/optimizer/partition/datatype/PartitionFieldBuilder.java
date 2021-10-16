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

package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class PartitionFieldBuilder {

    public static AbstractPartitionField createField(DataType<?> dataType) {
        MySQLStandardFieldType sqlType = dataType.fieldType();
        switch (sqlType) {
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
            return new DatetimePartitionField(dataType);
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            return new DatePartitionField(dataType);
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            return new TimestampPartitionField(dataType);
        case MYSQL_TYPE_LONGLONG:
            return new BigIntPartitionField(dataType);
        case MYSQL_TYPE_LONG: {
            return new IntPartitionField(dataType);
        }
        case MYSQL_TYPE_INT24:
            return new MediumIntPartitionField(dataType);
        case MYSQL_TYPE_SHORT:
            return new SmallIntPartitionField(dataType);
        case MYSQL_TYPE_TINY:
            return new TinyIntPartitionField(dataType);
        case MYSQL_TYPE_STRING:
            return new CharPartitionField(dataType);
        case MYSQL_TYPE_VAR_STRING:
            return new VarcharPartitionField(dataType);
        default:
            throw new UnsupportedOperationException("unsupported field data type: " + dataType);
        }
    }
}
