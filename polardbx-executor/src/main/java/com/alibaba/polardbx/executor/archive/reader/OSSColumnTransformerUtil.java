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

package com.alibaba.polardbx.executor.archive.reader;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;

public class OSSColumnTransformerUtil {

    /**
     * Compare two field type and return the specific comparison result.
     *
     * @param target target column data type.
     * @param source from column data type.
     * @return Defines for comparison results of two datatype.
     */
    public static TypeComparison compare(ColumnMeta target, ColumnMeta source) {
        if (source == null) {
            return TypeComparison.MISSING_EQUAL;
        }
        MySQLStandardFieldType fromFieldType = source.getDataType().fieldType();
        MySQLStandardFieldType toFieldType = target.getDataType().fieldType();

        switch (fromFieldType) {
        // for varchar(length)
        case MYSQL_TYPE_VAR_STRING: {
            switch (toFieldType) {
            // for varchar(length)
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_VARCHAR: {
                CharsetName fromCharset = source.getDataType().getCharsetName();
                CharsetName toCharset = target.getDataType().getCharsetName();
                if (toFieldType == fromFieldType && toCharset == fromCharset) {
                    int fromLen = source.getDataType().length();
                    int toLen = target.getDataType().length();
                    if (toLen == fromLen) {
                        // VARCHAR with the same length and character set.
                        return TypeComparison.IS_EQUAL_YES;
                    }
                    if (toLen > fromLen && ((toLen <= 255 && fromLen <= 255) || (toLen > 255 && fromLen > 255))) {
                        // VARCHAR, longer variable length.
                        return TypeComparison.IS_EQUAL_YES;
                    }
                }
                return TypeComparison.IS_EQUAL_NO;
            }
            default:
                return TypeComparison.IS_EQUAL_NO;
            }
        }

        // For char(length)
        case MYSQL_TYPE_STRING: {
            if (fromFieldType == toFieldType) {
                CharsetName fromCharset = source.getDataType().getCharsetName();
                CharsetName toCharset = target.getDataType().getCharsetName();
                int fromLen = source.getDataType().length();
                int toLen = target.getDataType().length();
                if (fromCharset == toCharset && fromLen == toLen) {
                    return TypeComparison.IS_EQUAL_YES;
                }
            }
            return TypeComparison.IS_EQUAL_NO;
        }

        // for any temporal type.
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2: {
            if (fromFieldType == toFieldType) {
                int fromScale = source.getDataType().getScale();
                int toScale = target.getDataType().getScale();
                if (fromScale == toScale) {
                    // Temporal type with the same field enum and scale.
                    return TypeComparison.IS_EQUAL_YES;
                }
            }
            return TypeComparison.IS_EQUAL_NO;
        }

        // For any fixed-precision numeric datatype。
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG: {
            if (fromFieldType == toFieldType) {
                boolean isToUnsigned = target.getDataType().isUnsigned();
                boolean isFromUnsigned = source.getDataType().isUnsigned();
                if (isToUnsigned == isFromUnsigned) {
                    return TypeComparison.IS_EQUAL_YES;
                }
            }
            return TypeComparison.IS_EQUAL_NO;
        }

        // For decimal datatype.
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            if (fromFieldType == toFieldType) {
                int fromScale = source.getDataType().getScale();
                int toScale = target.getDataType().getScale();
                int fromPrecision = source.getDataType().getPrecision();
                int toPrecision = target.getDataType().getPrecision();
                if (fromScale == toScale && fromPrecision == toPrecision) {
                    return TypeComparison.IS_EQUAL_YES;
                }
            }
            return TypeComparison.IS_EQUAL_NO;
        }

        // NOTE: we cannot compare the blob, geom, json type except for type name.
        default: {
            if (fromFieldType == toFieldType) {
                return TypeComparison.IS_EQUAL_YES;
            }
            return TypeComparison.IS_EQUAL_NO;
        }

        }
    }

    /**
     * decide the type of missing
     *
     * @param result the compare result of missing column's default value and the latest column
     * @return whether default value's datatype and latest datatype is equal
     */
    public static TypeComparison defaultColumnCompare(TypeComparison result) {
        switch (result) {
        case IS_EQUAL_YES:
        case IS_EQUAL_PACK_LENGTH:
            return TypeComparison.MISSING_EQUAL;
        case INVALID:
            return TypeComparison.INVALID;
        default:
            return TypeComparison.MISSING_NO_EQUAL;
        }
    }
}
