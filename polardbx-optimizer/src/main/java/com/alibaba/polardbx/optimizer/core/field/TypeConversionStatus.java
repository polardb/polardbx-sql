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

package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;

/**
 * Status when storing a value in a field or converting from one
 * datatype to another. The values should be listed in order of
 * increasing seriousness so that if two type_conversion_status
 * variables are compared, the bigger one is most serious.
 */
public enum TypeConversionStatus {
    /**
     * Storage/conversion went fine.
     */
    TYPE_OK(0),
    /**
     * A minor problem when converting between temporal values, e.g.
     * if datetime is converted to date the time information is lost.
     */
    TYPE_NOTE_TIME_TRUNCATED(1),
    /**
     * Value outside min/max limit of datatype. The min/max value is
     * stored by Field::store() instead (if applicable)
     */
    TYPE_WARN_OUT_OF_RANGE(2),
    /**
     * Value was stored, but something was cut. What was cut is
     * considered insignificant enough to only issue a note. Example:
     * trying to store a number with 5 decimal places into a field that
     * can only store 3 decimals. The number rounded to 3 decimal places
     * should be stored. Another example: storing the string "foo " into
     * a VARCHAR(3). The string "foo" is stored in this case, so only
     * whitespace is cut.
     */
    TYPE_NOTE_TRUNCATED(3),
    /**
     * Value was stored, but something was cut. What was cut is
     * considered significant enough to issue a warning. Example: storing
     * the string "foo" into a VARCHAR(2). The string "fo" is stored in
     * this case. Another example: storing the string "2010-01-01foo"
     * into a DATE. The garbage in the end of the string is cut in this
     * case.
     */
    TYPE_WARN_TRUNCATED(4),
    /**
     * Value has invalid string data. When present in a predicate with
     * equality operator, range optimizer returns an impossible where.
     */
    TYPE_WARN_INVALID_STRING(5),
    /**
     * Trying to store NULL in a NOT NULL field.
     */
    TYPE_ERR_NULL_CONSTRAINT_VIOLATION(6),
    /**
     * Store/convert incompatible values, like converting "foo" to a
     * date.
     */
    TYPE_ERR_BAD_VALUE(7),
    /**
     * Out of memory
     */
    TYPE_ERR_OOM(8),

    TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST(9);

    private final int code;

    TypeConversionStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static TypeConversionStatus fromParseStatus(TimeParseStatus status) {
        if (status.checkWarnings(TimeParserFlags.FLAG_TIME_NOTE_TRUNCATED)) {
            return TypeConversionStatus.TYPE_NOTE_TIME_TRUNCATED;
        }

        if (status.checkWarnings(TimeParserFlags.FLAG_TIME_WARN_OUT_OF_RANGE)) {
            return TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
        }

        if (status.checkWarnings(TimeParserFlags.FLAG_TIME_WARN_TRUNCATED)) {
            return TypeConversionStatus.TYPE_NOTE_TRUNCATED;
        }

        if (status.checkWarnings(
            TimeParserFlags.FLAG_TIME_WARN_ZERO_DATE | TimeParserFlags.FLAG_TIME_WARN_ZERO_IN_DATE)) {
            return TypeConversionStatus.TYPE_ERR_BAD_VALUE;
        }

        if (status.checkWarnings(TimeParserFlags.FLAG_TIME_WARN_INVALID_TIMESTAMP))
        // date was fine but pointed to daylight saving time switch gap
        {
            return TypeConversionStatus.TYPE_OK;
        }

        return TypeConversionStatus.TYPE_OK;
    }

    public static boolean checkForSelect(TypeConversionStatus status) {
        switch (status) {
        case TYPE_OK:
        case TYPE_NOTE_TRUNCATED:
        case TYPE_WARN_TRUNCATED:
        case TYPE_NOTE_TIME_TRUNCATED:
            return true;
        default:
            return false;
        }
    }

}
