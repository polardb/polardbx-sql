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

import com.alibaba.polardbx.common.SQLModeFlags;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.type.MySQLResultType;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.JsonType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.google.common.primitives.UnsignedLongs;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.alibaba.polardbx.common.type.MySQLResultType.DECIMAL_RESULT;
import static com.alibaba.polardbx.common.type.MySQLResultType.INT_RESULT;
import static com.alibaba.polardbx.common.type.MySQLResultType.REAL_RESULT;
import static com.alibaba.polardbx.common.type.MySQLResultType.STRING_RESULT;
import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_BIT;
import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_DATE;
import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_TIME;
import static com.alibaba.polardbx.optimizer.partition.datatype.PredicateBoolean.*;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.EQUALS;

public class PartitionDataTypeUtils {

    public static PartitionField minValueOf(DataType fieldType) {
        MySQLStandardFieldType standardFieldType = fieldType.fieldType();
        switch (standardFieldType) {
        case MYSQL_TYPE_LONGLONG:
            return fieldType.isUnsigned() ? BigIntPartitionField.MIN_UNSIGNED_VALUE : BigIntPartitionField.MIN_VALUE;
        case MYSQL_TYPE_LONG:
            return fieldType.isUnsigned() ? IntPartitionField.MIN_UNSIGNED_VALUE : IntPartitionField.MIN_VALUE;
        }
        return null;
    }

    public static PartitionField maxValueOf(DataType fieldType) {
        MySQLStandardFieldType standardFieldType = fieldType.fieldType();
        switch (standardFieldType) {
        case MYSQL_TYPE_LONGLONG:
            return fieldType.isUnsigned() ? BigIntPartitionField.MAX_UNSIGNED_VALUE : BigIntPartitionField.MAX_VALUE;
        case MYSQL_TYPE_LONG:
            return fieldType.isUnsigned() ? IntPartitionField.MAX_UNSIGNED_VALUE : IntPartitionField.MAX_VALUE;
        }
        return null;
    }

    /**
     * The type of dynamic params are strictly limited, so we can infer the type from object class.
     */
    @Deprecated
    public static DataType typeOfDynamic(Object dynamicParam) {
        if (dynamicParam instanceof Integer || dynamicParam instanceof Long) {
            return new LongType();
        } else if (dynamicParam instanceof BigInteger) {
            return new ULongType();
        } else if (dynamicParam instanceof BigDecimal || dynamicParam instanceof Decimal) {
            return new DecimalType();
        } else {
            // todo util now, we don't support collation literal
            return new VarcharType();
        }
    }

    /**
     * Test if 'value' is comparable to 'field' when setting up range
     * access for predicate "field OP value". 'field' is a field in the
     * table being optimized for while 'value' is whatever 'field' is
     * compared to.
     *
     * @param fieldType The type of partition field.
     * @param resultType The type of value to be partitioned.
     * @return comparable
     */
    public static boolean isComparable(DataType fieldType, DataType resultType) {
        //  Usually an index cannot be used if the column collation
        //  differs from the operation collation.
        if (isStringResultType(fieldType)
            && needCollation(fieldType)
            && isStringResultType(resultType)
            && !isCollationComparable(fieldType, resultType)) {
            return false;
        }

        //  Temporal values: Cannot use range access if:
        //  'indexed_varchar_column = temporal_value'
        //  because there are many ways to represent the same date as a
        //  string. A few examples: "01-01-2001", "1-1-2001", "2001-01-01",
        //  "2001#01#01". The same problem applies to time. Thus, we cannot
        //  create a useful range predicate for temporal values into VARCHAR
        //  column indexes.
        if (isStringResultType(fieldType) && isTemporalType(resultType)) {
            return false;
        }

        //  Temporal values: Cannot use range access if
        //  'indexed_time = temporal_value_with_date_part'
        //  because:
        //  - without index, a TIME column with value '48:00:00' is
        //    equal to a DATETIME column with value
        //   'CURDATE() + 2 days'
        //  - with range access into the TIME column, CURDATE() + 2
        //    days becomes "00:00:00" (Field_timef::store_internal()
        //    simply extracts the time part from the datetime) which
        //    is a lookup key which does not match "48:00:00". On the other
        //    hand, we can do ref access for IndexedDatetimeComparedToTime
        //    because Field_temporal_with_date::store_time() will convert
        //    48:00:00 to CURDATE() + 2 days which is the correct lookup
        //    key.
        if (isTemporalType(fieldType)
            && !isTemporalTypeWithDate(fieldType)
            && isTemporalTypeWithDate(resultType)) {
            return false;
        }

        //    We can't always use indexes when comparing a string index to a
        //    number. cmp_type() is checked to allow comparison of dates and
        //    numbers.
        if (isStringResultType(fieldType)
            && !isStringResultType(resultType)) {
            return false;
        }

        //    We can't use indexes when comparing to a JSON value. For example,
        //    the string '{}' should compare equal to the JSON string "{}". If
        //    we use a string index to compare the two strings, we will be
        //    comparing '{}' and '"{}"', which don't compare equal.
        return !isStringResultType(fieldType)
            || !(resultType instanceof JsonType);
    }

    /**
     * Compare the value stored in field with the expression from the query.
     * <p>
     * We use this in the range optimizer/partition pruning,
     * because in some cases we can't store the value in the field
     * without some precision/character loss.
     * <p>
     * We similarly use it to verify that expressions like
     * BIGINT_FIELD <cmp> <literal value>
     * is done correctly (as int/decimal/float according to literal type).
     *
     * @param field Field which the Item is stored in after conversion.
     * @param value Original expression from query.
     * @return an integer greater than, equal to, or less than 0 if
     * the value stored in the field is greater than, equal to,
     * or less than the original Item. A 0 may also be returned if
     * out of memory.
     */
    public static int compareStoredFieldToValue(PartitionField field, Object value, DataType<?> valueType,
                                                SessionProperties sessionProperties) {
        // handle null value.
        if (field.isNull() && value == null) {
            return 0;
        } else if (field.isNull()) {
            return -1;
        } else if (value == null) {
            return 1;
        }

        DataType fieldType = field.dataType();
        MySQLResultType resType =
            getCompareType(valueType.fieldType().toResultType(), fieldType.fieldType().toResultType());

        if (resType == INT_RESULT) {
            // nothing different if both side are int type.
            return 0;
        } else if (fieldType.fieldType() == MYSQL_TYPE_TIME && valueType.fieldType() == MYSQL_TYPE_TIME) {
            // Don't support time type util now.
            // We should preserve this branch for time field support.
            return 0;
        } else if (isTemporalTypeWithDate(fieldType) && isTemporalType(valueType)) {
            // compare packed long
            OriginalTemporalValue temporalValue = (OriginalTemporalValue) valueType.convertFrom(value);
            if (temporalValue == null) {
                // handle null value.
                return 1;
            }
            long packedLong = temporalValue.getMysqlDateTime().toPackedLong();
            long storedPackedLong = field.datetimeValue(0, sessionProperties).toPackedLong();

            return storedPackedLong < packedLong ? -1 : storedPackedLong > packedLong ? 1 : 0;
        } else if (resType == STRING_RESULT) {
            if (value == null) {
                return 0;
            }
            Slice storedStr = GeneralUtil.coalesce(field.stringValue(sessionProperties), Slices.EMPTY_SLICE);

            if (isTemporalTypeWithDate(fieldType)) {
                // compare temporal field with string value.
                Slice temporalValueStr = DataTypes.VarcharType.convertFrom(value);
                if (temporalValueStr == null) {
                    // handle null value.
                    return 1;
                }

                // get time parser flag from session properties.
                int flag = TimeParserFlags.FLAG_TIME_FUZZY_DATE | TimeParserFlags.FLAG_TIME_INVALID_DATES;
                long sqlModeFlag = sessionProperties.getSqlModeFlag();
                if (SQLModeFlags.check(sqlModeFlag, SQLModeFlags.MODE_NO_ZERO_DATE)) {
                    flag |= TimeParserFlags.FLAG_TIME_NO_ZERO_DATE;
                }
                if (SQLModeFlags.check(sqlModeFlag, SQLModeFlags.MODE_NO_ZERO_IN_DATE)) {
                    flag |= TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE;
                }

                MysqlDateTime mysqlDateTime = StringTimeParser.parseDatetime(temporalValueStr.getBytes(), flag);
                MysqlDateTime fieldMysqlDatetime = StringTimeParser.parseDatetime(storedStr.getBytes(), flag);

                long unsignedComparableLong = mysqlDateTime == null ? 0L : mysqlDateTime.toUnsignedLong();
                long storedUnsignedComparableLong =
                    fieldMysqlDatetime == null ? 0L : fieldMysqlDatetime.toUnsignedLong();
                return UnsignedLongs.compare(storedUnsignedComparableLong, unsignedComparableLong);
            } else if (fieldType instanceof SliceType) {
                // compare field in slice type.
                Slice valueStr = ((SliceType) fieldType).convertFrom(value);
                if (valueStr == null) {
                    // handle null value.
                    return 1;
                }
                return ((SliceType) fieldType).getCollationHandler().compareSp(storedStr, valueStr);
            } else {
                String valueStr = DataTypes.StringType.convertFrom(value);
                if (valueStr == null) {
                    // handle null value.
                    return 1;
                }
                return storedStr.toStringUtf8().compareTo(valueStr);
            }
        } else if (resType == DECIMAL_RESULT) {
            // compare decimal value.
            Decimal decimalVal = DataTypes.DecimalType.convertFrom(value);
            if (decimalVal == null) {
                // handle null value.
                return 1;
            }
            Decimal storedDecimalVal = field.decimalValue(sessionProperties);
            return storedDecimalVal.compareTo(decimalVal);
        } else if (resType == REAL_RESULT) {
            Double doubleValue = DataTypes.DoubleType.convertFrom(value);
            if (doubleValue == null) {
                return 1;
            }

            Double storedDoubleValue = field.doubleValue(sessionProperties);
            return storedDoubleValue < doubleValue ? -1 : storedDoubleValue > doubleValue ? 1 : 0;
        }
        // And we don't consider row type
        return 0;
    }

    static PredicateBoolean isAlwaysTrueOrFalse(PartitionField field, TypeConversionStatus conversionStatus,
                                   SqlKind comparisonKind) {
        DataType fieldType = field.dataType();
        MySQLStandardFieldType standardFieldType = field.mysqlStandardFieldType();
        MySQLResultType resultType = standardFieldType.toResultType();
        switch (conversionStatus) {
        case TYPE_OK:
        case TYPE_NOTE_TRUNCATED:
        case TYPE_WARN_TRUNCATED:
            return IS_NOT_ALWAYS_TRUE_OR_FALSE;
        case TYPE_WARN_INVALID_STRING:
            // An invalid string does not produce any rows when used with
            // equality operator.
            if (comparisonKind == EQUALS) {
                return IS_ALWAYS_FALSE;
            }
            // For other operations on invalid strings, we assume that the range
            // predicate is always true and let evaluate_join_record() decide
            // the outcome.
            return IS_ALWAYS_TRUE;
        case TYPE_ERR_BAD_VALUE:
            // In the case of incompatible values, MySQL's SQL dialect has some
            // strange interpretations. For example,
            //
            //   "int_col > 'foo'" is interpreted as "int_col > 0"
            //
            // instead of always false. Because of this, we assume that the
            // range predicate is always true instead of always false and let
            // evaluate_join_record() decide the outcome.
            return IS_ALWAYS_TRUE;
        case TYPE_ERR_NULL_CONSTRAINT_VIOLATION:
            // Checking NULL value on a field that cannot contain NULL.
            // impossible_cond_cause= "null_field_in_non_null_column";
            return IS_ALWAYS_FALSE;
        case TYPE_WARN_OUT_OF_RANGE:
            // value to store was either higher than field::max_value or lower
            // than field::min_value. The field's max/min value has been stored
            // instead.
            if (comparisonKind == EQUALS) {
                // Independent of data type, "out_of_range_value =/<=> field" is
                // always false.
                return IS_ALWAYS_FALSE;
            }

            // If the field is numeric, we can interpret the out of range value.
            if ((standardFieldType != MYSQL_TYPE_BIT)
                && (resultType == REAL_RESULT || resultType == INT_RESULT || resultType == DECIMAL_RESULT)) {

                // value to store was higher than field::max_value if
                //  a) field has a value greater than 0, or
                //  b) if field is unsigned and has a negative value (which, when
                // cast to unsigned, means some value higher than LLONG_MAX).
                if (field.longValue() > 0 ||
                    (fieldType.isUnsigned() && field.longValue() < 0)) {
                    if (comparisonKind == LESS_THAN || comparisonKind == LESS_THAN_OR_EQUAL) {
                        // '<' or '<=' compared to a value higher than the field
                        // can store is always true.
                        return IS_ALWAYS_TRUE;
                    }
                    if (comparisonKind == GREATER_THAN || comparisonKind == GREATER_THAN_OR_EQUAL) {
                        //  '>' or '>=' compared to a value higher than the field can
                        //  store is always false.
                        return IS_ALWAYS_FALSE;
                    }
                } else {
                    // value is lower than field::min_value
                    if (comparisonKind == GREATER_THAN || comparisonKind == GREATER_THAN_OR_EQUAL) {
                        // '>' or '>=' compared to a value lower than the field
                        // can store is always true.
                        return IS_ALWAYS_TRUE;
                    }
                    if (comparisonKind == LESS_THAN || comparisonKind == LESS_THAN_OR_EQUAL) {
                        //  '<' or '=' compared to a value lower than the field can
                        // store is always false.
                        return IS_ALWAYS_FALSE;
                    }
                }
            }
            // Value is out of range on a datatype where it can't be decided if
            // it was underflow or overflow. It is therefore not possible to
            // determine whether or not the condition is impossible or always
            // true and we have to assume always true.
            return IS_ALWAYS_TRUE;
        case TYPE_NOTE_TIME_TRUNCATED:
            if (standardFieldType == MYSQL_TYPE_DATE &&
                (comparisonKind == GREATER_THAN || comparisonKind == GREATER_THAN_OR_EQUAL ||
                    comparisonKind == LESS_THAN || comparisonKind == LESS_THAN_OR_EQUAL)) {
                // We were saving DATETIME into a DATE column, the conversion went ok
                // but a non-zero time part was cut off.
                //
                // In MySQL's SQL dialect, DATE and DATETIME are compared as datetime
                // values. Index over a DATE column uses DATE comparison. Changing
                // from one comparison to the other is possible:
                //
                // datetime(date_col)< '2007-12-10 12:34:55' -> date_col<='2007-12-10'
                // datetime(date_col)<='2007-12-10 12:34:55' -> date_col<='2007-12-10'
                //
                // datetime(date_col)> '2007-12-10 12:34:55' -> date_col>='2007-12-10'
                // datetime(date_col)>='2007-12-10 12:34:55' -> date_col>='2007-12-10'
                //
                // but we'll need to convert '>' to '>=' and '<' to '<='. This will
                // be done together with other types at the end of get_mm_leaf()
                // (grep for stored_field_cmp_to_item)
                return IS_NOT_ALWAYS_TRUE_OR_FALSE;
            }
            if (comparisonKind == EQUALS) {
                // Equality comparison is always false when time info has been truncated.
                return IS_ALWAYS_FALSE;
            }
            return IS_ALWAYS_TRUE;
        case TYPE_ERR_OOM:
            return IS_ALWAYS_TRUE;
        // No default here to avoid adding new conversion status codes that are
        // unhandled in this function.
        }
        return IS_ALWAYS_TRUE;
    }

    private static MySQLResultType getCompareType(MySQLResultType l, MySQLResultType r) {
        MySQLResultType resType;

        if (l == STRING_RESULT && r == STRING_RESULT) {
            resType = STRING_RESULT;
        } else if (l == INT_RESULT && r == INT_RESULT) {
            resType = MySQLResultType.INT_RESULT;
        } else if ((l == INT_RESULT || l == DECIMAL_RESULT)
            && (r == INT_RESULT || r == DECIMAL_RESULT)) {
            resType = MySQLResultType.DECIMAL_RESULT;
        } else {
            resType = MySQLResultType.REAL_RESULT;
        }
        return resType;
    }

    /**
     * For enum Item_result::STRING_RESULT
     */
    private static boolean isStringResultType(DataType dataType) {
        return dataType.fieldType().toResultType() == STRING_RESULT;
    }

    /**
     * Is Temporal type with or without date
     */
    private static boolean isTemporalType(DataType dataType) {
        MySQLStandardFieldType fieldType = dataType.fieldType();
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

    /**
     * Is Temporal type with date value.
     */
    private static boolean isTemporalTypeWithDate(DataType dataType) {
        MySQLStandardFieldType fieldType = dataType.fieldType();
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

    /**
     * Check if this type need collation calculation.
     */
    private static boolean needCollation(DataType dataType) {
        MySQLStandardFieldType fieldType = dataType.fieldType();
        switch (fieldType) {
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if collations from both side are comparable.
     */
    private static boolean isCollationComparable(DataType fieldType, DataType resultType) {
        return resultType.isUtf8Encoding()
            || fieldType.getCollationName() == resultType.getCollationName();
    }
}
