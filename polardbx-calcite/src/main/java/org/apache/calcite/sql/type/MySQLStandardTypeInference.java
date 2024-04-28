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

package org.apache.calcite.sql.type;

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperatorBinding;

import java.math.BigInteger;
import java.util.Optional;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT_UNSIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;

/**
 * "this method must be invoke during validating, rather than RexNode building.");
 * Standard MySQL type inference rules.
 */
public class MySQLStandardTypeInference {
    /**
     * Suitable for unary minus operator.
     */
    public static final SqlReturnTypeInference UNARY_MINUS_OPERATOR_RETURN_TYPE = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            RelDataTypeFactory factory = opBinding.getTypeFactory();
            SqlTypeName returnTypeName;

            RelDataType arg0Type = opBinding.getOperandType(0);
            if (SqlTypeUtil.isIntType(arg0Type)) {
                returnTypeName = BIGINT;
            } else if (SqlTypeUtil.isString(arg0Type) || SqlTypeUtil.isApproximateNumeric(arg0Type)) {
                returnTypeName = DOUBLE;
            } else {
                returnTypeName = DECIMAL;
            }

            return factory.createSqlType(returnTypeName);
        }
    };

    /**
     * Suitable for mod operator.
     */
    public static final SqlReturnTypeInference MOD_OPERATOR_RETURN_TYPE = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            RelDataTypeFactory factory = opBinding.getTypeFactory();
            SqlTypeName returnTypeName;

            RelDataType arg0Type = opBinding.getOperandType(0);
            RelDataType arg1Type = opBinding.getOperandType(1);

            if (SqlTypeUtil.isApproximateNumeric(arg0Type)
                || SqlTypeUtil.isString(arg0Type)
                || SqlTypeUtil.isApproximateNumeric(arg1Type)
                || SqlTypeUtil.isString(arg1Type)) {
                // Since DATE/TIME/DATETIME data types return INT_RESULT/DECIMAL_RESULT
                // type codes, we should never get to here when both fields are temporal.
                returnTypeName = DOUBLE;
            } else if (SqlTypeUtil.isDecimal(arg0Type) || SqlTypeUtil.isDecimal(arg1Type)) {
                returnTypeName = DECIMAL;
            } else {
                boolean isUnsigned = SqlTypeUtil.isUnsigned(arg0Type);
                returnTypeName = isUnsigned ? BIGINT_UNSIGNED : BIGINT;
            }

            return factory.createSqlType(returnTypeName);
        }
    };

    /**
     * Suitable for divide operator.
     */
    public static final SqlReturnTypeInference DIVIDE_OPERATOR_RETURN_TYPE = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            RelDataTypeFactory factory = opBinding.getTypeFactory();
            SqlTypeName returnTypeName;

            RelDataType arg0Type = opBinding.getOperandType(0);
            RelDataType arg1Type = opBinding.getOperandType(1);

            if (SqlTypeUtil.isApproximateNumeric(arg0Type)
                || SqlTypeUtil.isString(arg0Type)
                || SqlTypeUtil.isApproximateNumeric(arg1Type)
                || SqlTypeUtil.isString(arg1Type)) {
                // Since DATE/TIME/DATETIME data types return INT_RESULT/DECIMAL_RESULT
                // type codes, we should never get to here when both fields are temporal.
                returnTypeName = DOUBLE;
            } else {
                returnTypeName = DECIMAL;
            }

            return factory.createSqlType(returnTypeName);
        }
    };

    /**
     * Set result type for abs function of one argument
     * (can be also used by a numeric function of many arguments, if the result
     * type depends only on the first argument)
     */
    public static final SqlReturnTypeInference ABS_RETURN_TYPE = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            RelDataTypeFactory factory = opBinding.getTypeFactory();
            SqlTypeName returnTypeName = DOUBLE;

            RelDataType arg0Type = opBinding.getOperandType(0);

            // result_type == REAL_RESULT or STRING_RESULT
            if (SqlTypeUtil.isIntType(arg0Type)) {
                returnTypeName = BIGINT;
            } else if (SqlTypeUtil.isApproximateNumeric(arg0Type)
                || SqlTypeUtil.isString(arg0Type)
                || SqlTypeUtil.isDatetime(arg0Type)) {
                returnTypeName = DOUBLE;
            } else if (SqlTypeUtil.isDecimal(arg0Type)) {
                returnTypeName = DECIMAL;
            }

            return factory.createSqlType(returnTypeName);
        }
    };

    /**
     * - If first arg is a MYSQL_TYPE_DATETIME result is MYSQL_TYPE_DATETIME
     * - If first arg is a MYSQL_TYPE_DATE and the interval type uses hours,
     * minutes or seconds then type is MYSQL_TYPE_DATETIME.
     * - Otherwise the result is MYSQL_TYPE_STRING
     * (This is because you can't know if the string contains a DATE, MYSQL_TIME or
     * DATETIME argument)
     */
    public static final SqlReturnTypeInference DATE_ADD_INTERVAL = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            RelDataType operandType = opBinding.getOperandType(0);
            MySQLIntervalType intervalType;
            SqlCall intervalPrimary = null;
            SqlCall call;
            int intervalDecimal = 0;
            if (!(opBinding instanceof SqlCallBinding)
                && opBinding.getOperandType(1) instanceof IntervalSqlType) {

                // ADDDATE(date,INTERVAL expr unit)
                // SUBDATE(date,INTERVAL expr unit),
                // DATE_ADD(date,INTERVAL expr unit)
                // DATE_SUB(date,INTERVAL expr unit)
                //
                //       DATE_ADD
                //       /      \
                //   TIME   INTERVAL_PRIMARY
                //                 /    \
                //           VALUE        TIME_UNIT

                // for interval type
                IntervalSqlType intervalSqlType = (IntervalSqlType) opBinding.getOperandType(1);
                intervalType = MySQLIntervalType.of(intervalSqlType.getIntervalQualifier().timeUnitRange.name());

                // for time value scale
                int timeValueScale = Math.max(intervalSqlType.getScale(), MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);

                // scale of interval
                if (intervalType == MySQLIntervalType.INTERVAL_MICROSECOND
                    || intervalType == MySQLIntervalType.INTERVAL_DAY_MICROSECOND
                    || intervalType == MySQLIntervalType.INTERVAL_SECOND_MICROSECOND) {
                    intervalDecimal = MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE;
                } else if (intervalType == MySQLIntervalType.INTERVAL_SECOND && timeValueScale > 0) {
                    // for second interval
                    intervalDecimal = Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, timeValueScale);
                }

            } else if (opBinding instanceof SqlCallBinding
                && (call = ((SqlCallBinding) opBinding).getCall()).getOperandList().get(1) instanceof SqlCall
                && ((SqlCall) call.getOperandList().get(1)).getOperator().getName().equals("INTERVAL_PRIMARY")) {

                // ADDDATE(date,INTERVAL expr unit)
                // SUBDATE(date,INTERVAL expr unit),
                // DATE_ADD(date,INTERVAL expr unit)
                // DATE_SUB(date,INTERVAL expr unit)
                //
                //       DATE_ADD
                //       /      \
                //   TIME   INTERVAL_PRIMARY
                //                 /    \
                //           VALUE        TIME_UNIT

                // for interval type
                intervalPrimary = (SqlCall) call.getOperandList().get(1);
                SqlIntervalQualifier interval = (SqlIntervalQualifier) intervalPrimary.getOperandList().get(1);
                intervalType = MySQLIntervalType.of(interval.toString());

                // for time value scale
                int timeValueScale;
                SqlNode intervalPrimaryArg0 = intervalPrimary.getOperandList().get(0);
                if (intervalPrimaryArg0 instanceof SqlNumericLiteral) {
                    timeValueScale = ((SqlNumericLiteral) intervalPrimaryArg0).getScale();
                } else {
                    timeValueScale = 0;
                }

                // scale of interval
                if (intervalType == MySQLIntervalType.INTERVAL_MICROSECOND
                    || intervalType == MySQLIntervalType.INTERVAL_DAY_MICROSECOND
                    || intervalType == MySQLIntervalType.INTERVAL_SECOND_MICROSECOND) {
                    intervalDecimal = MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE;
                } else if (intervalType == MySQLIntervalType.INTERVAL_SECOND && timeValueScale > 0) {
                    // for second interval
                    intervalDecimal = Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, timeValueScale);
                }
            } else {

                // ADDDATE(expr,days),
                // SUBDATE(expr,days),
                //
                //       DATE_ADD
                //       /      \
                //   TIME        DAYS
                intervalType = MySQLIntervalType.INTERVAL_DAY;
            }

            if (isDatetimeOrTimestamp(operandType)) {
                // not support fractional second interval
                // if microsecond, scale = 6
                int scale = Math.max(operandType.getScale(), intervalDecimal);
                return typeFactory.createSqlType(
                    SqlTypeName.DATETIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                    scale
                );
            } else if (SqlTypeUtil.isDate(operandType)) {
                if (intervalType == MySQLIntervalType.INTERVAL_DAY
                    || intervalType == MySQLIntervalType.INTERVAL_MONTH
                    || intervalType == MySQLIntervalType.INTERVAL_YEAR
                    || intervalType == MySQLIntervalType.INTERVAL_QUARTER
                    || intervalType == MySQLIntervalType.INTERVAL_WEEK) {
                    // to date
                    return typeFactory.createSqlType(
                        SqlTypeName.DATE
                    );
                } else {
                    // to datetime
                    return typeFactory.createSqlType(
                        SqlTypeName.DATETIME,
                        typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                        intervalDecimal
                    );
                }

            } else if (SqlTypeUtil.isTime(operandType)) {
                int scale = Math.max(operandType.getScale(), intervalDecimal);
                return typeFactory.createSqlType(
                    SqlTypeName.TIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                    scale
                );
            } else {
                // dynamic scale
                return typeFactory.createSqlType(SqlTypeName.VARCHAR);
            }
        }
    };

    public static final SqlReturnTypeInference TIMESTAMP_ADD = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            RelDataType operandType = opBinding.getOperandType(2);

            //        TIMESTAMP_ADD
            //      /       |       \
            //   UNIT   TIME_VALUE  TIME
            String interval = opBinding.getOperandLiteralValue(0, String.class);
            MySQLIntervalType intervalType = MySQLIntervalType.of(interval);

            // scale of interval
            int intervalDecimal = 0;
            int timeValueScale = 0;
            if (intervalType == MySQLIntervalType.INTERVAL_MICROSECOND
                || intervalType == MySQLIntervalType.INTERVAL_DAY_MICROSECOND
                || intervalType == MySQLIntervalType.INTERVAL_SECOND_MICROSECOND) {
                intervalDecimal = MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE;
            } else if (intervalType == MySQLIntervalType.INTERVAL_SECOND && timeValueScale > 0) {
                // for second interval
                intervalDecimal = Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, timeValueScale);
            }

            if (isDatetimeOrTimestamp(operandType)) {
                // not support fractional second interval
                // if microsecond, scale = 6
                int scale = Math.max(operandType.getScale(), intervalDecimal);
                return typeFactory.createSqlType(
                    SqlTypeName.DATETIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                    scale
                );
            } else if (SqlTypeUtil.isDate(operandType)) {
                if (intervalType == MySQLIntervalType.INTERVAL_DAY
                    || intervalType == MySQLIntervalType.INTERVAL_MONTH
                    || intervalType == MySQLIntervalType.INTERVAL_YEAR
                    || intervalType == MySQLIntervalType.INTERVAL_QUARTER
                    || intervalType == MySQLIntervalType.INTERVAL_WEEK) {
                    // to date
                    return typeFactory.createSqlType(
                        SqlTypeName.DATE
                    );
                } else {
                    // to datetime
                    return typeFactory.createSqlType(
                        SqlTypeName.DATETIME,
                        typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                        intervalDecimal
                    );
                }

            } else if (SqlTypeUtil.isTime(operandType)) {
                int scale = Math.max(operandType.getScale(), intervalDecimal);
                return typeFactory.createSqlType(
                    SqlTypeName.TIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                    scale
                );
            } else {
                // dynamic scale
                return typeFactory.createSqlType(SqlTypeName.VARCHAR);
            }
        }
    };

    /**
     * - If first arg is a MYSQL_TYPE_DATETIME or MYSQL_TYPE_TIMESTAMP
     * result is MYSQL_TYPE_DATETIME
     * - If first arg is a MYSQL_TYPE_TIME result is MYSQL_TYPE_TIME
     * - Otherwise the result is MYSQL_TYPE_STRING
     * <p>
     * when the first argument is MYSQL_TYPE_DATE.
     */
    public static final SqlReturnTypeInference ADD_TIME = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            //    ADD_TIME
            //     /    \
            //  time1   time2
            RelDataType operandType1 = opBinding.getOperandType(0);
            RelDataType operandType2 = opBinding.getOperandType(1);

            if (SqlTypeUtil.isTime(operandType1)) {
                // 无法兼容 arg[1] 参数化的情况 （动态精度）
                // 建议用户将 addtime(col, ?)
                // 写成 addtime(col, cast(? as time(fsp)))
                int scale = Math.max(dynamicTemporalScale(operandType1), dynamicTemporalScale(operandType2));

                return typeFactory.createSqlType(
                    SqlTypeName.TIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                    scale
                );
            } else if (isDatetimeOrTimestamp(operandType1)
                || (InstanceVersion.isMYSQL80() && SqlTypeUtil.isDate(operandType1))) {
                int scale = Math.max(dynamicTemporalScale(operandType1), dynamicTemporalScale(operandType2));
                return typeFactory.createSqlType(
                    SqlTypeName.DATETIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                    scale
                );
            } else {
                // dynamic scale
                return typeFactory.createSqlType(SqlTypeName.VARCHAR);
            }
        }
    };

    /**
     * TIMEDIFF(t,s) is a time function that calculates the
     * time value between a start and end time.
     */
    public static final SqlReturnTypeInference TIME_DIFF = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            // 无法兼容 参数化的情况 （动态精度）
            RelDataType operandType1 = opBinding.getOperandType(0);
            RelDataType operandType2 = opBinding.getOperandType(1);

            int scale1 = dynamicTemporalScale(operandType1);
            int scale2 = dynamicTemporalScale(operandType2);
            int scale = Math.max(scale1, scale2);
            return typeFactory.createSqlType(
                SqlTypeName.TIME,
                typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                scale
            );
        }
    };

    /**
     * SEC_TO_TIME(seconds)
     * Returns the seconds argument, converted to hours, minutes, and seconds, as a TIME value. The
     * range of the result is constrained to that of the TIME data type. A warning occurs if the argument
     * corresponds to a value outside that range.
     */
    public static final SqlReturnTypeInference SEC_TO_TIME = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            RelDataType operandType = opBinding.getOperandType(0);

            int scale = dynamicTemporalScale(operandType);
            return typeFactory.createSqlType(
                SqlTypeName.TIME,
                typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                scale
            );
        }
    };

    /**
     * FROM_UNIXTIME(unix_timestamp[,format])
     * <p>
     * Returns a representation of unix_timestamp as a datetime or character string value.
     * If format is omitted, this function returns a DATETIME value.
     * If unix_timestamp is an integer, the fractional seconds precision of the DATETIME is zero.
     * When unix_timestamp is a decimal value, the fractional seconds precision of the DATETIME is the same as the precision of the decimal value, up to a maximum of 6.
     * When unix_timestamp is a floating point number, the fractional seconds precision of the datetime is 6.
     * format is used to format the result in the same way as the format string used for the DATE_FORMAT() function. If format is supplied, the value returned is a VARCHAR.
     */
    public static final SqlReturnTypeInference FROM_UNIX_TIME_TYPE = new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            if (opBinding.getOperandCount() == 2) {
                return typeFactory.createSqlType(SqlTypeName.VARCHAR);
            }

            RelDataType operandType = opBinding.getOperandType(0);
            int scale;
            if (opBinding.isConstant(0)) {
                scale = dynamicTemporalScale(operandType);
            } else {
                scale = operandType.getScale();
            }

            return typeFactory.createSqlType(
                SqlTypeName.DATETIME,
                typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                scale);
        }
    };

    /**
     * FROM_DAYS(N)
     * Given a day number N, returns a DATE value.
     * get data type with 0 scale.
     */
    public static final SqlReturnTypeInference DATE_0 = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            return typeFactory.createSqlType(SqlTypeName.DATE);
        }
    };

    /**
     * MAKETIME(hour,minute,second)
     * Returns a time value calculated from the hour, minute, and second arguments.
     * The second argument can have a fractional part.
     */
    public static final SqlReturnTypeInference MAKE_TIME = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            return typeFactory.createSqlType(
                SqlTypeName.TIME,
                typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                6
            );
        }
    };

    /**
     * TIMESTAMP(expr), TIMESTAMP(expr1,expr2)
     * With a single argument, this function returns the date or datetime expression expr as a datetime
     * value. With two arguments, it adds the time expression expr2 to the date or datetime expression
     * expr1 and returns the result as a datetime value.
     */
    public static final SqlReturnTypeInference TIMESTAMP_FUNC = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            RelDataType operandType1 = opBinding.getOperandType(0);

            if (opBinding.getOperandCount() > 1) {
                // for timestamp(expr1, expr2)
                RelDataType operandType2 = opBinding.getOperandType(1);

                int scale1 = dynamicTemporalScale(operandType1);
                int scale2 = dynamicTemporalScale(operandType2);
                int scale = Math.max(scale1, scale2);

                return typeFactory.createSqlType(
                    SqlTypeName.DATETIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                    scale);
            } else {
                // for timestamp(expr)
                int scale = dynamicTemporalScale(operandType1);
                return typeFactory.createSqlType(
                    SqlTypeName.DATETIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                    scale);
            }

        }
    };

    /**
     * UNIX_TIMESTAMP([date])
     * If UNIX_TIMESTAMP() is called with no date argument, it returns a Unix timestamp representing
     * seconds since '1970-01-01 00:00:00' UTC.
     * If UNIX_TIMESTAMP() is called with a date argument, it returns the value of the argument as
     * seconds since '1970-01-01 00:00:00' UTC. The server interprets date as a value in the
     * session time zone and converts it to an internal Unix timestamp value in UTC.
     */
    public static final SqlReturnTypeInference UNIX_TIMESTAMP = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            if (opBinding.getOperandCount() == 0) {
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            }
            RelDataType operandType1 = opBinding.getOperandType(0);

            int scale = dynamicTemporalScale(operandType1);

            if (scale <= 0) {
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            } else {
                return typeFactory.createSqlType(
                    SqlTypeName.DECIMAL,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL),
                    scale
                );
            }
        }
    };

    /**
     * TIME()
     */
    public static final SqlReturnTypeInference DYNAMIC_TIME = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            // time(expr)
            RelDataType operandType1 = opBinding.getOperandType(0);

            int scale = dynamicTemporalScale(operandType1);

            return typeFactory.createSqlType(
                SqlTypeName.TIME,
                typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                scale
            );
        }
    };

    /**
     * NOW(fsp) NOW()
     * CURRENT_TIMESTAMP(fsp) CURRENT_TIMESTAMP()
     * LOCALTIME LOCALTIME(fsp)
     * LOCALTIMESTAMP LOCALTIMESTAMP(fsp)
     * SYSDATE()
     */
    public static final SqlReturnTypeInference DATETIME_WITH_FSP = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            int scale;
            if (opBinding.getOperandCount() == 0) {
                // for now()
                scale = 0;
            } else {
                try {
                    scale = opBinding.getOperandLiteralValue(0, Integer.class);
                } catch (Throwable t) {
                    scale = 6;
                }
            }

            return typeFactory.createSqlType(
                SqlTypeName.DATETIME,
                typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                scale
            );
        }
    };

    /**
     * CURRENT_TIME, CURRENT_TIME([fsp]), CURTIME([fsp])
     */
    public static final SqlReturnTypeInference TIME_WITH_FSP = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            int scale;
            if (opBinding.getOperandCount() == 0) {
                // for curtime()
                scale = 0;
            } else {
                // for curtime(fsp)
                try {
                    scale = opBinding.getOperandLiteralValue(0, Integer.class);
                } catch (Throwable t) {
                    scale = 6;
                }
            }

            return typeFactory.createSqlType(
                SqlTypeName.TIME,
                typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                scale
            );
        }
    };

    /**
     * CONVERT_TZ(dt,from_tz,to_tz)
     * CONVERT_TZ() converts a datetime value dt from the time zone given by from_tz to the time
     * zone given by to_tz and returns the resulting value.
     */
    public static final SqlReturnTypeInference CONVERT_TZ = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            RelDataType operandType1 = opBinding.getOperandType(0);

            int scale = dynamicTemporalScale(operandType1);

            return typeFactory.createSqlType(
                SqlTypeName.DATETIME,
                typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                scale);
        }
    };

    private static final String TIME_PART_FORMATS = "HISThiklrs";
    private static final String DATE_PART_FORMATS = "MVUXYWabcjmvuxyw";

    public static final SqlReturnTypeInference STR_TO_DATE = new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            String format;
            if (!opBinding.isOperandLiteral(1, false)
                || (format = opBinding.getOperandLiteralValue(1, String.class)) == null
                || format.length() == 0) {
                return typeFactory.createSqlType(
                    SqlTypeName.DATETIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                    MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);
            }

            byte[] formatAsBytes = format.getBytes();
            boolean fracSecondUsed = false, timePartUsed = false, datePartUsed = false;
            for (int i = 0; i < formatAsBytes.length; i++) {
                if (formatAsBytes[i] == '%' & i + 1 < formatAsBytes.length) {
                    i++;
                    if (formatAsBytes[i] == 'f') {
                        fracSecondUsed = true;
                        timePartUsed = true;
                    } else if (!timePartUsed && TIME_PART_FORMATS.indexOf(formatAsBytes[i]) != -1) {
                        timePartUsed = true;
                    } else if (!datePartUsed && DATE_PART_FORMATS.indexOf(formatAsBytes[i]) != -1) {
                        datePartUsed = true;
                    }

                    if (datePartUsed && fracSecondUsed) {
                        // frac_second_used implies time_part_used, and thus we already
                        // have all types of date-time components and can end our search.
                        return typeFactory.createSqlType(
                            SqlTypeName.DATETIME,
                            typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME),
                            MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);
                    }
                }
            }

            //  We don't have all three types of date-time components
            if (fracSecondUsed) {
                // TIME with microseconds
                return typeFactory.createSqlType(
                    SqlTypeName.TIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME),
                    MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);
            } else if (timePartUsed) {
                if (datePartUsed) {
                    // DATETIME, no microseconds
                    return typeFactory.createSqlType(
                        SqlTypeName.DATETIME,
                        typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DATETIME), 0);
                } else {
                    // TIME, no microseconds
                    return typeFactory.createSqlType(
                        SqlTypeName.TIME,
                        typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME), 0);
                }
            } else {
                // DATE
                return typeFactory.createSqlType(SqlTypeName.DATE);
            }
        }
    };

    /**
     * We ignore some code logic about decimal and length.
     * according to void Item_func_round::fix_length_and_dec()
     */
    public static final SqlReturnTypeInference ROUND_FUNCTION_TYPE = new SqlReturnTypeInference() {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            RelDataTypeFactory factory = opBinding.getTypeFactory();
            SqlTypeName returnTypeName = DOUBLE;
            RelDataType arg0Type = opBinding.getOperandType(0);
            boolean isUnsigned = SqlTypeUtil.isUnsigned(arg0Type);

            // Sometimes, a function call can be parameterized.
            // Util now, the parameter system cannot distinct sql call from normal parameter.
            if (opBinding.getOperandCount() >= 2 && !opBinding.isConstant(1)) {
                if (SqlTypeUtil.isDecimal(arg0Type)) {
                    returnTypeName = DECIMAL;
                } else {
                    returnTypeName = DOUBLE;
                }
                return factory.createSqlType(returnTypeName);
            }

            // result_type == REAL_RESULT or STRING_RESULT
            if (SqlTypeUtil.isApproximateNumeric(arg0Type)
                || SqlTypeUtil.isString(arg0Type)
                || SqlTypeUtil.isDatetime(arg0Type)) {
                returnTypeName = DOUBLE;
            } else if (SqlTypeUtil.isIntType(arg0Type)) {
                returnTypeName = isUnsigned ? BIGINT_UNSIGNED : BIGINT;
            } else if (SqlTypeUtil.isDecimal(arg0Type)) {
                returnTypeName = DECIMAL;
            }

            return factory.createSqlType(returnTypeName);
        }
    };

    /**
     * get temporal scale values
     */
    private static int dynamicTemporalScale(RelDataType operandType) {
        int scale;
        if ((SqlTypeUtil.isDatetime(operandType) && !SqlTypeUtil.isDate(operandType))
            || SqlTypeUtil.isTime(operandType)) {
            // for time / datetime / timestamp, use actual scale.
            scale = operandType.getScale();
        } else if (SqlTypeUtil.isString(operandType)
            || SqlTypeUtil.isApproximateNumeric(operandType)
            || SqlTypeUtil.isDecimal(operandType)) {
            // for string / double / float / decimal , preserve the maximum scale.
            scale = 6;
        } else {
            // for int / long / date, use 0 scale.
            scale = 0;
        }
        // avoid overflow
        return Math.min(scale, MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);
    }

    private static boolean isDatetimeOrTimestamp(RelDataType operandType) {
        return Optional.ofNullable(operandType)
            .map(RelDataType::getSqlTypeName)
            .map(
                t -> t == SqlTypeName.DATETIME
                    || t == SqlTypeName.TIMESTAMP
                    || t == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            )
            .orElse(false);
    }
}
