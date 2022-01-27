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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.IntervalString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Calendar;

/**
 * @author chenmo.cm
 */
public class RexLiteralTypeUtils {

    static Logger logger = LoggerFactory.getLogger(RexLiteralTypeUtils.class);

    /**
     *  The Table of FastSql change sql LiteralExpr to javaType
     *
     *   MySqlType    FastSqlLiteralType    -->    JavaType
     *     varchar      SQLNCharExpr                String
     *     Bool         SQLBooleanExpr              bool
     *     bigint       SQLBigIntExpr               Long
     *     decimal      SQLDecimalExpr              BigDecimal
     *     int          SQLIntegerExpr              Number
     *     real         SQLRealExpr                 Float
     *     smallint     SQLSmallIntExpr             Short
     *     tinyint      SQLTinyIntExpr              Byte
     *     number       SQLNumberExpr               Number
     *     varbinary    SQLHexExpr                  byte[]
     *
     *     timestamp    SQLTimestampExpr            String
     *     date         SQLDateExpr                 String
     *     time         SQLTimeExpr                 String
     *     json         SQLJSONExpr                 String
     *
     *     null         SQLNullExpr                 EVAL_VALUE_NULL(Object)
     *
     *
     */

    /**
     * convert jdbc type of value to the type that matchs RexLiteral Value, for
     * <p>
     * convert value to RexLitieral
     */
    public static Object convertJavaTypeToRexLiteralValueType(Object value, RelDataType dataType,
                                                              SqlTypeName typeName) {
        if (value == null) {
            return value;
        }
        switch (typeName) {
        case BOOLEAN:
            // Unlike SqlLiteral, we do not allow boolean null.

            if (value instanceof Boolean) {
                return value;
            } else {
                return Boolean.valueOf(String.valueOf(value));
            }

            //return value instanceof Boolean;

        case NULL:

            return value;

        //return false; // value should have been null

        case TINYINT:
        case INTEGER: // not allowed -- use Decimal
        case SMALLINT:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case REAL:
        case BIGINT:
        case SIGNED:
        case UNSIGNED:

            if (value instanceof BigDecimal) {
                return value;
            } else if (value instanceof Byte) {
                // For tinyint
                long v = (long) byteToInt((Byte) value);
                BigDecimal bd = new BigDecimal(v);
                return bd;
            } else {
                BigDecimal bd = new BigDecimal(String.valueOf(value));
                return bd;
            }
            //return value instanceof BigDecimal;

        case DATE:

            if (value instanceof DateString) {
                return value;
            } else {
                if (value instanceof String) {
                    DateString ds = new DateString((String) value);
                    return ds;
                } else {
                    throw Util.unexpected(typeName);
                }
            }

            //return value instanceof DateString;
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
            if (value instanceof TimeString) {
                return value;
            } else {
                if (value instanceof String) {
                    TimeString ts = new TimeString((String) value);
                    return ts;
                } else {
                    throw Util.unexpected(typeName);
                }
            }

            //return value instanceof TimeString;
            //return value instanceof TimeString;

        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:

            if (value instanceof TimestampString) {
                return value;
            } else {
                if (value instanceof String) {
                    TimestampString tss = new TimestampString((String) value);
                    return tss;
                } else {
                    throw Util.unexpected(typeName);
                }
            }

            //return value instanceof TimestampString;
            //return value instanceof TimestampString;
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:

            // All internal expr in CoronaDB will NOT be parameterized
            if (value instanceof IntervalString) {
                return value;
            } else {
                throw Util.unexpected(typeName);
            }

            //return value instanceof IntervalString;
        case VARBINARY: // not allowed -- use Binary
        case BINARY:

            if (value instanceof ByteString) {
                return value;
            } else if (value instanceof Byte) {

            } else {
                ByteString bs = null;
                if (value instanceof String) {
                    String v = (String) value;
                    try {
                        bs = new ByteString(v.getBytes("latin1"));
                    } catch (UnsupportedEncodingException e) {
                        //
                        logger.error(e);
                    }
                } else if (value instanceof byte[]) {
                    byte[] bytes = (byte[]) value;
                    bs = new ByteString(bytes);
                } else {
                    throw Util.unexpected(typeName);
                }
                return bs;

            }

            //return value instanceof ByteString;

        case VARCHAR: // not allowed -- use Char
        case CHAR:

            if (value instanceof NlsString) {
                return value;

            } else {
                NlsString nls = new NlsString(DataTypes.StringType.convertFrom(value), dataType.getCharset().name(),
                    dataType.getCollation());
                return nls;
            }

            // A SqlLiteral's charset and collation are optional; not so a
            // RexLiteral.
            //return (value instanceof NlsString) && (((NlsString) value).getCharset() != null)
            //  && (((NlsString) value).getCollationName() != null);

        case SYMBOL:
            return value instanceof Enum;
        case ROW:
        case MULTISET:
        case ANY:
            throw Util.unexpected(typeName);
        default:
            throw Util.unexpected(typeName);
        }
    }

    public static int byteToInt(byte b) {
        //Java 总是把 byte 当做有符处理；我们可以通过将其和 0xFF 进行二进制与得到它的无符值
        return b & 0xFF;
    }

    /**
     * The Table of FastSql change sql LiteralExpr to javaType
     * <p>
     * MySqlType    FastSqlLiteralType    -->    JavaType
     * varchar      SQLNCharExpr                String
     * Bool         SQLBooleanExpr              bool
     * bigint       SQLBigIntExpr               Long
     * decimal      SQLDecimalExpr              BigDecimal
     * int          SQLIntegerExpr              Number
     * real         SQLRealExpr                 Float
     * smallint     SQLSmallIntExpr             Short
     * tinyint      SQLTinyIntExpr              Byte
     * number       SQLNumberExpr               Number
     * varbinary    SQLHexExpr                  byte[]
     * <p>
     * timestamp    SQLTimestampExpr            String
     * date         SQLDateExpr                 String
     * time         SQLTimeExpr                 String
     * json         SQLJSONExpr                 String
     * <p>
     * null         SQLNullExpr                 EVAL_VALUE_NULL(Object)
     */
    public static RexLiteral convertJavaObjectToRexLiteral(Object value, RelDataType type, SqlTypeName typeName,
                                                           RexBuilder rexBuilder) {

        Object rexLiterValObj = null;
        switch (typeName) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:

            if (value instanceof BigDecimal) {
                rexLiterValObj = value;
            } else {
                rexLiterValObj = new BigDecimal(((Number) value).longValue());

            }
            break;

        case FLOAT:

            if (value instanceof BigDecimal) {
                rexLiterValObj = value;
            } else {
                rexLiterValObj = new BigDecimal(((Number) value).doubleValue(), MathContext.DECIMAL32)
                    .stripTrailingZeros();
            }

            break;

        case REAL:
        case DOUBLE:
            // return BigDecimal

            if (value instanceof BigDecimal) {
                rexLiterValObj = value;
            } else {
                rexLiterValObj = new BigDecimal(((Number) value).doubleValue(), MathContext.DECIMAL64)
                    .stripTrailingZeros();
            }

            break;

        case CHAR:
        case VARCHAR:
            // return NlsString

            if (value instanceof NlsString) {
                rexLiterValObj = value;
            } else if (value instanceof Slice){
                rexLiterValObj = ((Slice) value).toStringUtf8();
            } else {
                rexLiterValObj = new NlsString((String) value, type.getCharset().name(),
                    type.getCollation());
            }
            break;

        case BINARY:
        case VARBINARY:
            // return ByteString

            if (value instanceof byte[]) {
                rexLiterValObj = new ByteString((byte[]) value);
            } else if (value instanceof ByteString) {
                rexLiterValObj = value;
            }

            break;

        case TIME:
            // return TimeString
            // Calendar -> TimeString
            // Integer -> TimeString

            if (value instanceof String) {
                rexLiterValObj = new TimeString((String) value);
            } else if (value instanceof TimeString) {
                rexLiterValObj = value;
            } else if (value instanceof Calendar) {
                rexLiterValObj = TimeString.fromCalendarFields((Calendar) value);
            } else {
                rexLiterValObj = TimeString.fromMillisOfDay((Integer) value);
            }
            break;

        case TIME_WITH_LOCAL_TIME_ZONE:
            // return TimeString
            // Integer -> TimeString

            if (value instanceof String) {
                rexLiterValObj = new TimeString((String) value);
            } else if (value instanceof TimeString) {
                rexLiterValObj = value;
            } else {
                rexLiterValObj = TimeString.fromMillisOfDay((Integer) value);

            }

            break;

        case DATE:
            // return DateString
            // Calendar -> DateString
            // Date -> DateString
            // Integer -> DateString

            if (value instanceof String) {
                rexLiterValObj = new DateString((String) value);
            } else if (value instanceof Calendar) {
                rexLiterValObj = DateString.fromCalendarFields((Calendar) value);
            } else if (value instanceof java.sql.Date) {
                Calendar c = Calendar.getInstance();
                c.setTime(new java.util.Date(((java.sql.Date) value).getTime()));
                rexLiterValObj = DateString.fromCalendarFields(c);
            } else {
                rexLiterValObj = DateString.fromDaysSinceEpoch((Integer) value);
            }

            break;

        case DATETIME:
            // return TimestampString
            // Calendar -> TimestampString
            // Long -> TimestampString

            // convert DATETIME to TIMESTAMP
            type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);

            if (value instanceof String) {
                rexLiterValObj = new TimestampString((String) value);
            } else if (value instanceof TimestampString) {
                rexLiterValObj = value;
            } else if (value instanceof Calendar) {
                rexLiterValObj = TimestampString.fromCalendarFields((Calendar) value);
            } else {
                rexLiterValObj = TimestampString.fromMillisSinceEpoch((Long) value);
            }

            break;

        case TIMESTAMP:
            // return TimestampString
            // Calendar -> TimestampString
            // Long -> TimestampString

            if (value instanceof String) {
                rexLiterValObj = new TimestampString((String) value);
            } else if (value instanceof TimestampString) {
                rexLiterValObj = value;
            } else if (value instanceof Calendar) {
                rexLiterValObj = TimestampString.fromCalendarFields((Calendar) value);
            } else {
                rexLiterValObj = TimestampString.fromMillisSinceEpoch((Long) value);
            }

            break;

        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            // return TimestampString
            // Long -> TimestampString

            if (value instanceof String) {
                rexLiterValObj = new TimestampString((String) value);
            } else if (value instanceof TimestampString) {
                rexLiterValObj = value;
            } else {
                rexLiterValObj = TimestampString.fromMillisSinceEpoch((Long) value);
            }
            break;

        default:
            rexLiterValObj = value;
        }
        RexLiteral rexNode = (RexLiteral) rexBuilder.makeLiteral(rexLiterValObj, type, true);
        return rexNode;
    }

    /**
     * The Table of FastSql change sql LiteralExpr to javaType
     * <p>
     * MySqlType    FastSqlLiteralType    -->    JavaType
     * varchar      SQLNCharExpr                String
     * Bool         SQLBooleanExpr              bool
     * bigint       SQLBigIntExpr               Long
     * decimal      SQLDecimalExpr              BigDecimal
     * int          SQLIntegerExpr              Number
     * real         SQLRealExpr                 Float
     * smallint     SQLSmallIntExpr             Short
     * tinyint      SQLTinyIntExpr              Byte
     * number       SQLNumberExpr               Number
     * varbinary    SQLHexExpr                  byte[]
     * <p>
     * timestamp    SQLTimestampExpr            String
     * date         SQLDateExpr                 String
     * time         SQLTimeExpr                 String
     * json         SQLJSONExpr                 String
     * <p>
     * null         SQLNullExpr                 EVAL_VALUE_NULL(Object)
     */
    public static Object getJavaObjectFromRexLiteral(RexLiteral rexLiteral) {
        return getJavaObjectFromRexLiteral(rexLiteral, false);
    }

    public static Object getJavaObjectFromRexLiteral(RexLiteral rexLiteral, boolean convertTimeToString) {

        SqlTypeName typeName = rexLiteral.getTypeName();
        Object javaObjOfLiteral = null;
        switch (typeName) {
        case TINYINT:
        case TINYINT_UNSIGNED:
        case SMALLINT:
        case SMALLINT_UNSIGNED:
        case MEDIUMINT:
        case MEDIUMINT_UNSIGNED:
        case INTEGER:
        case SIGNED: {
            Integer integerValue = (Integer) rexLiteral.getValue();
            javaObjOfLiteral = integerValue;
        }
        break;
        case UNSIGNED:
        case INTEGER_UNSIGNED:
        case BIGINT: {
            Long longValue = (Long) rexLiteral.getValue();
            javaObjOfLiteral = longValue;
        }
        break;
        case BIGINT_UNSIGNED: {
            BigInteger bigIntegerValue = (BigInteger) rexLiteral.getValue();
            javaObjOfLiteral = bigIntegerValue;
        }
        break;
        case DECIMAL: {
            final BigDecimal bigDecimal = (BigDecimal) rexLiteral.getValue();
            javaObjOfLiteral = bigDecimal;
        }
        break;
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND: {
            BigDecimal bigDecimal = (BigDecimal) rexLiteral.getValue();
            javaObjOfLiteral = bigDecimal.longValue();
        }
        break;

        case FLOAT: {
            BigDecimal bigDecimal = (BigDecimal) rexLiteral.getValue();
            javaObjOfLiteral = bigDecimal.floatValue();
        }

        break;

        case REAL:
        case DOUBLE:
            // return BigDecimal

        {
            BigDecimal bigDecimal = (BigDecimal) rexLiteral.getValue();
            javaObjOfLiteral = bigDecimal.doubleValue();
        }

        break;

        case CHAR:
        case VARCHAR:
            // return NlsString
        {
            NlsString nlsStr = (NlsString) rexLiteral.getValue();
            javaObjOfLiteral = nlsStr.getValue();
        }
        break;

        case BINARY:
        case VARBINARY:
            // return ByteString
        {
            ByteString byStr = (ByteString) rexLiteral.getValue();
            javaObjOfLiteral = byStr.getBytes();
        }

        break;

        case TIME:
        case DATE:
        case TIMESTAMP: {
            if (convertTimeToString) {
                javaObjOfLiteral = rexLiteral.toString();
            } else {
                Calendar calendar = (Calendar) rexLiteral.getValue();
                javaObjOfLiteral = calendar;
            }
        }
        break;

        case DATETIME:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {

            if (convertTimeToString) {
                javaObjOfLiteral = rexLiteral.toString();
            } else {
                Calendar calendar = ((TimestampString) rexLiteral.getValue()).toCalendar();
                javaObjOfLiteral = calendar;
            }

        }
        break;

        case TIME_WITH_LOCAL_TIME_ZONE: {

            if (convertTimeToString) {
                javaObjOfLiteral = rexLiteral.toString();
            } else {
                Calendar calendar = ((TimeString) rexLiteral.getValue()).toCalendar();
                javaObjOfLiteral = calendar;
            }
        }
        break;

        default:
            javaObjOfLiteral = rexLiteral.getValue();
            break;
        }

        return javaObjOfLiteral;
    }

}
