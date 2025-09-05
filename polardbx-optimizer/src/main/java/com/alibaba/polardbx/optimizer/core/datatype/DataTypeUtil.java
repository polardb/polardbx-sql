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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTime;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.NumericTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.type.BasicTypeBuilders;
import com.alibaba.polardbx.optimizer.core.expression.ISelectable;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.expression.bean.LobVal;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.exception.SqlValidateException;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

public class DataTypeUtil {

    private static TddlTypeFactoryImpl FACTORY = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    /**
     * Support precious type conversion.
     */
    public static <T> T convert(DataType<?> fromType, DataType<T> toType, Object value) {
        if (fromType == null) {
            if (toType == null) {
                return null;
            } else {
                return toType.convertFrom(value);
            }
        }
        if (fromType instanceof TimestampType && isStringType(toType)) {
            // convert timestamp to String, use timestampType.toStandardString
            String s = ((TimestampType) fromType).toStandardString(value);
            // maybe slice / string
            return toType.convertFrom(s);
        } else if (fromType instanceof TimeType && isStringType(toType)) {
            // convert time to String, use TimeType.toStandardString
            String s = ((TimeType) fromType).toStandardString(value);
            // maybe slice / string
            return toType.convertFrom(s);
        } else if (fromType instanceof DateType && isStringType(toType)) {
            // convert date to String, use DateType.toStandardString
            String s = ((DateType) fromType).toStandardString(value);
            // maybe slice / string
            return toType.convertFrom(s);
        } else if (fromType instanceof SliceType
            && toType instanceof BytesType
            && value instanceof Slice) {
            if (fromType.getCharsetName() == CharsetName.BINARY) {
                return (T) ((Slice) value).getBytes();
            }
            return (T) ((SliceType) fromType).getCharsetHandler().encodeFromUtf8((Slice) value).getBytes();
        } else if (fromType instanceof BlobType
            && toType instanceof BytesType
            && value instanceof Blob) {
            com.alibaba.polardbx.optimizer.core.datatype.Blob blob =
                (com.alibaba.polardbx.optimizer.core.datatype.Blob) DataTypes.BlobType.convertFrom(value);
            return (T) blob.getSlice().getBytes();
        } else if (fromType instanceof FloatType && fromType.getScale() != XResultUtil.DECIMAL_NOT_SPECIFIED
            && toType instanceof StringType && value instanceof Float) {
            return (T) String.format(String.format("%%.%df", fromType.getScale()), value);
        } else if (fromType instanceof DoubleType && fromType.getScale() != XResultUtil.DECIMAL_NOT_SPECIFIED
            && toType instanceof StringType && value instanceof Double) {
            return (T) String.format(String.format("%%.%df", fromType.getScale()), value);
        }

        return toType.convertFrom(value);
    }

    /**
     * from any object to mysql datetime representation.
     */
    public static MysqlDateTime toMySQLDatetime(Object timeObj) {
        return toMySQLDatetime(timeObj, Types.OTHER);
    }

    /**
     * from any object to mysql datetime representation.
     * by flags
     */
    public static MysqlDateTime toMySQLDatetimeByFlags(Object timeObj, int flags) {
        return toMySQLDatetimeByFlags(timeObj, Types.OTHER, flags);
    }

    /**
     * sqlType = Types.OTHER means don't intervene the type of mysql datetime.
     * flag = default flag
     */
    public static MysqlDateTime toMySQLDatetime(Object timeObj, int sqlType) {
        if (timeObj == null) {
            return null;
        }
        MysqlDateTime mysqlDateTime;
        if (timeObj instanceof Timestamp) {
            mysqlDateTime = MySQLTimeTypeUtil.toMysqlDateTime((Timestamp) timeObj);
            mysqlDateTime = MySQLTimeConverter.convertTemporalToTemporal(mysqlDateTime, Types.TIMESTAMP, sqlType);
            // need adjust
            mysqlDateTime = adjust(mysqlDateTime, sqlType);
        } else if (timeObj instanceof Date) {
            mysqlDateTime = MySQLTimeTypeUtil.toMysqlDate((Date) timeObj);
            mysqlDateTime = MySQLTimeConverter.convertTemporalToTemporal(mysqlDateTime, Types.DATE, sqlType);
            // need adjust
            mysqlDateTime = adjust(mysqlDateTime, sqlType);
        } else if (timeObj instanceof java.sql.Time) {
            mysqlDateTime = MySQLTimeTypeUtil.toMysqlTime((java.sql.Time) timeObj);
            mysqlDateTime = MySQLTimeConverter.convertTemporalToTemporal(mysqlDateTime, Types.TIME, sqlType);
            // need adjust
            mysqlDateTime = adjust(mysqlDateTime, sqlType);
        } else if (timeObj instanceof Number) {
            // for number object
            mysqlDateTime = NumericTimeParser.parseNumeric((Number) timeObj, sqlType);
        } else {
            Slice slice = DataTypes.VarcharType.convertFrom(timeObj);
            mysqlDateTime = StringTimeParser.parseString(slice.getBytes(), sqlType);
        }

        return mysqlDateTime;
    }

    /**
     * sqlType = Types.OTHER means don't intervene the type of mysql datetime.
     * By Flags
     */
    public static MysqlDateTime toMySQLDatetimeByFlags(Object timeObj, int sqlType, int flags) {
        if (timeObj == null) {
            return null;
        }
        MysqlDateTime mysqlDateTime;
        if (timeObj instanceof Timestamp) {
            mysqlDateTime = MySQLTimeTypeUtil.toMysqlDateTime((Timestamp) timeObj, flags);
            mysqlDateTime = MySQLTimeConverter.convertTemporalToTemporal(mysqlDateTime, Types.TIMESTAMP, sqlType);
            // need adjust
            mysqlDateTime = adjust(mysqlDateTime, sqlType);
        } else if (timeObj instanceof Date) {
            mysqlDateTime = MySQLTimeTypeUtil.toMysqlDate((Date) timeObj, flags);
            mysqlDateTime = MySQLTimeConverter.convertTemporalToTemporal(mysqlDateTime, Types.DATE, sqlType);
            // need adjust
            mysqlDateTime = adjust(mysqlDateTime, sqlType);
        } else if (timeObj instanceof java.sql.Time) {
            mysqlDateTime = MySQLTimeTypeUtil.toMysqlTime((java.sql.Time) timeObj, flags);
            mysqlDateTime = MySQLTimeConverter.convertTemporalToTemporal(mysqlDateTime, Types.TIME, sqlType);
            // need adjust
            mysqlDateTime = adjust(mysqlDateTime, sqlType);
        } else if (timeObj instanceof Number) {
            // for number object
            mysqlDateTime = NumericTimeParser.parseNumeric((Number) timeObj, sqlType, flags);
        } else {
            Slice slice = DataTypes.VarcharType.convertFrom(timeObj);
            mysqlDateTime = StringTimeParser.parseString(slice.getBytes(), sqlType, flags);
        }

        return mysqlDateTime;
    }

    private static MysqlDateTime adjust(MysqlDateTime mysqlDateTime, int sqlType) {
        if (mysqlDateTime == null) {
            return null;
        }
        // adjust time value according to the type.
        switch (sqlType) {
        case Types.TIME:
            mysqlDateTime.setYear(0);
            mysqlDateTime.setMonth(0);
            mysqlDateTime.setDay(0);
            mysqlDateTime.setSqlType(sqlType);
            break;
        case Types.DATE:
            mysqlDateTime.setHour(0);
            mysqlDateTime.setMinute(0);
            mysqlDateTime.setSecond(0);
            mysqlDateTime.setSecondPart(0);
            mysqlDateTime.setSqlType(sqlType);
            break;
        case Types.TIMESTAMP:
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
            mysqlDateTime.setSqlType(sqlType);
        default:
            break;
        }
        return mysqlDateTime;
    }

    public static MysqlDateTime getNow(int scale, ExecutionContext executionContext) {
        // get zoned now datetime
        ZonedDateTime zonedDateTime;
        if (executionContext.getTimeZone() != null) {
            ZoneId zoneId = executionContext.getTimeZone().getZoneId();
            zonedDateTime = ZonedDateTime.now(zoneId);
        } else {
            zonedDateTime = ZonedDateTime.now();
        }

        // round to scale.
        MysqlDateTime t = MySQLTimeTypeUtil.fromZonedDatetime(zonedDateTime);
        t = MySQLTimeCalculator.timeTruncate(t, scale);
        return t;
    }

    /**
     * convert mysql datetime to proper type.
     */
    public static Object fromMySQLDatetime(DataType resultType, MysqlDateTime ret, TimeZone timezone) {
        if (ret == null || MySQLTimeTypeUtil.isDatetimeRangeInvalid(ret)) {
            // for null or invalid time
            return null;
        } else if (DataTypeUtil.anyMatchSemantically(resultType, DataTypes.TimestampType, DataTypes.DatetimeType)) {
            // for timestamp / datetime
            ret.setTimezone(timezone);
            return new OriginalTimestamp(ret);
        } else if (DataTypeUtil.equalsSemantically(resultType, DataTypes.DateType)) {
            // for date
            ret.setTimezone(timezone);
            return new OriginalDate(ret);
        } else if (DataTypeUtil.equalsSemantically(resultType, DataTypes.TimeType)) {
            // for time
            ret.setTimezone(timezone);
            return new OriginalTime(ret);
        } else if (DataTypeUtil.isStringType(resultType)) {
            // for varchar
            return ret.toStringBySqlType();
        } else {
            return ret.toDatetimeString(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);
        }
    }

    public static boolean isTemporalTypeWithDate(DataType type) {
        return anyMatchSemantically(
            type,
            DataTypes.DatetimeType,
            DataTypes.TimestampType,
            DataTypes.DateType
        );
    }

    public static DataType getTypeOfObject(Object v) {
        if (v == null || v instanceof NullValue) {
            return DataTypes.NullType;
        }
        Class clazz = v.getClass();

        if (clazz == Long.class || clazz == long.class) {
            return DataTypes.LongType;
        }
        if (clazz == String.class || Clob.class.isAssignableFrom(clazz)) {
            return DataTypes.StringType;
        }
        if (clazz == Integer.class || clazz == int.class) {
            return DataTypes.IntegerType;
        }
        if (clazz == Short.class || clazz == short.class) {
            return DataTypes.ShortType;
        }
        if (clazz == Float.class || clazz == float.class) {
            return DataTypes.FloatType;
        }
        if (clazz == Double.class || clazz == double.class) {
            return DataTypes.DoubleType;
        }
        if (clazz == Byte.class || clazz == byte.class) {
            return DataTypes.ByteType;
        }
        if (clazz == Boolean.class || clazz == boolean.class) {
            return DataTypes.BooleanType;
        }
        if (clazz == BigInteger.class || clazz == UInt64.class) {
            return DataTypes.ULongType;
        }
        if (clazz == Decimal.class) {
            return DataTypes.DecimalType;
        }
        if (clazz == BigDecimal.class) {
            return DataTypes.DecimalType;
        }
        if (clazz == Slice.class) {
            return new VarcharType();
        }

        if (clazz == Timestamp.class || clazz == java.util.Date.class || Calendar.class.isAssignableFrom(clazz)
            || clazz == ZeroTimestamp.class || clazz == OriginalTimestamp.class) {
            return DataTypes.TimestampType;
        }
        if (clazz == java.sql.Date.class || clazz == ZeroDate.class || clazz == OriginalDate.class) {
            return DataTypes.DateType;
        }
        if (clazz == Time.class || clazz == ZeroTime.class || clazz == OriginalTime.class) {
            return DataTypes.TimeType;
        }

        if (clazz == Byte[].class || clazz == byte[].class || Blob.class.isAssignableFrom(clazz)
            || LobVal.class == clazz) {
            return DataTypes.BytesType;
        }
        if (clazz == EnumValue.class) {
            return ((EnumValue) v).getType();
        }

        if (v instanceof ISelectable) {
            return ((ISelectable) v).getDataType();
        }

        if (v instanceof IntervalType) {
            return DataTypes.IntervalType;
        }

        if (v instanceof ByteString) {
            return DataTypes.BitType;
        }

        if (v instanceof RowValue) {
            return DataTypes.RowType;
        }

        throw new OptimizerException("type: " + v.getClass().getSimpleName() + " is not supported");
    }

    /**
     * 判断是否为时间类型
     */
    public static boolean isDateType(DataType type) {

        return type != null
            && (DataTypeUtil.equalsSemantically(DataTypes.DateType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.TimeType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.TimestampType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.YearType, type));
    }

    /**
     * MySql time type is different from DateType,for it's lacking year type
     */
    public static boolean isMysqlTimeType(DataType type) {
        return type != null
            && DataTypeUtil
            .anyMatchSemantically(type,
                DataTypes.DateType,
                DataTypes.TimeType,
                DataTypes.DatetimeType,
                DataTypes.TimestampType);
    }

    public static boolean isFractionalTimeType(DataType type) {
        return type != null
            && DataTypeUtil
            .anyMatchSemantically(type, DataTypes.TimeType, DataTypes.DatetimeType, DataTypes.TimestampType);
    }

    /**
     * 判断是否为mysql的各种int类,取值范围<=Long.MaxValue,不包含unsigned bigint/deicmal/number类型
     */
    public static boolean isUnderLongType(DataType type) {
        return type != null && (isUnderIntType(type)
            || DataTypeUtil.equalsSemantically(DataTypes.UIntegerType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.LongType, type));
    }

    public static boolean isUnderIntType(DataType type) {
        return type != null && (DataTypeUtil.equalsSemantically(DataTypes.TinyIntType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.UTinyIntType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.ShortType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.SmallIntType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.USmallIntType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.MediumIntType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.UMediumIntType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.IntegerType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.BooleanType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.ByteType, type));
    }

    public static boolean isIntType(DataType type) {
        return DataTypeUtil.anyMatchSemantically(type,
            DataTypes.TinyIntType,
            DataTypes.UTinyIntType,
            DataTypes.SmallIntType,
            DataTypes.USmallIntType,
            DataTypes.MediumIntType,
            DataTypes.UMediumIntType,
            DataTypes.IntegerType,
            DataTypes.UIntegerType,
            DataTypes.LongType,
            DataTypes.ULongType
        );
    }

    public static boolean isRealType(DataType type) {
        return DataTypeUtil.anyMatchSemantically(type,
            DataTypes.DoubleType,
            DataTypes.FloatType
        );
    }

    public static boolean isDecimalType(DataType type) {
        return DataTypeUtil.equalsSemantically(type, DataTypes.DecimalType);
    }

    public static boolean isTimezoneDependentType(DataType dataType) {
        return dataType.getSqlType() == Types.TIMESTAMP || dataType.getSqlType() == Types.TIME;
    }

    public static boolean isUnderBigintType(DataType dataType) {
        return isUnderLongType(dataType) || dataType.getSqlType() == Types.BIGINT;
    }

    public static boolean isUnderBigintUnsignedType(DataType dataType) {
        return isUnderBigintType(dataType) || isBigintUnsigned(dataType);
    }

    public static boolean isStringType(DataType type) {
        return type != null && (type instanceof StringType
            || type instanceof SliceType
            || DataTypeUtil.equalsSemantically(DataTypes.SensitiveStringType, type));
    }

    public static boolean isBinaryType(DataType type) {
        return type != null && (DataTypeUtil.equalsSemantically(DataTypes.BlobType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.BinaryType, type));
    }

    public static boolean isFloatSqlType(DataType dataType) {
        switch (dataType.getSqlType()) {
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
            return true;
        default:
            return false;
        }
    }

    public static boolean isAutoIncrementSqlType(DataType dataType) {
        switch (dataType.getSqlType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.NUMERIC:
        case DataType.MEDIUMINT_SQL_TYPE:
            return true;
        default:
            return false;
        }
    }

    public static boolean isBigintUnsigned(DataType dataType) {
        return dataType.getSqlType() == Types.BIGINT && dataType.isUnsigned();
    }

    public static boolean isNumberSqlType(DataType dataType) {
        switch (dataType.getSqlType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
        case DataType.MEDIUMINT_SQL_TYPE:
        case DataType.YEAR_SQL_TYPE:
            return true;
        default:
            return false;
        }
    }

    public static boolean isStringSqlType(DataType dataType) {
        switch (dataType.getSqlType()) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return true;
        default:
            return false;
        }
    }

    private static boolean isBinaryColumnType(ColumnMeta cm) {
        try {
            if (cm != null && "BINARY".equalsIgnoreCase(cm.getField().getRelType().getCharset().name())) {
                return true;
            }
        } catch (Throwable e) {
            // Ignore
        }
        return false;
    }

    /**
     * Convert inner type to JDBC types
     */
    public static Object toJavaObject(ColumnMeta cm, Object value) {
        if (value instanceof Decimal) {
            value = ((Decimal) value).toBigDecimal();
        } else if (value instanceof Slice) {
            if (isBinaryColumnType(cm)) {
                value = ((Slice) value).getBytes();
            } else {
                value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
            }
        } else if (value instanceof UInt64) {
            value = ((UInt64) value).toBigInteger();
        } else if (value instanceof ZeroDate || value instanceof ZeroTime || value instanceof ZeroTimestamp) {
            /**
             * For date like "0000-00-00" partition result is different for ZeroDate and String.
             * INSERT and SELECT use String data type, so UPDATE/DELETE here should keep same.
             * </p>
             * For ZeroDate/ZeroTime/ZeroTimestamp object,
             * jdbc will convert object to string "1970-01-01 08:00:00" in {@link com.mysql.jdbc.PreparedStatement#setObject(int, java.lang.Object)}.
             * This will make where-condition return false because "1970-01-01 08:00:00" is not equal to "0000-00-00 00:00:00", and then cause update fail.
             * INSERT and SELECT use String data type, so UPDATE/DELETE here should keep same.
             */
            value = value.toString();
        } else if (value instanceof OriginalDate || value instanceof OriginalTimestamp) {
            /**
             * For zero month or day date like "0000-00-00" partition result is different for ZeroDate and String.
             * INSERT and SELECT use String data type, so UPDATE/DELETE here should keep same.
             * </p>
             * For OriginalDate/OriginalTime/OriginalTimestamp object,
             * jdbc will convert object to string "0002-11-30 00:00:00" in {@link com.mysql.jdbc.PreparedStatement#setObject(int, java.lang.Object)}.
             * This will make where-condition return false because "0002-11-30 00:00:00" is not equal to "0000-00-00 00:00:00", and then cause update fail.
             * INSERT and SELECT use String data type, so UPDATE/DELETE here should keep same.
             */
            MysqlDateTime mysqlDateTime = ((OriginalTemporalValue) value).getMysqlDateTime();
            long month = mysqlDateTime.getMonth();
            long day = mysqlDateTime.getDay();
            if (month == 0 || day == 0) {
                value = value.toString();
            }
        } else if (value instanceof EnumValue) {
            // EnumValue can not be used in setObject
            value = ((EnumValue) value).getValue();
        }
        return value;
    }

    public static DataType calciteToDrdsType(RelDataType type) {
        if (type.isStruct()) {
            List<DataType> dataTypes = new ArrayList<>();
            for (RelDataTypeField child : type.getFieldList()) {
                DataType dataType = calciteToDrdsType(child.getType());
                dataTypes.add(dataType);
            }
            return new RowType(dataTypes);
        }
        return DataTypeFactory.INSTANCE.create(type);
    }

    /**
     * Convert column types in ResultSetMetaData to RelDataType
     *
     * @param jdbcType rsmd.getColumnType(i)
     * @param columnTypeName rsmd.getColumnTypeName(i)
     * @param precision rsmd.getPrecision(i)
     * @param scale rsmd.getScale(i)
     * @param length com.mysql.jdbc.Field.getLength()
     * @param nullable (rsmd.isNullable(i) != 0)
     * @return the converted RelDataType
     */
    public static RelDataType jdbcTypeToRelDataType(int jdbcType, String columnTypeName,
                                                    int precision, int scale, long length, boolean nullable) {
        TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        String columnTypeUpper = columnTypeName.toUpperCase();

        SqlTypeName calciteTypeName;
        switch (columnTypeUpper) {
        case "TINYINT": // MySQL JDBC returns -7 (Types.BIT)
            calciteTypeName = SqlTypeName.TINYINT;
            break;
        case "TINYINT UNSIGNED": // see above
            calciteTypeName = SqlTypeName.TINYINT_UNSIGNED;
            break;
        case "YEAR": // non-standard JDBC type
            calciteTypeName = SqlTypeName.YEAR;
            break;
        case "BIT":
            if (length > 1) {
                calciteTypeName = SqlTypeName.BIG_BIT;
            } else {
                calciteTypeName = SqlTypeName.BIT;
            }
            break;
        case "MEDIUMINT UNSIGNED": // non-standard JDBC type
            calciteTypeName = SqlTypeName.MEDIUMINT_UNSIGNED;
            break;
        case "MEDIUMINT": // non-standard JDBC type
            calciteTypeName = SqlTypeName.MEDIUMINT;
            break;
        case "DATETIME": // non-standard JDBC type
            calciteTypeName = SqlTypeName.DATETIME;
            break;
        case "JSON":
            calciteTypeName = SqlTypeName.JSON;
            break;
        case "TINYBLOB":
        case "MEDIUMBLOB":
        case "BLOB":
        case "LONGBLOB":
            calciteTypeName = SqlTypeName.BLOB;
            break;
        default:
            if (columnTypeUpper.startsWith("ENUM")) {
                EnumType enumType = parseEnumType(columnTypeName);
                final ImmutableList.Builder<String> builder = ImmutableList.builder();

                final Set<String> strings = enumType.getEnumValues().keySet();
                for (String enumValue : strings) {
                    builder.add(enumValue);
                }

                final ImmutableList<String> build = builder.build();

                calciteTypeName = SqlTypeName.ENUM;
                RelDataType calciteType = factory.createEnumSqlType(calciteTypeName, build);
                return calciteType;
            }
            if (columnTypeUpper.startsWith("SET")) {
                SetType setType = parseSetType(columnTypeName);
                // keep compatible with old manners
                calciteTypeName = SqlTypeName.CHAR;
                return factory.createSetSqlType(calciteTypeName, (int) Long.min(precision, Integer.MAX_VALUE),
                    setType.getSetValues());
            }
            boolean unsigned = columnTypeUpper.contains("UNSIGNED");
            if (unsigned) {
                // Some unsigned types are not supported in DRDS, such as DECIMAL/FLOAT/DOUBLE UNSIGNED
                // In such case we use the signed type instead
                SqlTypeName unsignedType = SqlTypeName.getNameForUnsignedJdbcType(jdbcType);
                calciteTypeName = unsignedType != null ? unsignedType : SqlTypeName.getNameForJdbcType(jdbcType);
            } else {
                calciteTypeName = SqlTypeName.getNameForJdbcType(jdbcType);
            }
        }
        if (calciteTypeName == null) {
            throw new IllegalArgumentException("unsupported column type: " + columnTypeName);
        }

        RelDataType calciteType;
        switch (calciteTypeName) {
        case TINYINT:
        case TINYINT_UNSIGNED:
            calciteType = factory.createSqlType(calciteTypeName, (int) length);
            break;
        case DECIMAL:
        case DATETIME:
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case TIME_WITH_LOCAL_TIME_ZONE:
        case TIME:
            calciteType = factory.createSqlType(calciteTypeName, precision, scale);
            break;
        case FLOAT:
        case DOUBLE:
            // 由于历史存储的元数据原因，忽略scale=0的情况
            calciteType = factory.createSqlType(calciteTypeName, RelDataType.PRECISION_NOT_SPECIFIED,
                scale > 0 ? scale : RelDataType.SCALE_NOT_SPECIFIED);
            break;
        /*
         * Note: Why CHAR and BINARY was commented here?
         * For CHAR with precision specified, e.g. CHAR(20), `RexBuilder.makeLiteral()` will do right-padding,
         * which may cause incorrect sharding.
         *
         * Now we remove the right-padding from RexBuilder.makeLiteral().
         */
        case CHAR:
        case BINARY:
        case VARCHAR:
        case BINARY_VARCHAR:
        case BIT:
        case BIG_BIT:
        case VARBINARY:
            calciteType = factory.createSqlType(calciteTypeName, (int) Long.min(precision, Integer.MAX_VALUE));
            break;
        default:
            calciteType = factory.createSqlType(calciteTypeName);
            break;
        }
        calciteType = factory.createTypeWithNullability(calciteType, nullable);

        return calciteType;
    }

    public static RelDataType getCharacterTypeWithCharsetAndCollation(RelDataType type, String charset,
                                                                      String collation) {
        Preconditions.checkArgument(SqlTypeUtil.isCharacter(type));
        Charset sqlCharset = Optional.ofNullable(charset)
            .map(CharsetName::convertStrToJavaCharset)
            .orElseGet(
                () -> Optional.of(CharsetName.defaultCharset())
                    .map(CharsetName::getJavaCharset)
                    .map(Charset::forName)
                    .get()
            );
        SqlCollation sqlCollation = new SqlCollation(sqlCharset, collation, SqlCollation.Coercibility.IMPLICIT);
        RelDataType newType = FACTORY.createTypeWithCharsetAndCollation(type, sqlCharset, sqlCollation);
        return newType;
    }

    public static EnumType parseEnumType(String typeSpec) {
        String[] enums = TStringUtil.substringBetween(typeSpec, "(", ")").split(",");
        for (int i = 0; i < enums.length; i++) {
            enums[i] = TStringUtil.substringBetween(enums[i], "'");
        }
        return new EnumType(Arrays.asList(enums));
    }

    public static SetType parseSetType(String typeSpec) {
        String[] setValues = TStringUtil.substringBetween(typeSpec, "(", ")").split(",");
        for (int i = 0; i < setValues.length; i++) {
            setValues[i] = TStringUtil.substringBetween(setValues[i], "'");
        }
        return new SetType(Arrays.asList(setValues));
    }

    /**
     * Do NOT use this method since it cannot handle the type parameters (precision, scale, etc.) correctly
     *
     * @param length field length, or 0 for unknown
     */
    @Deprecated
    public static RelDataType tddlTypeToRelDataType(DataType type, int length) {
        SqlTypeName calciteTypeName;
        RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        // Firstly, deal with special type
        if (type instanceof BigBitType) {
            calciteTypeName = SqlTypeName.BIG_BIT;
        } else if (type instanceof EnumType) {
            calciteTypeName = SqlTypeName.ENUM;
        } else if (type.isUnsigned()) {
            // then deal with unsigned type
            if (type.getSqlType() == DataType.MEDIUMINT_SQL_TYPE) {
                calciteTypeName = SqlTypeName.MEDIUMINT_UNSIGNED;
            } else {
                calciteTypeName = SqlTypeName.getNameForUnsignedJdbcType(type.getSqlType());
            }
        } else {
            calciteTypeName = SqlTypeName.getNameForJdbcType(type.getSqlType());

            if (calciteTypeName == null) {
                // TDDL 扩展了一些类型，需要特殊处理
                switch (type.getSqlType()) {
                case DataType.YEAR_SQL_TYPE:
                    calciteTypeName = SqlTypeName.YEAR;
                    break;
                case DataType.MEDIUMINT_SQL_TYPE:
                    calciteTypeName = SqlTypeName.MEDIUMINT;
                    break;
                case DataType.DATETIME_SQL_TYPE:
                    calciteTypeName = SqlTypeName.DATETIME;
                    break;
                case DataType.JSON_SQL_TYPE:
                    calciteTypeName = SqlTypeName.JSON;
                    break;
                case DataType.UNDECIDED_SQL_TYPE:
                    calciteTypeName = SqlTypeName.NULL;
                    break;
                default:
                    String errMsg = "Can not find matched data type at Calcite for type : " + type;
                    throw new SqlValidateException(errMsg);
                }
            }
        }

        RelDataType calciteType;
        if (length > 0
            && (DataTypeUtil.anyMatchSemantically(type,
            DataTypes.BitType, DataTypes.BigBitType,
            DataTypes.TinyIntType, DataTypes.StringType,
            DataTypes.UTinyIntType, DataTypes.BinaryType))) {
            calciteType = factory.createSqlType(calciteTypeName, length);
        } else if (DataTypeUtil.isFractionalTimeType(type)) {
            // for fsp time type.
            int precision = factory.getTypeSystem().getMaxPrecision(calciteTypeName);
            int scale = type.getScale();
            calciteType = factory.createSqlType(calciteTypeName, precision, scale);
        } else {
            calciteType = factory.createSqlType(calciteTypeName);
        }
        if (DataTypeUtil.isStringType(type)) {
            Charset javaCharset = type.getCharsetName().toJavaCharset();
            // RISK: Coercibility.COERCIBLE
            SqlCollation sqlCollation = new SqlCollation(javaCharset,
                type.getCollationName().name(),
                SqlCollation.Coercibility.COERCIBLE);
            calciteType = factory.createTypeWithCharsetAndCollation(calciteType,
                javaCharset,
                sqlCollation);
        }
        calciteType = factory.createTypeWithNullability(calciteType, true);
        return calciteType;
    }

    public static boolean equalsSemantically(DataType type1, DataType type2) {
        if (type1 == null || type2 == null) {
            return false;
        }
        boolean res = type1 == type2;
        // for non-singleton data type
        res |= type1.getClass() == type2.getClass();
        return res;
    }

    public static boolean anyMatchSemantically(DataType type, DataType... toMatch) {
        if (type == null) {
            return false;
        }
        for (int i = 0; i < toMatch.length; i++) {
            if (equalsSemantically(type, toMatch[i])) {
                return true;
            }
        }
        return false;
    }

    public static long estimateTypeSize(DataType type) {
        if (type == DataTypes.BooleanType) {
            return 1;
        }
        if (DataTypes.TinyIntType == type || DataTypes.UTinyIntType == type || DataTypes.SmallIntType == type) {
            return Short.BYTES;
        }
        if (DataTypes.USmallIntType == type || DataTypes.MediumIntType == type || DataTypes.UMediumIntType == type
            || DataTypes.IntegerType == type || DataTypes.BitType == type) {
            return Integer.BYTES;
        }
        if (DataTypes.UIntegerType == type || DataTypes.YearType == type) {
            return Long.BYTES;
        }
        if (DataTypes.DecimalType == type || DataTypes.BigBitType == type) {
            // on average
            return 20;
        }
        if (DataTypes.FloatType == type) {
            return Float.BYTES;
        }
        if (DataTypes.DoubleType == type) {
            return Double.BYTES;
        }
        if (DataTypes.TimestampType == type || DataTypes.DateType == type || DataTypes.DatetimeType == type
            || DataTypes.TimeType == type) {
            return 8;
        }
        if (DataTypes.IntervalType == type) {
            return Long.BYTES;
        }

        if (DataTypes.CharType == type || DataTypes.VarcharType == type || DataTypes.BinaryType == type
            || DataTypes.BinaryStringType == type) {
            int precision = type.getPrecision();
            if (precision <= 0) {
                precision = 20;
            }
            // treat VARCHAR(256) as length 100
            return Math.min(precision, 100);
        }
        if (DataTypes.NullType == type) {
            return 0;
        }
        // for all strange values
        return 100;
    }

    public static boolean equals(DataType type1, DataType type2, boolean deep) {
        boolean equalsSemantically = equalsSemantically(type1, type2);
        if (!deep || !equalsSemantically) {
            return equalsSemantically;
        } else {
            return type1.equalDeeply(type2);
        }
    }

    public static DataType aggResultTypeOf(DataType dataType, SqlKind sqlKind) {
        switch (sqlKind) {
        case COUNT:
        case CHECK_SUM:
        case CHECK_SUM_V2:
            return DataTypes.LongType;
        case MIN:
        case MAX:
            return dataType;
        case SUM:
        case SUM0:
            switch (dataType.fieldType()) {
            case MYSQL_TYPE_LONGLONG:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_TINY:
                // long value (18 slot) as decimal.
                return new DecimalType(18, 0);
            case MYSQL_TYPE_DOUBLE:
            case MYSQL_TYPE_FLOAT:
                return DataTypes.DoubleType;
            }
        }
        throw new UnsupportedOperationException();
    }

    public static SqlTypeName typeNameOfParam(Object param) {
        if (param instanceof Integer || param instanceof Long) {
            return SqlTypeName.BIGINT;
        } else if (param instanceof BigInteger) {
            return SqlTypeName.BIGINT_UNSIGNED;
        } else if (param instanceof BigDecimal) {
            return SqlTypeName.DECIMAL;
        } else {
            return SqlTypeName.CHAR;
        }
    }

    // TODO : check collation and other situation
    public static RelDataType createBasicSqlType(RelDataTypeSystem typeSystem, SQLDataType dataType) {
        return BasicTypeBuilders.getTypeBuilder(dataType.getName()).createBasicSqlType(typeSystem, dataType);
    }

    /**
     * BIGINT (8 Bytes): -9223372036854775808 ~ 9223372036854775807
     * BIGINT UNSIGNED (8 Bytes): 0~18446744073709551615
     */
    private static BigInteger MAX_UNSIGNED_LONG = new BigInteger("18446744073709551615");
    private static BigInteger MIN_UNSIGNED_LONG = new BigInteger("0");

    public static boolean checkUnderBigintUnsigned(BigInteger bigint) {
        int rs1 = MAX_UNSIGNED_LONG.compareTo(bigint);
        int rs2 = MIN_UNSIGNED_LONG.compareTo(bigint);
        return rs1 >= 0 && rs2 <= 0;
    }

    private static BigInteger MAX_SIGNED_LONG = new BigInteger("9223372036854775807");
    private static BigInteger MIN_SIGNED_LONG = new BigInteger("-9223372036854775808");

    public static boolean checkUnderBigintSigned(BigInteger bigint) {
        int rs1 = MAX_SIGNED_LONG.compareTo(bigint);
        int rs2 = MIN_SIGNED_LONG.compareTo(bigint);
        return rs1 >= 0 && rs2 <= 0;
    }

    public static boolean fixDynamicParamObjectIfNeed(Object v, DataType dataTypeInput,
                                                      Object[] newV, DataType[] dataTypeOutput) {
        Class clazz = v.getClass();
        if (dataTypeInput == DataTypes.ULongType) {
            if (clazz == BigInteger.class) {
                BigInteger bigIntVal = (BigInteger) v;
                if (!DataTypeUtil.checkUnderBigintUnsigned(bigIntVal) && !DataTypeUtil.checkUnderBigintSigned(
                    bigIntVal)) {
                    String varStr = v.toString();
                    newV[0] = varStr;
                    dataTypeOutput[0] = DataTypes.StringType;
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isChar(DataType dataType) {
        return dataType.getSqlType() == Types.CHAR ||
            dataType.getSqlType() == Types.VARCHAR ||
            dataType.getSqlType() == Types.LONGVARCHAR ||
            dataType.getSqlType() == Types.NCHAR ||
            dataType.getSqlType() == Types.NVARCHAR ||
            dataType.getSqlType() == Types.LONGNVARCHAR;
    }
}
