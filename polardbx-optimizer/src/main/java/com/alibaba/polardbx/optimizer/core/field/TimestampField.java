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

import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.SQLModeFlags;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.NumericTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.config.table.charset.CollationHandlers;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Arrays;

/**
 * timestamp(N) type
 * In string context: YYYY-MM-DD HH:MM:SS.FFFFFF
 * In number context: YYYYMMDDHHMMSS.FFFFFF
 * Stored as a 7 byte value
 */
public class TimestampField extends AbstractTemporalField {
    private TimeParseStatus cachedStatus;
    private byte[] packedBinary;

    public TimestampField(DataType<?> fieldType) {
        super(fieldType);
        int packedLen = packetLength();
        this.packedBinary = new byte[packedLen];
        // init cache status
        getCachedStatus();
    }

    @Override
    protected int dateFlags(SessionProperties sessionProperties) {
        int flag = TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE;
        long sqlModeFlag = sessionProperties.getSqlModeFlag();
        if (SQLModeFlags.check(sqlModeFlag, SQLModeFlags.MODE_NO_ZERO_DATE)) {
            flag |= TimeParserFlags.FLAG_TIME_NO_ZERO_DATE;
        }
        return flag;
    }

    @Override
    protected TypeConversionStatus storeXProtocolInternal(XResult xResult, PolarxResultset.ColumnMetaData meta,
                                                          ByteString byteString, int columnIndex,
                                                          SessionProperties sessionProperties) throws Exception {
        Pair<Object, byte[]> pair = XResultUtil.resultToObject(meta, byteString, true,
            xResult.getConnection().getSession().getDefaultTimezone());
        final Object val = pair.getKey();
        final byte[] bytes = pair.getValue();
        if (val instanceof Timestamp || val instanceof Date) {
            return storeBytes(bytes, sessionProperties);
        } else if (val instanceof String) {
            return storeString((String) val, sessionProperties);
        } else {
            return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
        }
    }

    @Override
    protected TypeConversionStatus storeJdbcInternal(ResultSet rs, int columnIndex,
                                                     SessionProperties sessionProperties) throws Exception {

        // directly read raw bytes
        byte[] rawBytes = rs.getBytes(columnIndex);
        if (rawBytes == null) {
            setNull();
            return TypeConversionStatus.TYPE_OK;
        }
        return storeBytes(rawBytes, sessionProperties);
    }

    @Override
    protected TypeConversionStatus storeInternal(Object value, DataType<?> resultType,
                                                 SessionProperties sessionProperties) {
        switch (resultType.fieldType()) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            if (value instanceof Number) {
                long longTemporalValue = ((Number) value).longValue();
                return storeInteger(longTemporalValue, sessionProperties);
            } else if (value == null) {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
            break;
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
            if (value instanceof OriginalTemporalValue) {
                // store value from chunk executor as mysql datetime.
                OriginalTemporalValue temporalValue = (OriginalTemporalValue) value;
                MysqlDateTime mysqlDateTime = temporalValue.getMysqlDateTime();
                return storeTemporal(mysqlDateTime, sessionProperties, mysqlDateTime.getSqlType());
            } else if (value instanceof Timestamp) {
                // Bad case: Use jdbc-style temporal value (java.sql.Timestamp)
                ZoneId zoneId = sessionProperties.getTimezone();
                MysqlDateTime mysqlDateTime = MySQLTimeConverter.convertTimestampToDatetime((Timestamp) value, zoneId);
                mysqlDateTime.setSqlType(Types.TIMESTAMP);

                // Use mysqlDatetime rather than timeVal for temporal calculating.
                return storeMysqlDatetime(mysqlDateTime, sessionProperties);
            } else if (value instanceof Date) {
                // Bad case: Use jdbc-style temporal value (java.sql.Date)
                ZoneId zoneId = sessionProperties.getTimezone();
                MysqlDateTime mysqlDateTime =
                    MySQLTimeConverter.toMySqlDatetime(zoneId, ((Date) value).getTime() / 1000L, 0);
                mysqlDateTime.setSqlType(Types.DATE);

                // Use mysqlDatetime rather than timeVal for temporal calculating.
                return storeMysqlDatetime(mysqlDateTime, sessionProperties);
            } else if (value instanceof Slice) {
                // for parameterized value.
                return storeSlice((Slice) value, sessionProperties);
            } else if (value instanceof String) {
                return storeString((String) value, sessionProperties);
            } else if (value instanceof byte[]) {
                return storeBytes((byte[]) value, sessionProperties);
            } else if (value == null) {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
            break;
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            // must be in UTF-8 character set.
            if (value instanceof Slice) {
                return storeSlice((Slice) value, sessionProperties);
            } else if (value instanceof String) {
                return storeString((String) value, sessionProperties);
            } else if (value instanceof byte[]) {
                return storeBytes((byte[]) value, sessionProperties);
            } else if (value == null) {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
            break;
        default:
            return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
        }
        return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
    }

    @Override
    public CollationHandler getCollationHandler() {
        return CollationHandlers.COLLATION_HANDLER_BINARY;
    }

    @Override
    public MySQLStandardFieldType standardFieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_TIMESTAMP;
    }

    @Override
    public void reset() {
        Arrays.fill(packedBinary, (byte) 0);
        cachedStatus.clear();
        cachedPacketLength = UNSET_PACKET_LEN;
        isNull = false;
    }

    @Override
    public void setNull() {
        isNull = true;
        Arrays.fill(packedBinary, (byte) 0);
        cachedStatus.clear();
    }

    @Override
    public void hash(long[] numbers) {
        long nr1 = numbers[0];
        long nr2 = numbers[1];
        if (isNull()) {
            nr1 ^= (nr1 << 1) | 1;
            numbers[0] = nr1;
        } else {
            int length = packetLength();
            CollationHandler collationHandler = getCollationHandler();
            collationHandler.hashcode(packedBinary, length, numbers);
        }
    }

    @Override
    public long xxHashCode() {
        if (isNull()) {
            return NULL_HASH_CODE;
        }
        OriginalTimestamp originalTimestamp = new OriginalTimestamp(datetimeValue());
        byte[] rawBytes = originalTimestamp.toString().getBytes(StandardCharsets.UTF_8);
        return XxHash64.hash(rawBytes, 0, rawBytes.length);
    }

    @Override
    public void makeSortKey(byte[] result, int len) {
        System.arraycopy(packedBinary, 0, result, 0, len);
    }

    @Override
    int calPacketLength() {
        return 4 + (fieldType.getScale() + 1) / 2;
    }

    @Override
    public MysqlDateTime datetimeValue(int timeParseFlags, SessionProperties sessionProperties) {
        ZoneId zoneId = sessionProperties.getTimezone();
        MySQLTimeVal timeVal = readFromBinary();
        MysqlDateTime mysqlDateTime = MySQLTimeConverter.convertTimestampToDatetime(timeVal, zoneId);
        mysqlDateTime.setSqlType(Types.TIMESTAMP);
        return mysqlDateTime;
    }

    @Override
    public MysqlDateTime timeValue(int timeParseFlags, SessionProperties sessionProperties) {
        MysqlDateTime datetime = datetimeValue(timeParseFlags, sessionProperties);
        datetime.setSqlType(Types.TIME);
        datetime.setYear(0);
        datetime.setMonth(0);
        datetime.setDay(0);
        return datetime;
    }

    @Override
    public long longValue(SessionProperties sessionProperties) {
        MysqlDateTime datetime = datetimeValue(0, sessionProperties);
        if (datetime == null) {
            return 0L;
        }
        // convert datetime to long value with rounding. (distinct from packed long)
        return MySQLTimeConverter.datetimeToLongRound(datetime);
    }

    @Override
    public Slice stringValue(SessionProperties sessionProperties) {
        int scale = fieldType.getScale();
        MysqlDateTime datetime = datetimeValue(0, sessionProperties);
        if (datetime == null) {
            return null;
        }
        String str = datetime.toDatetimeString(scale);
        return Slices.utf8Slice(str);
    }

    @Override
    public MySQLTimeVal timestampValue(int timeParseFlags, SessionProperties sessionProperties) {
        MySQLTimeVal timeVal = readFromBinary();
        return timeVal;
    }

    @Override
    public byte[] rawBytes() {
        return packedBinary;
    }

    private TypeConversionStatus storeInteger(long value, SessionProperties sessionProperties) {
        TimeParseStatus status = getCachedStatus();
        if (value < 0) {
            reset();
            status.addWarning(TimeParserFlags.FLAG_TIME_WARN_OUT_OF_RANGE);
            return TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
        }
        long sqlModeFlag = sessionProperties.getSqlModeFlag();
        MysqlDateTime mysqlDateTime = NumericTimeParser.parseDatetimeFromInteger(value, dateFlags(sessionProperties));
        if (mysqlDateTime == null) {
            // bad value
            reset();
            return TypeConversionStatus.TYPE_ERR_BAD_VALUE;
        }

        // integer value cannot be cast with fractional.
        mysqlDateTime.setSecondPart(0L);
        return storeMysqlDatetime(mysqlDateTime, sessionProperties);
    }

    private TypeConversionStatus storeTemporal(MysqlDateTime mysqlDateTime, SessionProperties sessionProperties,
                                               final int sqlType) {
        TypeConversionStatus conversionStatus;
        switch (sqlType) {
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
        case Types.DATE:
        case Types.TIMESTAMP:
        case Types.TIMESTAMP_WITH_TIMEZONE: {
            // check validity of datetime according to sql mode.
            boolean notZeroDate = mysqlDateTime.getYear() != 0
                || mysqlDateTime.getMonth() != 0
                || mysqlDateTime.getDay() != 0;
            TimeParseStatus status = getCachedStatus();
            boolean invalid = MySQLTimeTypeUtil.isDateInvalid(
                mysqlDateTime,
                notZeroDate,
                dateFlags(sessionProperties),
                status);
            if (invalid) {
                // set null if invalid.
                conversionStatus = TypeConversionStatus.fromParseStatus(status);
                reset();
                break;
            } else {
                conversionStatus = storeMysqlDatetime(mysqlDateTime, sessionProperties);
                break;
            }
        }
        case Types.TIME:
        case Types.TIME_WITH_TIMEZONE: {
            // todo
            reset();
            conversionStatus = TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
            break;
        }
        default:
            reset();
            conversionStatus = TypeConversionStatus.TYPE_WARN_TRUNCATED;
        }
        return conversionStatus;
    }

    private TypeConversionStatus storeMysqlDatetime(MysqlDateTime mysqlDateTime, SessionProperties sessionProperties) {
        // check if need to round.
        int scale = fieldType.getScale();
        if (MySQLTimeCalculator.needToRound((int) mysqlDateTime.getSecondPart(), scale)) {
            // round to proper scale.
            mysqlDateTime = MySQLTimeCalculator.roundDatetime(mysqlDateTime, scale);
        }
        if (mysqlDateTime == null) {
            reset();
            TimeParseStatus status = getCachedStatus();
            return TypeConversionStatus.fromParseStatus(status);
        } else {
            return storeTimestampInternal(mysqlDateTime, sessionProperties);
        }
    }

    private TypeConversionStatus storeTimestampInternal(MysqlDateTime mysqlDateTime,
                                                        SessionProperties sessionProperties) {
        // store internal
        TimeParseStatus status = getCachedStatus();
        ZoneId zoneId = sessionProperties.getTimezone();
        // No need to do check_date(TIME_NO_ZERO_IN_DATE)
        MySQLTimeVal timeVal = MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(mysqlDateTime, status, zoneId);
        if (timeVal == null) {
            // for error time value, set to zero.
            timeVal = new MySQLTimeVal();
        }
        TypeConversionStatus conversionStatus = TypeConversionStatus.fromParseStatus(status);
        // store to binary
        storeAsBinary(timeVal);
        return conversionStatus;
    }

    private TypeConversionStatus storeString(String str, SessionProperties sessionProperties) {
        return storeBytes(str.getBytes(), sessionProperties);
    }

    private TypeConversionStatus storeSlice(Slice slice, SessionProperties sessionProperties) {
        Object base = slice.getBase();
        byte[] rawBytes;
        if (base instanceof byte[]) {
            // avoid to copy.
            rawBytes = (byte[]) base;
        } else {
            // need to copy.
            rawBytes = slice.getBytes();
        }
        return storeBytes(rawBytes, sessionProperties);
    }

    /**
     * Parse from bytes according to destination sql type and session sql mode.
     */
    private TypeConversionStatus storeBytes(byte[] bytes, SessionProperties sessionProperties) {
        TypeConversionStatus conversionStatus;
        TimeParseStatus status = getCachedStatus();
        // parse string to mysql datetime
        MysqlDateTime mysqlDateTime = StringTimeParser.parseDatetime(bytes, dateFlags(sessionProperties), status);

        if (mysqlDateTime == null) {
            if (status.checkWarnings(TimeParserFlags.FLAG_TIME_WARN_ZERO_DATE
                | TimeParserFlags.FLAG_TIME_WARN_ZERO_IN_DATE)
                && !SQLMode.isStrictMode(sessionProperties.getSqlModeFlag())) {
                conversionStatus = TypeConversionStatus.TYPE_NOTE_TIME_TRUNCATED;
            } else {
                conversionStatus = TypeConversionStatus.TYPE_ERR_BAD_VALUE;
            }
            // When time parser returns error, mysql datetime has been set to
            // 0 so there's nothing to store in the field.
            reset();
        } else {
            conversionStatus = TypeConversionStatus.fromParseStatus(status);
            // store as mysql datetime
            TypeConversionStatus tempConversionStatus = storeMysqlDatetime(mysqlDateTime, sessionProperties);
            // Return the most serious error of the two, see type conversion status
            if (tempConversionStatus.getCode() > conversionStatus.getCode()) {
                conversionStatus = tempConversionStatus;
            }
        }
        return conversionStatus;
    }

    private void storeAsBinary(MySQLTimeVal timeVal) {
        int scale = fieldType.getScale();
        byte[] bytes = packedBinary;
        long sec = timeVal.getSeconds();
        long nano = timeVal.getNano();

        // to store seconds value
        int i0 = (int) sec;
        bytes[3] = (byte) (i0 & 0xFF);
        bytes[2] = (byte) ((i0 >> 8) & 0xFF);
        bytes[1] = (byte) ((i0 >> 16) & 0xFF);
        bytes[0] = (byte) ((i0 >> 24) & 0xFF);
        switch (scale) {
        case 1:
        case 2:
            bytes[4] = (byte) ((nano / 10_000_000) & 0xFF);
            break;
        case 3:
        case 4:
            int i1 = (int) (nano / 100_000);
            bytes[5] = (byte) (i1 & 0xFF);
            bytes[4] = (byte) ((i1 >> 8) & 0xFF);
            break;
        case 5:
        case 6:
            int i2 = (int) (nano / 1000);
            bytes[6] = (byte) (i2 & 0xFF);
            bytes[5] = (byte) ((i2 >> 8) & 0xFF);
            bytes[4] = (byte) ((i2 >> 16) & 0xFF);
            break;
        case 0:
        default:
            break;
        }
    }

    public MySQLTimeVal readFromBinary() {
        int scale = fieldType.getScale();
        byte[] bytes = packedBinary;
        MySQLTimeVal timeVal = new MySQLTimeVal();

        // get seconds value from binary
        long seconds = Byte.toUnsignedInt(bytes[3])
            + (Byte.toUnsignedInt(bytes[2]) << 8)
            + (Byte.toUnsignedInt(bytes[1]) << 16)
            + (Byte.toUnsignedInt(bytes[0]) << 24);
        timeVal.setSeconds(seconds);

        // get nano second from binary
        long nano;
        switch (scale) {
        case 1:
        case 2:
            nano = (bytes[4]) * 10_000_000;
            break;
        case 3:
        case 4:
            nano = ((Byte.toUnsignedInt(bytes[5])) + (Byte.toUnsignedInt(bytes[4]) << 8)) * 100_000;
            break;
        case 5:
        case 6:
            if ((Byte.toUnsignedInt(bytes[4]) & 128) != 0) {
                nano = (int) ((Integer.toUnsignedLong(255) << 24)
                    | (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[4])) << 16)
                    | (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[5])) << 8)
                    | Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[6])));
            } else {
                nano = (int) ((Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[4])) << 16)
                    | (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[5])) << 8)
                    | Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[6])));
            }
            nano *= 1000;
            break;
        case 0:
        default:
            nano = 0;
        }
        timeVal.setNano(nano);
        return timeVal;
    }

    private TimeParseStatus getCachedStatus() {
        if (cachedStatus == null) {
            cachedStatus = new TimeParseStatus();
        }
        cachedStatus.clear();
        return cachedStatus;
    }
}
