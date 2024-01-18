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

import com.alibaba.polardbx.common.SQLModeFlags;
import com.alibaba.polardbx.optimizer.config.table.charset.CollationHandlers;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.parser.NumericTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Arrays;

/**
 * The new date type
 * Stored as 3 bytes
 * In number context: YYYYMMDD
 */
public class DateField extends AbstractTemporalField {
    private TimeParseStatus cachedStatus;
    private byte[] packedBinary;

    public DateField(DataType<?> fieldType) {
        super(fieldType);
        int packedLen = packetLength();
        this.packedBinary = new byte[packedLen];
        // init cache status
        getCachedStatus();
    }

    @Override
    int calPacketLength() {
        return 3;
    }

    @Override
    public CollationHandler getCollationHandler() {
        return CollationHandlers.COLLATION_HANDLER_BINARY;
    }

    @Override
    public MySQLStandardFieldType standardFieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_NEWDATE;
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
    protected TypeConversionStatus storeJdbcInternal(ResultSet rs, int columnIndex, SessionProperties sessionProperties)
        throws Exception {
        // directly write raw bytes
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
                return storeMysqlDatetime(mysqlDateTime, sessionProperties);
            } else if (value instanceof Timestamp) {
                // Bad case: Use jdbc-style temporal value (java.sql.Timestamp)
                ZoneId zoneId = sessionProperties.getTimezone();
                MysqlDateTime mysqlDateTime = MySQLTimeConverter.convertTimestampToDatetime((Timestamp) value, zoneId);
                mysqlDateTime.setSqlType(Types.TIMESTAMP);
                return storeMysqlDatetime(mysqlDateTime, sessionProperties);
            } else if (value instanceof Date) {
                // Bad case: Use jdbc-style temporal value (java.sql.Date)
                ZoneId zoneId = sessionProperties.getTimezone();
                MysqlDateTime mysqlDateTime =
                    MySQLTimeConverter.toMySqlDatetime(zoneId, ((Date) value).getTime() / 1000L, 0);
                mysqlDateTime.setSqlType(Types.DATE);
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
    public void makeSortKey(byte[] result, int len) {
        result[0] = packedBinary[0];
        result[1] = packedBinary[1];
        result[2] = packedBinary[2];
    }

    @Override
    public MysqlDateTime datetimeValue(int timeParseFlags, SessionProperties sessionProperties) {
        return getDateInternal();
    }

    @Override
    public MysqlDateTime timeValue(int timeParseFlags, SessionProperties sessionProperties) {
        // zero time
        return new MysqlDateTime();
    }

    @Override
    public long longValue(SessionProperties sessionProperties) {
        int storedLong = readFromBinary();
        long j = storedLong;
        j = (j % 32) + (j / 32 % 16) * 100L + (j / (16 * 32)) * 10000;
        return j;
    }

    @Override
    public Slice stringValue(SessionProperties sessionProperties) {
        MysqlDateTime mysqlDateTime = getDateInternal();
        String str = mysqlDateTime.toDateString();
        return Slices.utf8Slice(str);
    }

    @Override
    public byte[] rawBytes() {
        return packedBinary;
    }

    @Override
    public boolean isNull() {
        return super.isNull();
    }

    @Override
    public void setNull(boolean isNull) {
        super.setNull(isNull);
    }

    @Override
    protected int dateFlags(SessionProperties sessionProperties) {
        int flag = TimeParserFlags.FLAG_TIME_FUZZY_DATE;
        long sqlModeFlag = sessionProperties.getSqlModeFlag();
        if (SQLModeFlags.check(sqlModeFlag, SQLModeFlags.MODE_NO_ZERO_DATE)) {
            flag |= TimeParserFlags.FLAG_TIME_NO_ZERO_DATE;
        }
        if (SQLModeFlags.check(sqlModeFlag, SQLModeFlags.MODE_NO_ZERO_IN_DATE)) {
            flag |= TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE;
        }
        if (SQLModeFlags.check(sqlModeFlag, SQLModeFlags.MODE_INVALID_DATES)) {
            flag |= TimeParserFlags.FLAG_TIME_INVALID_DATES;
        }
        return flag;
    }

    private TypeConversionStatus storeInteger(long value, SessionProperties sessionProperties) {
        TimeParseStatus status = getCachedStatus();
        if (value < 0) {
            reset();
            status.addWarning(TimeParserFlags.FLAG_TIME_WARN_OUT_OF_RANGE);
            return TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
        }
        MysqlDateTime mysqlDateTime = NumericTimeParser.parseDatetimeFromInteger(value, dateFlags(sessionProperties));
        if (mysqlDateTime == null) {
            // bad value
            reset();
            return TypeConversionStatus.TYPE_ERR_BAD_VALUE;
        }

        // integer value cannot be cast with fractional.
        mysqlDateTime.setSecondPart(0L);

        // store internal
        return storeInternal(mysqlDateTime);
    }

    private TypeConversionStatus storeInternal(MysqlDateTime mysqlDateTime) {
        long nr = mysqlDateTime.getDay() + mysqlDateTime.getMonth() * 32 + mysqlDateTime.getYear() * 16 * 32;
        storeAsBinary((int) (nr & 0xFFFFFFFF));

        boolean nonZeroTime = mysqlDateTime.getHour() != 0
            || mysqlDateTime.getMinute() != 0
            || mysqlDateTime.getSecond() != 0
            || mysqlDateTime.getSecondPart() != 0;
        if (nonZeroTime) {
            return TypeConversionStatus.TYPE_NOTE_TIME_TRUNCATED;
        }
        return TypeConversionStatus.TYPE_OK;
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
            // When time parser returns error, mysql datetime has been set to
            // 0 so there's nothing to store in the field.
            reset();
            int warnings = status.getWarnings();
            if (status
                .checkWarnings(TimeParserFlags.FLAG_TIME_NO_ZERO_DATE | TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE)) {
                conversionStatus = TypeConversionStatus.TYPE_NOTE_TIME_TRUNCATED;
            } else {
                conversionStatus = TypeConversionStatus.TYPE_ERR_BAD_VALUE;
            }
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
            // store internal
            return storeInternal(mysqlDateTime);
        }
    }

    private MysqlDateTime getDateInternal() {
        int storedLong = readFromBinary();
        MysqlDateTime mysqlDateTime = new MysqlDateTime();
        mysqlDateTime.setDay(storedLong & 31);
        mysqlDateTime.setMonth((storedLong >> 5) & 15);
        mysqlDateTime.setYear(storedLong >> 9);
        mysqlDateTime.setSqlType(Types.DATE);
        return mysqlDateTime;
    }

    private void storeAsBinary(int nr) {
        byte[] bytes = packedBinary;
        bytes[0] = (byte) (nr & 0xFF);
        bytes[1] = (byte) ((nr >> 8) & 0xFF);
        bytes[2] = (byte) ((nr >> 16) & 0xFF);
    }

    public int readFromBinary(byte[] bytes) {
        return (Byte.toUnsignedInt(bytes[0]) +
            (Byte.toUnsignedInt(bytes[1]) << 8) +
            (Byte.toUnsignedInt(bytes[2]) << 16));
    }

    public int readFromBinary() {
        return (Byte.toUnsignedInt(packedBinary[0]) +
            (Byte.toUnsignedInt(packedBinary[1]) << 8) +
            (Byte.toUnsignedInt(packedBinary[2]) << 16));
    }

    private TimeParseStatus getCachedStatus() {
        if (cachedStatus == null) {
            cachedStatus = new TimeParseStatus();
        }
        cachedStatus.clear();
        return cachedStatus;
    }
}
