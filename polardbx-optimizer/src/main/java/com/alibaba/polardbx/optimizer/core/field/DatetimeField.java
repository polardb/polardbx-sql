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
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.NumericTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.charset.CollationHandlers;
import com.alibaba.polardbx.common.collation.CollationHandler;
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
import java.util.Arrays;

/**
 * datetime(N) type
 * In string context: YYYY-MM-DD HH:MM:DD.FFFFFF
 * In number context: YYYYMMDDHHMMDD.FFFFFF
 * Stored as a 8 byte value.
 */
public class DatetimeField extends AbstractTemporalField {
    private static final long UNSET_DEFAULT_PACKED_LONG = 0L;
    private TimeParseStatus cachedStatus;
    private long packedLong;
    private byte[] packedBinary;

    public DatetimeField(DataType<?> fieldType) {
        super(fieldType);
        this.packedLong = UNSET_DEFAULT_PACKED_LONG;
        int packedLen = packetLength();
        this.packedBinary = new byte[packedLen];
        // init cache status
        getCachedStatus();
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

    /**
     * We support implicit cast as follow:
     * exact number to datetime type.
     * temporal value to datetime type.
     * string value to datetime type.
     */
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
        packedLong = UNSET_DEFAULT_PACKED_LONG;
        packedBinary = storeAsBinary();
        cachedStatus.clear();
        cachedPacketLength = UNSET_PACKET_LEN;
        isNull = false;
    }

    @Override
    public void setNull() {
        isNull = true;
        packedLong = 0L;
        packedBinary = storeAsBinary();
        cachedStatus.clear();
    }

    @Override
    public MysqlDateTime datetimeValue(int timeParseFlag, SessionProperties sessionProperties) {
        if (isNull()) {
            return null;
        }
        // parse mysql date time from packed long value.
        MysqlDateTime mysqlDateTime = TimeStorage.readTimestamp(packedLong);
        // check fuzzy date
        boolean fuzzyDate = TimeParserFlags.check(timeParseFlag, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
        if (!fuzzyDate && (mysqlDateTime.getMonth() == 0 || mysqlDateTime.getDay() == 0)) {
            // invalid fuzzy date
            return null;
        }
        return mysqlDateTime;
    }

    /**
     * NOTE: The long value of datetime is not the packed long value of datetime value.
     */
    @Override
    public long longValue(SessionProperties sessionProperties) {
        MysqlDateTime mysqlDateTime = TimeStorage.readTimestamp(packedLong);
        if (mysqlDateTime == null) {
            return 0L;
        }
        // convert datetime to long value with rounding. (distinct from packed long)
        return MySQLTimeConverter.datetimeToLongRound(mysqlDateTime);
    }

    @Override
    public Slice stringValue(SessionProperties sessionProperties) {
        int scale = fieldType.getScale();
        MysqlDateTime mysqlDateTime = TimeStorage.readTimestamp(packedLong);
        if (mysqlDateTime == null) {
            return null;
        }
        String str = mysqlDateTime.toDatetimeString(scale);
        return Slices.utf8Slice(str);
    }

    @Override
    public byte[] rawBytes() {
        return packedBinary;
    }

    @Override
    public MySQLStandardFieldType standardFieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_DATETIME;
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
        System.arraycopy(packedBinary, 0, result, 0, len);
    }

    @Override
    int calPacketLength() {
        int scale = fieldType.getScale();
        return 5 + (scale + 1) / 2;
    }

    @Override
    public CollationHandler getCollationHandler() {
        return CollationHandlers.COLLATION_HANDLER_BINARY;
    }

    // for test
    public long readFromBinary() {
        int scale = fieldType.getScale();
        byte[] bytes = packedBinary;

        // from year to seconds
        long intPart =
            Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[4]))
                + (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[3])) << 8)
                + (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[2])) << 16)
                + (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[1])) << 24)
                + (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[0])) << 32) - 0x8000000000L;

        // part of fractional (nano seconds)
        int frac;
        switch (scale) {
        case 1:
        case 2:
            frac = ((int) bytes[5]) * 10000;
            break;
        case 3:
        case 4:
            frac = (Byte.toUnsignedInt(bytes[6]) + ((int) (bytes[5]) << 8)) * 100;
            break;
        case 5:
        case 6:
            if ((Byte.toUnsignedInt(bytes[5]) & 128) != 0) {
                frac = (int) ((Integer.toUnsignedLong(255) << 24)
                    | (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[5])) << 16)
                    | (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[6])) << 8)
                    | Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[7])));
            } else {
                frac = (int) ((Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[5])) << 16)
                    | (Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[6])) << 8)
                    | Integer.toUnsignedLong(Byte.toUnsignedInt(bytes[7])));
            }
            break;
        case 0:
        default:
            return intPart << 24;
        }
        return (intPart << 24) + frac;
    }

    // for test
    public long rawPackedLong() {
        return packedLong;
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
        if (value < 0) {
            // out of range
            return TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
        }
        MysqlDateTime mysqlDateTime = NumericTimeParser.parseDatetimeFromInteger(value, dateFlags(sessionProperties));
        if (mysqlDateTime == null) {
            // bad value
            return TypeConversionStatus.TYPE_ERR_BAD_VALUE;
        }

        // integer value cannot be cast with fractional.
        mysqlDateTime.setSecondPart(0L);
        return storeMysqlDatetime(mysqlDateTime, sessionProperties);
    }

    private TypeConversionStatus storeMysqlDatetime(MysqlDateTime mysqlDateTime, SessionProperties sessionProperties) {
        int sqlType = mysqlDateTime.getSqlType();
        switch (mysqlDateTime.getSqlType()) {
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
        case Types.TIMESTAMP:
        case Types.DATE: {
            // check validity of datetime according to sql mode.
            boolean notZeroDate =
                mysqlDateTime.getYear() != 0 || mysqlDateTime.getMonth() != 0 || mysqlDateTime.getDay() != 0;
            TimeParseStatus status = getCachedStatus();
            boolean invalid =
                MySQLTimeTypeUtil
                    .isDateInvalid(mysqlDateTime, notZeroDate, dateFlags(sessionProperties), status);
            if (invalid) {
                // set null if invalid.
                setNull();
                return TypeConversionStatus.fromParseStatus(status);
            }

            // check if need to round.
            int scale = fieldType.getScale();
            if (sqlType != Types.DATE &&
                MySQLTimeCalculator.needToRound((int) mysqlDateTime.getSecondPart(), scale)) {
                // round to proper scale.
                mysqlDateTime = MySQLTimeCalculator.roundDatetime(mysqlDateTime, scale);
            }

            // pack mysql datetime to 8 bytes
            this.packedLong = sqlType == Types.DATE ?
                TimeStorage.writeDate(mysqlDateTime)
                : TimeStorage.writeTimestamp(mysqlDateTime);
            this.packedBinary = storeAsBinary();
            break;
        }
        case Types.TIME:
            // todo from time to datetime
        default:
            setNull();
            return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
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
        TypeConversionStatus typeConversionStatus;
        TimeParseStatus status = getCachedStatus();
        int parseFlags = dateFlags(sessionProperties);
        MysqlDateTime mysqlDateTime = StringTimeParser.parseDatetime(bytes, parseFlags, status);
        if (mysqlDateTime == null) {
            if (status.checkWarnings(TimeParserFlags.FLAG_TIME_WARN_ZERO_DATE
                | TimeParserFlags.FLAG_TIME_WARN_ZERO_IN_DATE)
                && !SQLMode.isStrictMode(sessionProperties.getSqlModeFlag())) {
                typeConversionStatus = TypeConversionStatus.TYPE_NOTE_TIME_TRUNCATED;
            } else {
                typeConversionStatus = TypeConversionStatus.TYPE_ERR_BAD_VALUE;
            }
            // When time parser returns error, mysql datetime has been set to
            // 0 so there's nothing to store in the field.
            reset();
        } else {
            typeConversionStatus = TypeConversionStatus.fromParseStatus(status);
            TypeConversionStatus typeConversionStatus1 = storeInternalWithRound(mysqlDateTime, typeConversionStatus);

            if (typeConversionStatus1.getCode() > typeConversionStatus.getCode()) {
                typeConversionStatus = typeConversionStatus1;
            }
        }
        return typeConversionStatus;
    }

    private TypeConversionStatus storeInternalWithRound(MysqlDateTime mysqlDateTime,
                                                        TypeConversionStatus typeConversionStatus) {
        // check if need to round.
        int scale = fieldType.getScale();
        if (MySQLTimeCalculator.needToRound((int) mysqlDateTime.getSecondPart(), scale)) {
            // round to proper scale.
            // todo need set status
            mysqlDateTime = MySQLTimeCalculator.roundDatetime(mysqlDateTime, scale);
            if (mysqlDateTime == null) {
                reset();
                return typeConversionStatus;
            }
        }
        return storeTemporalInternal(mysqlDateTime);
    }

    private TypeConversionStatus storeTemporalInternal(MysqlDateTime mysqlDateTime) {
        // pack mysql datetime to 8 bytes
        this.packedLong = TimeStorage.writeTimestamp(mysqlDateTime);
        this.packedBinary = storeAsBinary();
        return TypeConversionStatus.TYPE_OK;
    }

    /**
     * Store in-memory numeric packed datetime representation to disk.
     */
    private byte[] storeAsBinary() {
        int scale = fieldType.getScale();
        long l = packedLong;
        byte[] bytes = packedBinary;

        // to store year ~ seconds
        long l0 = (l >> 24) + 0x8000000000L;
        long l1 = Integer.toUnsignedLong((int) l0);
        long l2 = l0 >> 32;
        bytes[4] = (byte) (l1 & 0xFF);
        bytes[3] = (byte) ((l1 >> 8) & 0xFF);
        bytes[2] = (byte) ((l1 >> 16) & 0xFF);
        bytes[1] = (byte) ((l1 >> 24) & 0xFF);
        bytes[0] = (byte) (l2 & 0xFF);

        // to store fractional (nano seconds)
        long l3 = (l % (1L << 24));
        switch (scale) {
        case 1:
        case 2:
            bytes[5] = (byte) ((l3 / 10000) & 0xFF);
            break;
        case 3:
        case 4:
            long l4 = Integer.toUnsignedLong((int) (l3 / 100));
            bytes[6] = (byte) (l4 & 0xFF);
            bytes[5] = (byte) ((l4 >> 8) & 0xFF);
            break;
        case 5:
        case 6:
            long l5 = Integer.toUnsignedLong((int) l3);
            bytes[7] = (byte) (l5 & 0xFF);
            bytes[6] = (byte) ((l5 >> 8) & 0xFF);
            bytes[5] = (byte) ((l5 >> 16) & 0xFF);
            break;
        case 0:
        default:
            break;
        }
        return bytes;
    }

    private TimeParseStatus getCachedStatus() {
        if (cachedStatus == null) {
            cachedStatus = new TimeParseStatus();
        }
        cachedStatus.clear();
        return cachedStatus;
    }
}
