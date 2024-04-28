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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBounds;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalRoundMod;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.datatype.RawBytesDecimalUtils;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.parser.NumericTimeParser;
import com.alibaba.polardbx.optimizer.config.table.charset.CollationHandlers;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OK;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OVERFLOW;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_TRUNCATED;

public class DecimalField extends AbstractNumericField {
    int precision;
    int scale;
    private byte[] packedBinary;
    boolean isUnsigned;

    public DecimalField(DataType<?> fieldType) {
        super(fieldType);
        packedBinary = new byte[DECIMAL_MEMORY_SIZE];
        precision = fieldType.getPrecision();
        scale = fieldType.getScale();
        isUnsigned = fieldType.isUnsigned();
    }

    @Override
    protected TypeConversionStatus storeInternal(Object value, DataType<?> resultType,
                                                 SessionProperties sessionProperties) {
        int fromSqlType = resultType.getSqlType();

        switch (resultType.fieldType()) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            if (resultType.isUnsigned()) {
                if (value instanceof BigInteger) {
                    // check range of unsigned long
                    if (((BigInteger) value).compareTo(MAX_UNSIGNED_INT64) > 0 ||
                        ((BigInteger) value).compareTo(MIN_SIGNED_INT64) < 0) {
                        Decimal decimal = Decimal.fromString((value).toString());
                        return storeDecimal(decimal.getDecimalStructure());
                    } else {
                        return storeLong(((BigInteger) value).longValue(), resultType.isUnsigned());
                    }
                } else if (value instanceof UInt64) {
                    if (((UInt64) value).compareTo(UInt64.MAX_UINT64) > 0) {
                        Decimal decimal = Decimal.fromUnsigned((UInt64) value);
                        return storeDecimal(decimal.getDecimalStructure());
                    } else {
                        return storeLong(((BigInteger) value).longValue(), resultType.isUnsigned());
                    }
                }
            }
            if (value instanceof Number) {
                long longValue = ((Number) value).longValue();
                return storeLong(longValue, resultType.isUnsigned());
            } else if (value == null) {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
            break;
        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_FLOAT:
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                return storeDouble(doubleValue);
            } else if (value == null) {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
            break;
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            if (value instanceof Decimal) {
                return storeDecimal(((Decimal) value).getDecimalStructure());
            } else if (value instanceof BigDecimal) {
                return storeDecimal(Decimal.fromBigDecimal((BigDecimal) value).getDecimalStructure());
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
                Number numericRes =
                    MySQLTimeConverter.convertTemporalToNumeric(mysqlDateTime, fromSqlType, Types.BIGINT);
                long longValue = numericRes.longValue();
                return storeLong(mysqlDateTime.isNeg() ? -longValue : longValue, resultType.isUnsigned());
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
    protected TypeConversionStatus storeXProtocolInternal(XResult xResult, PolarxResultset.ColumnMetaData meta,
                                                          ByteString byteString, int columnIndex,
                                                          SessionProperties sessionProperties) throws Exception {
        final Object val =
            XResultUtil.resultToObject(meta, byteString, true,
                    xResult.getConnection().getSession().getDefaultTimezone())
                .getKey();
        if (val instanceof Number) {
            double doubleValue = ((Number) val).doubleValue();
            return storeDouble(doubleValue);
        } else if (val instanceof BigDecimal) {
            return storeDecimal(Decimal.fromBigDecimal((BigDecimal) val).getDecimalStructure());
        } else if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            return storeBytes(bytes, sessionProperties);
        } else if (val instanceof String) {
            return storeString((String) val, sessionProperties);
        }
        return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
    }

    @Override
    protected TypeConversionStatus storeJdbcInternal(ResultSet rs, int columnIndex, SessionProperties sessionProperties)
        throws Exception {
        Object val = rs.getObject(columnIndex);
        if (val instanceof Number) {
            double doubleValue = ((Number) val).doubleValue();
            return storeDouble(doubleValue);
        } else if (val instanceof BigDecimal) {
            return storeDecimal(Decimal.fromBigDecimal((BigDecimal) val).getDecimalStructure());
        } else if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            return storeBytes(bytes, sessionProperties);
        } else if (val instanceof String) {
            return storeString((String) val, sessionProperties);
        }
        return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
    }

    @Override
    int calPacketLength() {
        return packedBinary.length;
    }

    @Override
    public MySQLStandardFieldType standardFieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_NEWDECIMAL;
    }

    @Override
    public CollationHandler getCollationHandler() {
        return CollationHandlers.COLLATION_HANDLER_LATIN1_SWEDISH_CI;
    }

    @Override
    public void reset() {
        cachedPacketLength = UNSET_PACKET_LEN;
        isNull = false;
    }

    @Override
    public void setNull() {
        Arrays.fill(packedBinary, (byte) 0);
        isNull = true;
    }

    private TypeConversionStatus storeValue(final DecimalStructure decimalValue) {
        TypeConversionStatus error = TypeConversionStatus.TYPE_OK;
        if (isUnsigned && decimalValue.isNeg()) {
            error = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            decimalValue.toZero();
        }
        int err = storeAsBinary(decimalValue, precision, scale);
        if (warnIfOverflow(err)) {
            DecimalStructure buff = new DecimalStructure();
            setValueOnOverflow(buff, decimalValue.isNeg());
            storeAsBinary(buff, precision, scale);
        }
        return (err != E_DEC_OK) ? TypeConversionStatus.decimalErrToTypeConvStatus(err) : error;
    }

    private TypeConversionStatus storeDouble(double nr) {
        DecimalStructure decimalValue = new DecimalStructure();
        byte[] bytesValue = Double.toString(nr).getBytes();
        int convErr = DecimalConverter.parseString(bytesValue, decimalValue, false);
        return storeInternalWithErrorCheck(convErr, decimalValue);
    }

    private TypeConversionStatus storeLong(long nr, boolean isUnsigned) {
        DecimalStructure decimalValue = new DecimalStructure();
        int convErr = DecimalConverter.longToDecimal(nr, decimalValue, isUnsigned);
        return storeInternalWithErrorCheck(convErr, decimalValue);
    }

    private TypeConversionStatus storeDecimal(DecimalStructure decimalValue) {
        return storeValue(decimalValue);
    }

    private TypeConversionStatus storeString(String str, SessionProperties sessionProperties) {
        DecimalStructure decimalValue = new DecimalStructure();
        int err = DecimalConverter.parseString(str.getBytes(), decimalValue, false);
        TypeConversionStatus storeStat = storeValue(decimalValue);
        return err != 0 ? TypeConversionStatus.decimalErrToTypeConvStatus(err) : storeStat;
    }

    public double doubleValue() {
        return doubleValue(SessionProperties.empty());
    }

    public double doubleValue(SessionProperties sessionProperties) {
        double result = DecimalConverter.decimalToDouble(decimalValue().getDecimalStructure());
        return result;
    }

    public Decimal decimalValue() {
        return decimalValue(SessionProperties.empty());
    }

    public Decimal decimalValue(SessionProperties sessionProperties) {
        DecimalStructure value = new DecimalStructure();
        DecimalConverter.binToDecimal(packedBinary, value, precision, scale);
        return new Decimal(value);
    }

    @Override
    public long longValue(SessionProperties sessionProperties) {
        long[] result = new long[] {DecimalConverter.decimal2Long(decimalValue().getDecimalStructure(), isUnsigned)[0]};
        return result[0];
    }

    @Override
    public Slice stringValue(SessionProperties sessionProperties) {
        String numberStr =
            new String(
                DecimalConverter.decimal2String(decimalValue().getDecimalStructure(), 0, scale, (byte) 0).getKey());
        return Slices.utf8Slice(numberStr);
    }

    @Override
    public MysqlDateTime datetimeValue(int timeParseFlags, SessionProperties sessionProperties) {
        double doubleValue = doubleValue();
        MysqlDateTime t =
            NumericTimeParser.parseNumeric(doubleValue, MySQLTimeTypeUtil.DATETIME_SQL_TYPE, timeParseFlags);
        return t;
    }

    @Override
    public MysqlDateTime timeValue(int timeParseFlags, SessionProperties sessionProperties) {
        double doubleValue = doubleValue();
        MysqlDateTime t = NumericTimeParser.parseNumeric(doubleValue, Types.TIME, timeParseFlags);
        return t;
    }

    private int storeAsBinary(DecimalStructure d, int precision, int scale) {
        int err1 = E_DEC_OK, err2;
        DecimalStructure rounded = d.copy();
        rounded.setFractions(rounded.getFractions());
        DecimalStructure to = new DecimalStructure();
        if (scale < rounded.getFractions()) {
            err1 = E_DEC_TRUNCATED;
            FastDecimalUtils.round(rounded, to, scale, DecimalRoundMod.HALF_UP);
        } else {
            to = rounded;
        }
        err2 = DecimalConverter.decimalToBin(to, packedBinary, precision, scale);
        if (err2 == 0) {
            err2 = err1;
        }
        return err2;
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
        int fromLen = packetLength();
        int toLen = Math.min(fromLen, len);
        copyInteger(result, toLen, packedBinary, fromLen, false, true);
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

    private TypeConversionStatus storeBytes(byte[] bytes, SessionProperties sessionProperties) {
        TypeConversionStatus conversionStatus = TypeConversionStatus.TYPE_OK;
        DecimalStructure value = new DecimalStructure();
        int[] ret = DecimalConverter.binToDecimal(bytes, value, precision, scale);
        if (ret[1] != E_DEC_OK) {
            return TypeConversionStatus.decimalErrToTypeConvStatus(ret[1]);
        }
        storeAsBinary(value, precision, scale);
        return conversionStatus;
    }

    boolean checkOverflow(int err) {
        return err == E_DEC_OVERFLOW;
    }

    boolean checkTruncated(int err) {
        return err == E_DEC_TRUNCATED;
    }

    void setValueOnOverflow(DecimalStructure decimalValue, boolean sign) {
        DecimalBounds.maxValue(precision, scale).copyTo(decimalValue);
        if (sign) {
            if (isUnsigned) {
                decimalValue.toZero();
            } else {
                decimalValue.setNeg(true);
            }
        }
    }

    private boolean warnIfOverflow(int err) {
        if (err == E_DEC_OVERFLOW) {
            return true;
        }
        return false;
    }

    private TypeConversionStatus storeInternalWithErrorCheck(int err, DecimalStructure value) {
        TypeConversionStatus stat = TypeConversionStatus.TYPE_OK;
        if (err != 0) {
            if (checkOverflow(err)) {
                setValueOnOverflow(value, value.isNeg());
                stat = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else if (checkTruncated(err)) {
                stat = TypeConversionStatus.TYPE_NOTE_TRUNCATED;
            }
        }
        TypeConversionStatus storeStat = storeValue(value);
        if (storeStat != TypeConversionStatus.TYPE_OK) {
            return storeStat;
        }
        return stat;
    }

    @Override
    public long xxHashCode() {
        if (isNull()) {
            return NULL_HASH_CODE;
        }
        return RawBytesDecimalUtils.hashCode(decimalValue().getMemorySegment());
    }
}
