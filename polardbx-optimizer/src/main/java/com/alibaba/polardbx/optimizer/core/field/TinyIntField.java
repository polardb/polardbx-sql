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
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UTinyIntType;
import com.alibaba.polardbx.rpc.result.XResult;
import com.google.common.primitives.UnsignedLongs;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.Types;

/**
 * MySQL tiny int data type (8bit)
 */
public class TinyIntField extends IntField {
    public static final Decimal DECIMAL_INT_8_MAX_VALUE = Decimal.fromLong(INT_8_MAX);
    public static final Decimal DECIMAL_INT_8_MIN_VALUE = Decimal.fromLong(INT_8_MIN);
    public static final Decimal DECIMAL_UNSIGNED_INT_8_MAX_VALUE =
        Decimal.fromString(Integer.toUnsignedString(UNSIGNED_INT_8_MAX));
    public static final Decimal DECIMAL_UNSIGNED_INT_8_MIN_VALUE =
        Decimal.fromString(Integer.toUnsignedString(UNSIGNED_INT_8_MIN));

    private static final int INT_8_BYTES = 1;
    final boolean isUnsigned;
    private byte pointer;

    public TinyIntField(DataType<?> fieldType) {
        super(fieldType);
        pointer = 0;
        isUnsigned = fieldType.isUnsigned();
    }

    @Override
    protected TypeConversionStatus storeXProtocolInternal(XResult xResult, PolarxResultset.ColumnMetaData meta,
                                                          ByteString byteString, int columnIndex,
                                                          SessionProperties sessionProperties) throws Exception {
        final byte[] rawBytes = byteString.toByteArray();
        final CodedInputStream stream = CodedInputStream.newInstance(rawBytes);
        short val = (short) getU64(meta.getType(), stream);
        return storeLong(val & LONG_MASK, isUnsigned);
    }

    @Override
    protected TypeConversionStatus storeJdbcInternal(ResultSet rs, int columnIndex, SessionProperties sessionProperties)
        throws Exception {
        short val = rs.getShort(columnIndex);
        if (val != 0 || !rs.wasNull()) {
            return storeLong(val & LONG_MASK, isUnsigned);
        } else {
            setNull();
            return TypeConversionStatus.TYPE_OK;
        }
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
            if (value instanceof BigDecimal) {
                return storeString(((BigDecimal) value).toPlainString(), sessionProperties);
            } else if (value instanceof BigInteger) {
                BigInteger bigInteger = (BigInteger) value;

                // The boundary value of bigint is min value of longlong and max value of ulonglong.
                // otherwise, the big integer number will be recognized as decimal value.
                if (bigInteger.compareTo(MAX_UNSIGNED_INT64) > 0 || bigInteger.compareTo(MIN_SIGNED_INT64) < 0) {
                    BigDecimal decimalNumber = new BigDecimal(bigInteger);
                    return storeBigDecimal(decimalNumber);
                } else {
                    return storeString(bigInteger.toString(), sessionProperties);
                }
            } else if (value instanceof Number) {
                boolean isDataTypeUnsigned = resultType.isUnsigned();
                long longValue = ((Number) value).longValue();
                return storeLong(longValue, isDataTypeUnsigned);
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
                return storeDecimal((Decimal) value);
            } else if (value instanceof BigDecimal) {
                return storeBigDecimal((BigDecimal) value);
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
                    MySQLTimeConverter.convertTemporalToNumeric(mysqlDateTime, fromSqlType, Types.TINYINT);
                long longValue = numericRes.longValue();
                return storeLong(mysqlDateTime.isNeg() ? -longValue : longValue, false);
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

    private TypeConversionStatus storeLong(long l, boolean isDataTypeUnsigned) {
        TypeConversionStatus conversionStatus = TypeConversionStatus.TYPE_OK;
        if (isUnsigned) {
            if (l < 0 && !isDataTypeUnsigned) {
                pointer = 0;
                return TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else if (UnsignedLongs.compare(l, ((long) 255) & 0xFFFFFFFFL) > 0) {
                pointer = (byte) 255;
                conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else {
                pointer = (byte) l;
            }
        } else {
            if (l < 0 && isDataTypeUnsigned) {
                // generate overflow
                l = 256;
            }
            if (l < -128) {
                pointer = (byte) -128;
                return TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else if (l > 127) {
                pointer = (byte) 127;
                return TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else {
                pointer = (byte) l;
            }
        }
        return conversionStatus;
    }

    private TypeConversionStatus storeDouble(double d) {
        TypeConversionStatus conversionStatus = TypeConversionStatus.TYPE_OK;
        d = Math.rint(d);
        if (isUnsigned) {
            if (d < 0.0D) {
                pointer = 0;
                conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else if (d > 255.0D) {
                pointer = (byte) 255;
                conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else {
                pointer = (byte) d;
            }
        } else {
            if (d < -128.0D) {
                pointer = (byte) -128;
                conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else if (d > 127.0D) {
                pointer = (byte) 127;
                conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
            } else {
                pointer = (byte) (int) d;
            }
        }
        return conversionStatus;
    }

    /**
     * Conversion from decimal to longlong. Checks overflow and returns
     * correct value (min/max) in case of overflow.
     */
    private TypeConversionStatus storeDecimal(Decimal decimal) {
        TypeConversionStatus conversionStatus = TypeConversionStatus.TYPE_OK;
        boolean hasOverflow = false;

        if (isUnsigned) {
            // check overflow for unsigned long.
            if (decimal.compareTo(DECIMAL_UNSIGNED_INT_8_MIN_VALUE) < 0) {
                hasOverflow = true;
                pointer = 0;
            } else if (decimal.compareTo(DECIMAL_UNSIGNED_INT_8_MAX_VALUE) > 0) {
                hasOverflow = true;
                pointer = (byte) 255;
            }
        } else {
            // check overflow for signed long.
            if (decimal.compareTo(DECIMAL_INT_8_MIN_VALUE) < 0) {
                hasOverflow = true;
                pointer = (byte) -127;
            } else if (decimal.compareTo(DECIMAL_INT_8_MAX_VALUE) > 0) {
                hasOverflow = true;
                pointer = (byte) 128;
            }
        }

        if (hasOverflow) {
            conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
        } else {
            // todo should integrate with new decimal type system.
            pointer = decimal.toBigDecimal().setScale(0, RoundingMode.HALF_UP).byteValue();
        }

        return conversionStatus;
    }

    private TypeConversionStatus storeBigDecimal(BigDecimal decimal) {
        TypeConversionStatus conversionStatus = TypeConversionStatus.TYPE_OK;
        boolean hasOverflow = false;

        if (isUnsigned) {
            // check overflow for unsigned long.
            if (decimal.compareTo(DECIMAL_UNSIGNED_INT_8_MIN_VALUE.toBigDecimal()) < 0) {
                hasOverflow = true;
                pointer = 0;
            } else if (decimal.compareTo(DECIMAL_UNSIGNED_INT_8_MAX_VALUE.toBigDecimal()) > 0) {
                hasOverflow = true;
                pointer = (byte) 255;
            }
        } else {
            // check overflow for signed long.
            if (decimal.compareTo(DECIMAL_INT_8_MIN_VALUE.toBigDecimal()) < 0) {
                hasOverflow = true;
                pointer = (byte) -127;
            } else if (decimal.compareTo(DECIMAL_INT_8_MAX_VALUE.toBigDecimal()) > 0) {
                hasOverflow = true;
                pointer = (byte) 128;
            }
        }

        if (hasOverflow) {
            conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
        } else {
            // todo should integrate with new decimal type system.
            pointer = decimal.setScale(0, RoundingMode.HALF_UP).byteValue();
        }

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

    private TypeConversionStatus storeBytes(byte[] bytes, SessionProperties sessionProperties) {
        TypeConversionStatus conversionStatus = TypeConversionStatus.TYPE_OK;

        // parse by charset-specific string numeric parser.
        long[] result = new long[3];
        CharsetHandler charsetHandler = getCharsetHandler();
        charsetHandler.parseToLongWithRound(bytes, 0, bytes.length, result, isUnsigned);
        long rounded = result[StringNumericParser.NUMERIC_INDEX];
        long parseError = result[StringNumericParser.ERROR_INDEX];
        int numberEnd = (int) result[StringNumericParser.POSITION_INDEX];

        if (isUnsigned) {
            if ((UnsignedLongs.compare(rounded, ((long) 255) & 0xFFFFFFFFL) > 0
                && (rounded = 255) != 0)
                || parseError == StringNumericParser.MY_ERRNO_ERANGE) {
                // out of range
                conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
                pointer = (byte) rounded;
                return conversionStatus;
            }
        } else {
            if (rounded < -128) {
                rounded = -128;
                // out of range
                conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
                pointer = (byte) rounded;
                return conversionStatus;
            } else if (rounded > 127) {
                rounded = 127;
                // out of range
                conversionStatus = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
                pointer = (byte) rounded;
                return conversionStatus;
            }
        }
        if (sessionProperties.getCheckLevel() != FieldCheckLevel.CHECK_FIELD_IGNORE) {
            // for non-ignorable level(like insert / update), check int
            conversionStatus = checkInt(bytes, parseError, numberEnd);
        }

        pointer = (byte) rounded;
        return conversionStatus;
    }

    @Override
    public long longValue(SessionProperties sessionProperties) {
        if (isUnsigned) {
            return ((int) pointer) & 0xFF;
        } else {
            return pointer;
        }
    }

    @Override
    public byte[] rawBytes() {
        return new byte[] {pointer};
    }

    @Override
    int calPacketLength() {
        return INT_8_BYTES;
    }

    @Override
    public MySQLStandardFieldType standardFieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_TINY;
    }

    @Override
    public void makeSortKey(byte[] result, int len) {
        if (isUnsigned) {
            result[0] = pointer;
        } else {
            result[0] = (byte) (pointer ^ 128);
        }
    }
}
