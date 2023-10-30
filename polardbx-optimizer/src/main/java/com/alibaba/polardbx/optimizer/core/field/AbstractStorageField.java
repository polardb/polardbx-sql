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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.rpc.compatible.XResultSet;
import com.alibaba.polardbx.rpc.result.XResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.sql.ResultSet;
import java.util.List;

public abstract class AbstractStorageField implements StorageField {
    protected static final int UNSET_PACKET_LEN = -1;
    /**
     * The field type of partition key.
     */
    protected final DataType<?> fieldType;

    protected boolean isNull;
    protected int cachedPacketLength;

    /**
     * Cache the last conversion status.
     */
    protected TypeConversionStatus cachedTypeConversionStatus;

    protected AbstractStorageField(DataType<?> fieldType) {
        this.fieldType = fieldType;
        this.isNull = false;
        this.cachedPacketLength = UNSET_PACKET_LEN;
    }

    @Override
    public TypeConversionStatus store(Object value, DataType<?> resultType) {
        return store(value, resultType, SessionProperties.empty());
    }

    @Override
    public TypeConversionStatus store(Object value, DataType<?> resultType, SessionProperties sessionProperties) {
        // cache the last storage status into the cachedTypeConversionStatus
        return cacheTypeConversionStatus(storeInternal(value, resultType, sessionProperties));
    }

    @Override
    public TypeConversionStatus store(ResultSet rs, int columnIndex, SessionProperties sessionProperties) {
        try {
            if (rs.isWrapperFor(XResultSet.class)) {
                // for X-protocol mode, read from XResult.
                XResult xResult = rs.unwrap(XResultSet.class).getXResult();
                int columnId = columnIndex - 1;

                List<ByteString> row = xResult.current().getRow();
                List<PolarxResultset.ColumnMetaData> metaData = xResult.getMetaData();
                final PolarxResultset.ColumnMetaData meta = metaData.get(columnId);
                final ByteString byteString = row.get(columnId);

                // handle null value
                final byte[] rawBytes = byteString.toByteArray();
                if (0 == rawBytes.length) {
                    setNull();
                    return TypeConversionStatus.TYPE_OK;
                }

                return cacheTypeConversionStatus(
                    storeXProtocolInternal(xResult, meta, byteString, columnIndex, sessionProperties));
            } else {
                // for JDBC mode, read from ResultSet.
                return cacheTypeConversionStatus(
                    storeJdbcInternal(rs, columnIndex, sessionProperties));
            }
        } catch (Exception e) {
            GeneralUtil.nestedException(e);
        }
        return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
    }

    @Override
    public TypeConversionStatus store(ResultSet rs, int columnIndex) {
        return store(rs, columnIndex, SessionProperties.empty());
    }

    /**
     * Store the data in designated datatype
     */
    protected abstract TypeConversionStatus
    storeInternal(Object value,
                  DataType<?> resultType,
                  SessionProperties sessionProperties);

    /**
     * Store the data in columnIndex position from XResult.
     */
    protected abstract TypeConversionStatus
    storeXProtocolInternal(XResult xResult,
                           PolarxResultset.ColumnMetaData meta,
                           ByteString byteString,
                           int columnIndex,
                           SessionProperties sessionProperties)
        throws Exception;

    /**
     * Store the data in columnIndex position from ResultSet.
     */
    protected abstract TypeConversionStatus
    storeJdbcInternal(ResultSet rs,
                      int columnIndex,
                      SessionProperties sessionProperties)
        throws Exception;

    /**
     * Preserve the last conversion status.
     */
    protected TypeConversionStatus cacheTypeConversionStatus(TypeConversionStatus typeConversionStatus) {
        return this.cachedTypeConversionStatus = typeConversionStatus;
    }

    @Override
    public TypeConversionStatus lastStatus() {
        return cachedTypeConversionStatus;
    }

    @Override
    public int packetLength() {
        if (cachedPacketLength == UNSET_PACKET_LEN) {
            cachedPacketLength = calPacketLength();
        }
        return cachedPacketLength;
    }

    abstract int calPacketLength();

    protected CharsetHandler getCharsetHandler() {
        return getCollationHandler().getCharsetHandler();
    }

    @Override
    public MysqlDateTime datetimeValue(int timeParseFlags, SessionProperties sessionProperties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MysqlDateTime datetimeValue() {
        return datetimeValue(0, SessionProperties.empty());
    }

    @Override
    public MysqlDateTime timeValue(int timeParseFlags, SessionProperties sessionProperties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MysqlDateTime timeValue() {
        return timeValue(0, SessionProperties.empty());
    }

    @Override
    public MySQLTimeVal timestampValue(int timeParseFlags, SessionProperties sessionProperties) {
        MysqlDateTime mysqlDateTime = datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, sessionProperties);
        MySQLTimeVal timeVal = MySQLTimeConverter.convertDatetimeToTimestamp(
            mysqlDateTime, null, sessionProperties.getTimezone());
        return timeVal;
    }

    @Override
    public MySQLTimeVal timestampValue() {
        return timestampValue(0, SessionProperties.empty());
    }

    @Override
    public long longValue() {
        return longValue(SessionProperties.empty());
    }

    @Override
    public long longValue(SessionProperties sessionProperties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice stringValue() {
        return stringValue(SessionProperties.empty());
    }

    @Override
    public Slice stringValue(SessionProperties sessionProperties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] rawBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull() {
        return isNull;
    }

    @Override
    public void setNull(boolean isNull) {
        if (isNull) {
            setNull();
        } else {
            this.isNull = false;
        }
    }

    @Override
    public DataType dataType() {
        return fieldType;
    }

    // Just for debug and print
    @Override
    public String toString() {
        return isNull ? "null" : new String(this.stringValue().getBytes());
    }

    /**
     * Copy from com.taobao.tddl.executor.Xprotocol.XRowSet#getU64(com.mysql.cj.polarx.protobuf.PolarxResultset.ColumnMetaData.FieldType, com.google.protobuf.CodedInputStream)
     */
    protected static long getU64(PolarxResultset.ColumnMetaData.FieldType type, CodedInputStream stream) throws
        IOException {
        final long val;
        switch (type) {
        case SINT:
            val = stream.readSInt64();
            break;

        case UINT:
        case BIT:
            val = stream.readUInt64(); // Ignore overflow.
            break;

        case FLOAT:
            val = (long) stream.readFloat();
            break;

        case DOUBLE:
            val = (long) stream.readDouble();
            break;

        case DECIMAL:
            byte scale = stream.readRawByte();
            // we allocate an extra char for the sign
            CharBuffer unscaledString = CharBuffer.allocate(2 * stream.getBytesUntilLimit());
            unscaledString.position(1);
            byte sign = 0;
            // read until we encounter the sign bit
            while (true) {
                int b = 0xFF & stream.readRawByte();
                if ((b >> 4) > 9) {
                    sign = (byte) (b >> 4);
                    break;
                }
                unscaledString.append((char) ((b >> 4) + '0'));
                if ((b & 0x0f) > 9) {
                    sign = (byte) (b & 0x0f);
                    break;
                }
                unscaledString.append((char) ((b & 0x0f) + '0'));
            }
            if (stream.getBytesUntilLimit() > 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                    "Did not read all bytes while decoding decimal. Bytes left: " + stream
                        .getBytesUntilLimit());
            }
            switch (sign) {
            case 0xa:
            case 0xc:
            case 0xe:
            case 0xf:
                unscaledString.put(0, '+');
                break;
            case 0xb:
            case 0xd:
                unscaledString.put(0, '-');
                break;
            }
            // may have filled the CharBuffer or one remaining. need to remove it before toString()
            int characters = unscaledString.position();
            unscaledString.clear(); // reset position
            BigInteger unscaled = new BigInteger(unscaledString.subSequence(0, characters).toString());
            val = (new BigDecimal(unscaled, scale)).longValue();
            break;

        case BYTES:
            val = Long.parseLong(new String(stream.readRawBytes(stream.getBytesUntilLimit() - 1)));
            break;

        default:
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                "Failed to get U64 from type " + type.name() + ".");
        }
        return val;
    }
}
