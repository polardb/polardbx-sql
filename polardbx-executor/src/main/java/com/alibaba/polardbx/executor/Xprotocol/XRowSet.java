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

package com.alibaba.polardbx.executor.Xprotocol;

import com.alibaba.polardbx.common.CrcAccumulator;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.rpc.jdbc.CharsetMapping;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.IXRowChunk;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.Blob;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.YearType;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.row.AbstractRow;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;

/**
 * @version 1.0
 */
public class XRowSet extends AbstractRow implements IXRowChunk {

    private final XResult result;
    private final TimeZone timeZone;
    private final List<PolarxResultset.ColumnMetaData> metaData;
    private final List<ByteString> row;
    private final Pair<Object, byte[]>[] cache;

    private final boolean legacy;

    public XRowSet(XResult result, CursorMeta cursorMeta, List<PolarxResultset.ColumnMetaData> metaData,
                   List<ByteString> row, boolean legacy) throws SQLException {
        super(cursorMeta);
        this.result = result;
        this.timeZone = result.getSession().getDefaultTimezone();
        this.metaData = metaData;
        this.row = row;
        this.colNum = metaData.size();
        this.cache = new Pair[metaData.size()];
        this.legacy = legacy;
    }

    public XResult getResult() {
        return result;
    }

    public int getColumnCount() {
        return metaData.size();
    }

    public byte[] fastGetBytes(int index, String targetCharset) throws Exception {
        return XResultUtil.resultToBytes(metaData.get(index), row.get(index), targetCharset);
    }

    public void fastParseToColumnVector(int index, String targetCharset, ColumnVector columnVector, int rowNumber, Optional<CrcAccumulator> accumulator)
        throws Exception {
        XResultUtil.resultToColumnVector(metaData.get(index), row.get(index), targetCharset, columnVector, rowNumber, false,
            -1, -1, -1, null, null, null, accumulator);
    }

    public void fastParseToColumnVector(int index, String targetCharset, ColumnVector columnVector, int rowNumber,
                                        ZoneId timezone, int scale, Optional<CrcAccumulator> accumulator) throws Exception {
        XResultUtil.resultToColumnVector(metaData.get(index), row.get(index), targetCharset, columnVector, rowNumber,
            false, -1, scale, -1, timezone, null, null, accumulator);
    }

    public void fastParseToColumnVector(int index, String targetCharset, ColumnVector columnVector, int rowNumber,
                                        boolean flipUnsigned, Optional<CrcAccumulator> accumulator) throws Exception {
        XResultUtil.resultToColumnVector(metaData.get(index), row.get(index), targetCharset, columnVector, rowNumber,
            flipUnsigned, -1, -1, -1, null, null, null, accumulator);
    }

    public void fastParseToColumnVector(int index, String targetCharset, ColumnVector columnVector, int rowNumber,
                                        boolean flipUnsigned, int precision, int scale, Optional<CrcAccumulator> accumulator) throws Exception {
        XResultUtil.resultToColumnVector(metaData.get(index), row.get(index), targetCharset, columnVector, rowNumber,
            flipUnsigned, precision, scale, -1, null, null, null, accumulator);
    }

    public void fastParseToColumnVector(int index, String targetCharset, ColumnVector columnVector, int rowNumber,
                                        int length, ColumnVector redundantColumnVector,
                                        BiFunction<byte[], Integer, byte[]> collationHandler, Optional<CrcAccumulator> accumulator) throws Exception {
        XResultUtil
            .resultToColumnVector(metaData.get(index), row.get(index), targetCharset, columnVector, rowNumber, false,
                -1, -1, length, null, redundantColumnVector, collationHandler, accumulator);
    }

    @Override
    public byte[] getBytes(int index) {
        if (cache[index] != null) {
            return cache[index].getValue();
        }
        try {
            cache[index] = XResultUtil.resultToObject(metaData.get(index), row.get(index), true, timeZone);
            return cache[index].getValue();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public byte[] getBytes(int index, String encoding) {
        if (cache[index] != null) {
            return cache[index].getValue();
        }
        try {
            cache[index] = XResultUtil.resultToObject(metaData.get(index), row.get(index), true, timeZone);
            final Object obj = cache[index].getKey();
            if (obj instanceof Date || obj instanceof Time || obj instanceof Timestamp) {
                return (new String(cache[index].getValue())).getBytes(TStringUtil.javaEncoding(encoding));
            } else {
                return super.getBytes(index, encoding);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private Object convert(int index, Object obj) {
        ColumnMeta cm = this.getParentCursorMeta().getColumnMeta(index);
        if (obj != null) {
            DataType dataType = cm.getDataType();

            if (dataType instanceof EnumType) {
                obj = new EnumValue((EnumType) dataType, (String) obj);
            }

            // No boolean in X-protocol.

            // For now, we only support YEAR(4).
            if (dataType instanceof YearType && obj instanceof Date) {
                obj = ((Date) obj).getYear() + 1900;
            }
        }
        return obj;
    }

    @Override
    public Object getObject(int index) {
        if (cache[index] != null) {
            return convert(index, cache[index].getKey());
        }

        try {
            cache[index] = XResultUtil.resultToObject(metaData.get(index), row.get(index), true, timeZone);
            return convert(index, cache[index].getKey());
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void setObject(int index, Object value) {
        throw new UnsupportedOperationException();
    }

    private static long bytesToLong(byte[] bytes) {
        assert bytes.length <= 8;
        long val = 0;
        for (int i = 0; i < bytes.length; i++) {
            val |= (bytes[i] & 0xFF) << ((bytes.length - i - 1) * 8);
        }
        return val;
    }

    @Override
    public void buildChunkRow(DataType[] dataTypes, BlockBuilder[] blockBuilders) {
        buildChunkRow(result, metaData, row, dataTypes, blockBuilders);
    }

    public static long getU64(PolarxResultset.ColumnMetaData.FieldType type, CodedInputStream stream) throws Exception {
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

    public static void buildChunkRow(XResult result, List<PolarxResultset.ColumnMetaData> metaData,
                                     List<ByteString> row, DataType[] dataTypes, BlockBuilder[] blockBuilders) {
        if (dataTypes.length != row.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, "XRow column number mismatch.");
        }

        try {
            for (int columnId = 0; columnId < dataTypes.length; ++columnId) {
                final Class clazz = dataTypes[columnId].getDataClass();
                final BlockBuilder builder = blockBuilders[columnId];

                final PolarxResultset.ColumnMetaData meta = metaData.get(columnId);
                final ByteString byteString = row.get(columnId);
                final byte[] rawBytes = byteString.toByteArray();

                if (0 == rawBytes.length) {
                    builder.appendNull();
                    continue;
                }

                final CodedInputStream stream = CodedInputStream.newInstance(rawBytes);
                final String encoding = CharsetMapping
                    .getJavaEncodingForCollationIndex(meta.hasCollation() ? (int) meta.getCollation() : 0);

                if (clazz == Integer.class) {
                    builder.writeInt((int) getU64(meta.getType(), stream));
                } else if (clazz == Long.class) {
                    builder.writeLong(getU64(meta.getType(), stream));
                } else if (clazz == Short.class) {
                    builder.writeShort((short) getU64(meta.getType(), stream));
                } else if (clazz == Byte.class) {
                    builder.writeByte((byte) getU64(meta.getType(), stream));
                } else if (clazz == Float.class) {
                    final float val;
                    switch (meta.getType()) {
                    case UINT:
                        val =
                            (new BigInteger(ByteBuffer.allocate(9).put((byte) 0).putLong(stream.readUInt64()).array()))
                                .floatValue();
                        break;

                    case FLOAT:
                        val = stream.readFloat();
                        break;

                    case DOUBLE:
                        val = (float) stream.readDouble();
                        break;

                    case BYTES:
                        val = Float.parseFloat(new String(rawBytes, 0, rawBytes.length - 1));
                        break;

                    default:
                        val = getU64(meta.getType(), stream);
                    }
                    builder.writeFloat(val);
                } else if (clazz == Double.class) {
                    final double val;
                    switch (meta.getType()) {
                    case UINT:
                        val =
                            (new BigInteger(ByteBuffer.allocate(9).put((byte) 0).putLong(stream.readUInt64()).array()))
                                .doubleValue();
                        break;

                    case FLOAT:
                        val = stream.readFloat();
                        break;

                    case DOUBLE:
                        val = stream.readDouble();
                        break;

                    case DECIMAL: {
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
                        val = (new BigDecimal(unscaled, scale)).doubleValue();
                    }
                    break;

                    case BYTES:
                        val = Double.parseDouble(new String(rawBytes, 0, rawBytes.length - 1));
                        break;

                    default:
                        val = getU64(meta.getType(), stream);
                    }
                    builder.writeDouble(val);
                } else if (clazz == String.class) {
                    final Pair<Object, byte[]> pair =
                        XResultUtil.resultToObject(meta, byteString, true,
                            result.getSession().getDefaultTimezone());
                    if (pair.getKey() instanceof String) {
                        builder.writeString((String) pair.getKey());
                    } else {
                        builder.writeString(new String(pair.getValue()));
                    }
                } else if (clazz == Slice.class) {
                    final Pair<Object, byte[]> pair =
                        XResultUtil.resultToObject(meta, byteString, true,
                            result.getSession().getDefaultTimezone());
                    if (pair.getKey() instanceof String) {
                        builder.writeString((String) pair.getKey());
                    } else {
                        builder.writeString(new String(pair.getValue()));
                    }
                } else if (clazz == BigInteger.class || clazz == UInt64.class) {
                    final Object val =
                        XResultUtil.resultToObject(meta, byteString, true,
                            result.getSession().getDefaultTimezone())
                            .getKey();
                    if (val instanceof BigInteger) {
                        builder.writeBigInteger((BigInteger) val);
                    } else if (val instanceof BigDecimal) {
                        builder.writeBigInteger(((BigDecimal) val).toBigInteger());
                    } else if (val instanceof Number) {
                        builder.writeBigInteger(BigInteger.valueOf(((Number) val).longValue()));
                    } else if (val instanceof byte[]) {
                        builder.writeBigInteger(BigInteger.valueOf(bytesToLong((byte[]) val)));
                    } else if (val instanceof String) {
                        builder.writeBigInteger(new BigInteger((String) val));
                    } else { // null or error type
                        builder.writeObject(val);
                    }
                } else if (clazz == Decimal.class) {
                    if (meta.getType() == PolarxResultset.ColumnMetaData.FieldType.DECIMAL) {
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
                        builder.writeDecimal(Decimal.fromBigDecimal(new BigDecimal(unscaled, scale)));
                    } else if (meta.getType() == PolarxResultset.ColumnMetaData.FieldType.SINT) {
                        builder.writeDecimal(Decimal.fromBigDecimal(new BigDecimal(stream.readSInt64())));
                    } else if (meta.getType() == PolarxResultset.ColumnMetaData.FieldType.UINT) {
                        builder.writeDecimal(Decimal.fromBigDecimal(new BigDecimal(stream.readUInt64())));
                    } else if (meta.getType() == PolarxResultset.ColumnMetaData.FieldType.BYTES) {
                        builder.writeDecimal(Decimal.fromString(new String(rawBytes, 0, rawBytes.length - 1)));
                    } else if (meta.getType() == PolarxResultset.ColumnMetaData.FieldType.FLOAT) {
                        final float val = stream.readFloat();
                        builder.writeDecimal(Decimal.fromBigDecimal(new BigDecimal(val)));
                    } else if (meta.getType() == PolarxResultset.ColumnMetaData.FieldType.DOUBLE) {
                        final double val = stream.readDouble();
                        builder.writeDecimal(Decimal.fromBigDecimal(new BigDecimal(val)));
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                            "Mismatch of clazz " + clazz.getName() + " type " + meta.getType().name() + ".");
                    }
                } else if (clazz == Timestamp.class) {
                    Pair<Object, byte[]> pair = XResultUtil.resultToObject(meta, byteString, true,
                        result.getSession().getDefaultTimezone());
                    final Object val = pair.getKey();
                    final byte[] bytes = pair.getValue();
                    if (val instanceof Timestamp || val instanceof Date) {
                        builder.writeByteArray(bytes);
                    } else if (val instanceof String) {
                        builder.writeString((String) val);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                            "Mismatch of clazz " + clazz.getName() + " type " + meta.getType().name() + ".");
                    }
                } else if (clazz == Date.class) {
                    Pair<Object, byte[]> pair = XResultUtil.resultToObject(meta, byteString, true,
                        result.getSession().getDefaultTimezone());
                    final Object val = pair.getKey();
                    final byte[] bytes = pair.getValue();
                    if (val instanceof Timestamp || val instanceof Date) {
                        builder.writeByteArray(bytes);
                    } else if (val instanceof String) {
                        builder.writeString((String) val);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                            "Mismatch of clazz " + clazz.getName() + " type " + meta.getType().name() + ".");
                    }
                } else if (clazz == Time.class) {
                    Pair<Object, byte[]> pair = XResultUtil.resultToObject(meta, byteString, true,
                        result.getSession().getDefaultTimezone());
                    final Object val = pair.getKey();
                    final byte[] bytes = pair.getValue();
                    if (val instanceof Time) {
                        builder.writeByteArray(bytes);
                    } else if (val instanceof String) {
                        builder.writeString((String) val);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                            "Mismatch of clazz " + clazz.getName() + " type " + meta.getType().name() + ".");
                    }
                } else if (clazz == byte[].class) {
                    final byte[] val =
                        XResultUtil.resultToObject(meta, byteString, true,
                            result.getSession().getDefaultTimezone())
                            .getValue();
                    builder.writeByteArray(val);
                } else if (clazz == java.sql.Blob.class) {
                    final byte[] bytes =
                        XResultUtil.resultToObject(meta, byteString, true,
                            result.getSession().getDefaultTimezone())
                            .getValue();
                    builder.writeBlob(new Blob(bytes));
                } else if (clazz == Enum.class) {
                    final Pair<Object, byte[]> pair =
                        XResultUtil.resultToObject(meta, byteString, true,
                            result.getSession().getDefaultTimezone());
                    if (pair.getKey() instanceof String) {
                        builder.writeString((String) pair.getKey());
                    } else {
                        builder.writeString(new String(pair.getValue()));
                    }
                } else {
                    throw new AssertionError("Data type " + clazz.getName() + " not supported");
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public long estimateSize() {
        if (row != null) {
            return row.stream().mapToLong(t -> t.size()).sum();
        } else {
            return 0;
        }
    }
}
