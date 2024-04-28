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

package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.UInt64Utils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.SetType;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import org.apache.orc.impl.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.datatype.DecimalConverter.getUnscaledDecimal;
import static com.alibaba.polardbx.executor.archive.columns.ColumnProvider.longFromByte;

/**
 * @author yaozhili
 */
public class RawOrcTypeCsvReader implements CSVFileReader {

    private int fieldNum;
    private InputStream inputStream;
    private List<ColumnMeta> columnMetas;
    private ByteCSVReader rowReader;
    private ExecutionContext context;
    private int chunkLimit;
    private int offset;
    public static Charset DEFAULT_CHARSET = CharsetName.defaultCharset().toJavaCharset();

    @Override
    public void open(ExecutionContext context, List<ColumnMeta> columnMetas, int chunkLimit, Engine engine,
                     String csvFileName, int offset, int length) throws IOException {
        this.chunkLimit = chunkLimit;
        this.context = context;
        this.fieldNum = columnMetas.size();
        // synchronous reading
        byte[] buffer;
        if (offset == 0 && length == EOF) {
            buffer = FileSystemUtils.readFullyFile(csvFileName, engine, true);
        } else {
            buffer = new byte[length];
            FileSystemUtils.readFile(csvFileName, offset, length, buffer, engine, true);
        }

        this.inputStream = new ByteArrayInputStream(buffer);
        this.columnMetas = columnMetas;
        this.rowReader = new ByteCSVReader(csvFileName, inputStream);
        this.offset = offset;
    }

    @Override
    public Chunk next() {
        return nextUntilPosition(Long.MAX_VALUE);
    }

    @Override
    public Chunk nextUntilPosition(long pos) {
        List<BlockBuilder> blockBuilders = this.columnMetas
            .stream()
            .map(ColumnMeta::getDataType)
            .map(t -> BlockBuilders.create(t, context))
            .collect(Collectors.toList());

        int totalRow = 0;
        while (offset + rowReader.position() < pos && rowReader.isReadable()) {
            try {
                CSVRow row = rowReader.nextRow();

                // for each row, parse each column and append onto block-builder
                for (int columnId = 0; columnId < fieldNum; columnId++) {
                    BlockBuilder blockBuilder = blockBuilders.get(columnId);
                    DataType dataType = columnMetas.get(columnId).getDataType();

                    convertFromCsvToOrc(blockBuilder, row, columnId, dataType);
                }

                // reach chunk limit
                if (++totalRow >= chunkLimit) {
                    return buildChunk(blockBuilders, totalRow);
                }

            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        // flush the remaining rows
        return totalRow == 0 ? null : buildChunk(blockBuilders, totalRow);
    }

    private void convertFromCsvToOrc(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
            return;
        }

        byte[] bytes = row.getBytes(columnId);
        switch (dataType.fieldType()) {
        // we can hold data using long value from bit(1) to bit(64)
        case MYSQL_TYPE_BIT: {
            //大端模式
            blockBuilder.writeLong(ColumnProvider.bigBitLongFromByte(bytes, bytes.length));
            return;
        }

        // for tiny int, small int, medium int, int
        case MYSQL_TYPE_TINY: {
            long longVal;
            boolean isUnsigned = dataType.isUnsigned();
            if (isUnsigned) {
                longVal = getUint8(bytes, 0);
            } else {
                longVal = getInt8(bytes, 0);
            }
            blockBuilder.writeLong(longVal);
            return;
        }
        case MYSQL_TYPE_SHORT: {
            long longVal;
            boolean isUnsigned = dataType.isUnsigned();
            if (isUnsigned) {
                longVal = getUint16(bytes, 0);
            } else {
                longVal = getInt16(bytes, 0);
            }
            blockBuilder.writeLong(longVal);
            return;
        }
        case MYSQL_TYPE_INT24: {
            long longVal;
            boolean isUnsigned = dataType.isUnsigned();
            if (isUnsigned) {
                longVal = getUint24(bytes, 0);
            } else {
                longVal = getInt24(bytes, 0);
            }
            blockBuilder.writeLong(longVal);
            return;
        }
        case MYSQL_TYPE_LONG: {
            long longVal;
            boolean isUnsigned = dataType.isUnsigned();
            if (isUnsigned) {
                longVal = getUint32(bytes, 0);
            } else {
                longVal = getInt32(bytes, 0);
            }
            blockBuilder.writeLong(longVal);
            return;
        }

        // for bigint, bigint unsigned.
        case MYSQL_TYPE_LONGLONG: {
            long longVal;
            int length = Math.min(8, bytes.length);
            boolean isUnsigned = dataType.isUnsigned();
            if (isUnsigned) {
                longVal = longFromByte(bytes, length) ^ UInt64Utils.FLIP_MASK;
            } else {
                longVal = longFromByte(bytes, length);
            }
            blockBuilder.writeLong(longVal);
            return;
        }

        // for real type.
        case MYSQL_TYPE_FLOAT: {
            int result = ColumnProvider.intFromByte(bytes, bytes.length);
            blockBuilder.writeDouble(Float.intBitsToFloat(result));
            return;
        }
        case MYSQL_TYPE_DOUBLE: {
            long result = ColumnProvider.longFromByte(bytes, bytes.length);
            blockBuilder.writeDouble(Double.longBitsToDouble(result));
            return;
        }

        // for date type
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE: {
            blockBuilder.writeLong(ColumnProvider.convertDateToLong(bytes));
            return;
        }

        // for datetime type
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2: {
            final int scale = dataType.getScale();
            blockBuilder.writeLong(ColumnProvider.convertDateTimeToLong(bytes, scale));
            return;
        }

        // for time type.
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2: {
            final int scale = dataType.getScale();
            blockBuilder.writeLong(ColumnProvider.convertTimeToLong(bytes, scale));
            return;
        }

        // for timestamp type.
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2: {
            final int scale = dataType.getScale();
            long second = 0;
            for (int i = 0; i < 4; i++) {
                byte b = bytes[i];
                second = (second << 8) | (b >= 0 ? (int) b : (b + 256));
            }

            // parse fsp
            int micro = 0;
            int length = (scale + 1) / 2;
            if (length > 0) {
                int fraction = 0;
                for (int i = 4; i < (4 + length); i++) {
                    byte b = bytes[i];
                    fraction = (fraction << 8) | (b >= 0 ? (int) b : (b + 256));
                }
                micro = fraction * (int) Math.pow(100, 3 - length);
            }

            // pack time value to long
            MySQLTimeVal timeVal = new MySQLTimeVal(second, micro * 1000L);
            blockBuilder.writeLong(XResultUtil.timeValToLong(timeVal));
            return;
        }

        // for year type.
        case MYSQL_TYPE_YEAR: {
            long longVal = ColumnProvider.longFromByte(bytes, bytes.length);
            blockBuilder.writeLong(longVal == 0 ? 0 : longVal + 1900);
            return;
        }

        // for decimal type.
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            if (TypeUtils.isDecimal64Precision(dataType.getPrecision())) {
                int precision = dataType.getPrecision();
                int scale = dataType.getScale();
                blockBuilder.writeLong(getUnscaledDecimal(bytes, precision, scale));
            } else {
                // fall back to byte[] representation
                blockBuilder.writeByteArray(bytes);
            }
            return;
        }

        case MYSQL_TYPE_ENUM: {
            int val = ColumnProvider.intFromByte(bytes, bytes.length);
            EnumType enumType = (EnumType) dataType;
            blockBuilder.writeByteArray(enumType.convertTo(val).getBytes());
            return;
        }

        case MYSQL_TYPE_JSON: {
            String charsetName = dataType.getCharsetName().getJavaCharset();
            String string = ColumnProvider.convertToString(bytes, charsetName);
            blockBuilder.writeByteArray(string.getBytes());
            return;
        }

        case MYSQL_TYPE_VAR_STRING: {
            if (dataType instanceof BinaryType) {
                BinaryType binaryType = (BinaryType) dataType;
                if (binaryType.isFixedLength()) {
                    byte[] paddingBytes = ColumnProvider.convertToPaddingBytes(bytes, binaryType);
                    blockBuilder.writeByteArray(paddingBytes);
                } else {
                    blockBuilder.writeByteArray(bytes);
                }
            } else {
                blockBuilder.writeByteArray(convertFromBinary(dataType.getCharsetName(), bytes));
            }
            return;
        }
        case MYSQL_TYPE_SET: {
            int val = ColumnProvider.intFromByte(bytes, bytes.length);
            SetType setType = (SetType) dataType;
            blockBuilder.writeByteArray(String.join(",", setType.convertFromBinary(val)).getBytes());
            return;
        }
        case MYSQL_TYPE_STRING:
            if (dataType instanceof SetType) {
                int val = ColumnProvider.intFromByte(bytes, bytes.length);
                SetType setType = (SetType) dataType;
                blockBuilder.writeByteArray(String.join(",", setType.convertFromBinary(val)).getBytes());
            } else {
                blockBuilder.writeByteArray(convertFromBinary(dataType.getCharsetName(), bytes));
            }
            return;
        default:
            blockBuilder.writeByteArray(bytes);
        }
    }

    @NotNull
    private Chunk buildChunk(List<BlockBuilder> blockBuilders, int totalRow) {
        return new Chunk(totalRow, blockBuilders.stream()
            .map(BlockBuilder::build).toArray(Block[]::new));
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    public static int getInt8(byte[] buf, int pos) {
        return buf[pos];
    }

    public static int getUint8(byte[] buf, int pos) {
        return 0xff & buf[pos];
    }

    public static int getInt16(byte[] buf, int pos) {
        return (0xff & buf[pos]) | ((buf[pos + 1]) << 8);
    }

    public static int getUint16(byte[] buf, int pos) {
        return (0xff & buf[pos]) | ((0xff & buf[pos + 1]) << 8);
    }

    public static int getInt24(byte[] buf, int pos) {
        return (0xff & buf[pos]) | ((0xff & buf[pos + 1]) << 8) | ((buf[pos + 2]) << 16);
    }

    public static int getUint24(byte[] buf, int pos) {
        return (0xff & buf[pos]) | ((0xff & buf[pos + 1]) << 8) | ((0xff & buf[pos + 2]) << 16);
    }

    public static int getInt32(byte[] buf, int pos) {
        return (0xff & buf[pos]) | ((0xff & buf[pos + 1]) << 8) | ((0xff & buf[pos + 2]) << 16)
            | ((buf[pos + 3]) << 24);
    }

    public static long getUint32(byte[] buf, int pos) {
        return ((long) (0xff & buf[pos])) | ((long) (0xff & buf[pos + 1]) << 8) | ((long) (0xff & buf[pos + 2]) << 16)
            | ((long) (0xff & buf[pos + 3]) << 24);
    }

    public static byte[] convertFromBinary(CharsetName charsetName, byte[] bytes) {
        if (charsetName == CharsetName.UTF8MB4 || charsetName == CharsetName.UTF8) {
            return bytes;
        }
        return new String(bytes, charsetName.toJavaCharset()).getBytes(DEFAULT_CHARSET);
    }
}
