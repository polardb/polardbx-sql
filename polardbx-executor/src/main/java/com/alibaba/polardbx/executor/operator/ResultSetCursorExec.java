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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.rpc.compatible.XResultSet;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IXRowChunk;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.row.ResultSetRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Convert Cursor to Executor
 *
 */
public class ResultSetCursorExec extends AbstractExecutor {

    private final Cursor cursor;

    private DataType[] dataTypes;

    private boolean[] isUtf8Encoding;

    private List<DataType> dataTypeList;

    private boolean isFinish;

    public ResultSetCursorExec(Cursor cursor, ExecutionContext context, long maxRowCount) {
        super(context);
        this.cursor = cursor;
        this.chunkLimit = (int) Math.max(Math.min(maxRowCount, chunkLimit), 1);
        this.dataTypeList = cursor.getReturnColumns().stream().map(t -> t.getDataType()).collect(Collectors.toList());
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        final int numColumns = dataTypeList.size();
        dataTypes = new DataType[numColumns];
        isUtf8Encoding = new boolean[numColumns];
        for (int i = 0; i < dataTypeList.size(); i++) {
            dataTypes[i] = dataTypeList.get(i);
            isUtf8Encoding[i] = Optional.ofNullable(dataTypes[i])
                .filter(SliceType.class::isInstance)
                .map(SliceType.class::cast)
                .map(SliceType::getCharsetName)
                .map(c -> c == CharsetName.UTF8 || c == CharsetName.UTF8MB4)
                .orElse(true);
        }
    }

    @Override
    Chunk doNextChunk() {
        Row row;
        int count = 0;

        GeneralUtil.checkInterrupted();

        try {
            while (count < chunkLimit && (row = cursor.next()) != null) {
                if (row instanceof ResultSetRow) {
                    ResultSet rs = ((ResultSetRow) row).getResultSet();
                    buildOneRow(rs, dataTypes, blockBuilders);
                } else if (row instanceof IXRowChunk) {
                    // XResult and deal with new interface.
                    ((IXRowChunk) row).buildChunkRow(dataTypes, blockBuilders);
                } else {
                    ResultSetCursorExec.buildOneRow(row, dataTypes, blockBuilders);
                }
                count++;
            }
        } catch (SQLException ex) {
            throw GeneralUtil.nestedException(ex);
        }

        if (count == 0) {
            isFinish = true;
            return null;
        } else {
            Chunk ret = buildChunkAndReset();
            return ret;
        }
    }

    public static void buildOneRow(ResultSet rs, DataType[] dataTypes, BlockBuilder[] blockBuilders)
        throws SQLException {
        if (rs.isWrapperFor(XResultSet.class)) {
            XResult xResult = rs.unwrap(XResultSet.class).getXResult();
            XRowSet.buildChunkRow(xResult, xResult.getMetaData(), xResult.current().getRow(), dataTypes, blockBuilders);
        } else {
            for (int i = 0; i < dataTypes.length; i++) {
                buildOneCell(rs, i, dataTypes[i], blockBuilders[i]);
            }
        }
    }

    public static void buildOneRow(Row row, DataType[] dataTypes, BlockBuilder[] blockBuilders) throws SQLException {
        if (row instanceof ResultSetRow) {
            buildOneRow(((ResultSetRow) row).getResultSet(), dataTypes, blockBuilders);
        } else {
            for (int i = 0; i < row.getColNum(); i++) {
                blockBuilders[i].writeObject(dataTypes[i].convertFrom(row.getObject(i)));
            }
        }
    }

    private static void buildOneCell(ResultSet rs, int i, DataType type, BlockBuilder builder) throws SQLException {
        final Class clazz = type.getDataClass();
        if (clazz == Integer.class) {
            if (DataTypeUtil.equalsSemantically(type,
                DataTypes.BitType)) { /* fix when BitType is null, rs.getInt throw Exception bug */
                Object val = rs.getObject(i + 1);
                if (val != null) {
                    builder.writeInt(rs.getInt(i + 1));
                } else {
                    builder.appendNull();
                }
            } else {
                int val = rs.getInt(i + 1);
                if (val != 0 || !rs.wasNull()) {
                    builder.writeInt(val);
                } else {
                    builder.appendNull();
                }
            }
        } else if (clazz == Long.class) {
            long val = rs.getLong(i + 1);
            if (val != 0 || !rs.wasNull()) {
                builder.writeLong(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Short.class) {
            short val = rs.getShort(i + 1);
            if (val != 0 || !rs.wasNull()) {
                builder.writeShort(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Byte.class) {
            byte val = rs.getByte(i + 1);
            if (val != 0 || !rs.wasNull()) {
                builder.writeByte(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Float.class) {
            float val = rs.getFloat(i + 1);
            if (val != 0 || !rs.wasNull()) {
                builder.writeFloat(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Double.class) {
            double val = rs.getDouble(i + 1);
            if (val != 0 || !rs.wasNull()) {
                builder.writeDouble(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Slice.class) {
            if (type.isUtf8Encoding()) {
                byte[] rawBytes = rs.getBytes(i + 1);
                if (rawBytes != null) {
                    builder.writeByteArray(rawBytes);
                } else {
                    builder.appendNull();
                }
            } else if (type.isLatin1Encoding()) {
                byte[] rawBytes = rs.getBytes(i + 1);
                if (rawBytes != null) {
                    ((SliceBlockBuilder) builder).writeBytesInLatin1(rawBytes);
                } else {
                    builder.appendNull();
                }
            } else {
                String val = rs.getString(i + 1);
                if (val != null) {
                    builder.writeString(val);
                } else {
                    builder.appendNull();
                }
            }
        } else if (clazz == String.class) {
            String val = rs.getString(i + 1);
            if (val != null) {
                builder.writeString(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Enum.class) {
            String val = rs.getString(i + 1);
            if (val != null) {
                builder.writeString(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == BigInteger.class || clazz == UInt64.class) {
            Object val = rs.getObject(i + 1);
            if (val instanceof BigInteger) {
                builder.writeBigInteger((BigInteger) val);
            } else if (val instanceof BigDecimal) {
                assert type instanceof ULongType;
                builder.writeBigInteger(((BigDecimal) val).toBigInteger());
            } else if (val instanceof Number) {
                assert type instanceof ULongType;
                builder.writeBigInteger(BigInteger.valueOf(((Number) val).longValue()));
            } else if (val instanceof byte[]) {
                //Note: this is a workaround for bit_8 type, the result will convert from
                //byte to integer
                byte[] bytes = (byte[]) val;
                builder.writeBigInteger(BigInteger.valueOf(bytesToLong(bytes)));
            } else { // null or error type
                builder.writeObject(val);
            }
        } else if (clazz == Decimal.class) {
            // Store the text representation in bytes
            byte[] val = rs.getBytes(i + 1);
            if (val != null && val.length != 0) {
                builder.writeByteArray(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Timestamp.class) {
            // directly write raw bytes
            byte[] rawBytes = rs.getBytes(i + 1);
            builder.writeByteArray(rawBytes);

        } else if (clazz == Date.class) {
            // directly write raw bytes
            byte[] rawBytes = rs.getBytes(i + 1);
            builder.writeByteArray(rawBytes);

        } else if (clazz == Time.class) {
            // directly write raw bytes
            byte[] rawBytes = rs.getBytes(i + 1);
            builder.writeByteArray(rawBytes);

        } else if (clazz == byte[].class) {
            byte[] val = rs.getBytes(i + 1);
            if (val != null) {
                builder.writeByteArray(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Blob.class) {
            Blob val = rs.getBlob(i + 1);
            if (val != null) {
                builder.writeBlob(val);
            } else {
                builder.appendNull();
            }
        } else if (clazz == Clob.class) {
            Clob val = rs.getClob(i + 1);
            if (val != null) {
                builder.writeClob(val);
            } else {
                builder.appendNull();
            }
        } else {
            throw new AssertionError("Data type " + clazz.getName() + " not supported");
        }
    }

    private static long bytesToLong(byte[] bytes) {
        assert bytes.length <= 8;
        long val = 0;
        for (int i = 0; i < bytes.length; i++) {
            val |= (bytes[i] & 0xFF) << (i * 8);
        }
        return val;
    }

    @Override
    void doClose() {
        dataTypes = null;
        List<Throwable> exceptions = new ArrayList<>();
        cursor.close(exceptions);
        if (!exceptions.isEmpty()) {
            throw GeneralUtil.nestedException(exceptions.get(0));
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypeList;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    protected void beforeOpen() {
        if (enableCpuProfile && targetPlanStatGroup != null) {
            startTimeCostNano = System.nanoTime();
        }
        super.beforeOpen();
    }

    @Override
    protected void afterOpen() {
        if (enableCpuProfile && targetPlanStatGroup != null) {
            targetPlanStatGroup.processLvTimeCost.addAndGet(System.nanoTime() - startTimeCostNano);
            startTimeCostNano = 0;
        }
        super.afterOpen();
    }

    @Override
    protected void beforeProcess() {
        if (enableCpuProfile && targetPlanStatGroup != null) {
            startTimeCostNano = System.nanoTime();
        }
        super.beforeProcess();
    }

    @Override
    protected void afterProcess(Chunk result) {
        if (enableCpuProfile && targetPlanStatGroup != null) {
            targetPlanStatGroup.processLvTimeCost.addAndGet(System.nanoTime() - startTimeCostNano);
            startTimeCostNano = 0;
        }
        super.afterProcess(result);
    }

    @Override
    protected void beforeClose() {
        if (statistics != null && enableCpuProfile) {
            startTime = ThreadCpuStatUtil.getThreadCpuTimeNano();
            if (targetPlanStatGroup != null) {
                // This process must be done before cursor.close();
                RuntimeStatHelper.processResultSetStatForLvInChunk(this.targetPlanStatGroup, this.cursor);
            }
        }
    }

    @Override
    protected void afterClose() {
        if (statistics != null && enableCpuProfile) {
            assert startTime > 0;
            statistics.addCloseDuration(ThreadCpuStatUtil.getThreadCpuTimeNano() - startTime);
            startTime = 0;
        }
    }

    @Override
    public boolean produceIsFinished() {
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}
