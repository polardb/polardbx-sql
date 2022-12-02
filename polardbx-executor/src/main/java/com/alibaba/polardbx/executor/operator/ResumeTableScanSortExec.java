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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.ResumeTableScanExec.StreamJdbcSplit;
import com.alibaba.polardbx.executor.operator.TableScanClient.SplitResultSet;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ResumeTableScanSortExec extends TableScanSortExec implements ResumeExec {

    private long stepSize;

    private volatile boolean suspendSortedRow;

    private CacheSortedRow remainData;

    private ChunkBuilder rowBuilder;

    private MemoryPool memoryPool;
    private MemoryAllocatorCtx allocator;

    public ResumeTableScanSortExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                                   long maxRowCount, long skipped, long fetched, SpillerFactory spillerFactory,
                                   long stepSize, List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, maxRowCount, skipped, fetched, spillerFactory, dataTypeList);
        this.stepSize = stepSize;

    }

    @Override
    void doOpen() {
        super.doOpen();
        this.rowBuilder = new ChunkBuilder(
            Arrays.stream(dataTypes).collect(Collectors.toList()), 1, context);
        this.memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        this.allocator = memoryPool.getMemoryAllocatorCtx();
        this.remainData = new CacheSortedRow(allocator, dataTypes, context);
    }

    @Override
    public void addSplit(Split split) {
        getJdbcByDeletegate(split);
        if (log.isDebugEnabled()) {
            log.debug(context.getTraceId() + ":lv=" + this.logicalView.getRelatedId() + " addSplit:" + split);
        }
        JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
        StreamJdbcSplit
            dynamicSplit = new StreamJdbcSplit(
            jdbcSplit, stepSize);
        scanClient.addSplit(split.copyWithSplit(dynamicSplit));
    }

    @Override
    public synchronized boolean resume() {
        Preconditions.checkState(isFinish, logicalView.getRelatedId() + " not finish previous stage");
        if (consumeResultSet != null) {
            consumeResultSet.close();
            consumeResultSet = null;
        }

        suspendSortedRow = false;

        if (!mergeSort) {
            //只有一个split且不包含sort的话，不走归并排序逻辑，所以需要额外判断split是否消费完成
            List<TableScanClient.PrefetchThread> lastFinishedThreads = scanClient.prefetchThreads;
            for (TableScanClient.PrefetchThread prefetchThread : lastFinishedThreads) {
                if (prefetchThread.getResultSet().count < ((StreamJdbcSplit) prefetchThread.split).expectCount()) {
                    ((StreamJdbcSplit) prefetchThread.split).setFinish(true);
                }
            }
        }

        scanClient.reset();
        sortedRows = null;
        if (fetched > 0) {
            Iterator<Split> it = scanClient.getSplitList().iterator();
            while (it.hasNext()) {
                StreamJdbcSplit x = (StreamJdbcSplit) (it.next()).getConnectorSplit();
                if (x.isFinish()) {
                    //split is useless
                    it.remove();
                } else {
                    x.increaseFetchNth();
                }
            }
        } else {
            //really finish!
            scanClient.getSplitList().clear();
        }
        if (scanClient.getSplitList().isEmpty()) {
            isFinish = true;
            return false;
        } else {
            this.isFinish = false;
            try {
                scanClient.executePrefetchThread(false);
            } catch (Throwable e) {
                TddlRuntimeException exception =
                    new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, e.getMessage());
                this.isFinish = true;
                scanClient.getSplitList().clear();
                scanClient.setException(exception);
                scanClient.throwIfFailed();
            }
            return true;
        }
    }

    @Override
    public boolean shouldSuspend() {
        return isFinish;
    }

    @Override
    public void doSuspend() {
        if (consumeResultSet != null) {
            consumeResultSet.close();
            consumeResultSet = null;
        }
        scanClient.cancelAllThreads(false);
    }

    @Override
    protected List<WorkProcessor<Row>> buildSortedStream() {
        List<WorkProcessor<Row>> sortedStreams = new ArrayList<>();
        TableScanClient.SplitResultSet splitResultSet = null;
        while ((splitResultSet = scanClient.popResultSet()) != null) {
            sortedStreams.add(WorkProcessor.fromIterator(new ResultIterator(splitResultSet)));
        }
        if (!remainData.chunks.isEmpty()) {
            sortedStreams.add(WorkProcessor.fromIterator(remainData.getSortedRows()));
            this.remainData = new CacheSortedRow(allocator, dataTypes, context);
        }
        return sortedStreams;
    }

    @Override
    protected void buildSortedChunk() throws SQLException {
        while (true) {
            if (!sortedRows.hasNext()) {
                isFinish = true;
                break;
            }
            Optional<Row> row = this.sortedRows.next();
            if (suspendSortedRow) {
                remainData.addRow(row.get());
                break;
            } else {
                if (skipped > 0) {
                    skipped--;
                } else {
                    if (row.isPresent() && fetched > 0) {
                        ResultSetCursorExec.buildOneRow(row.get(), dataTypes, blockBuilders, context);
                        fetched--;
                    } else {
                        isFinish = true;
                        fetched--;
                        break;
                    }
                    if (blockBuilders[0].getPositionCount() >= chunkLimit) {
                        break;
                    }
                }
            }
        }

        if (suspendSortedRow && !isFinish) {
            while (true) {
                if (!sortedRows.hasNext()) {
                    isFinish = true;
                    break;
                }
                remainData.addRow(sortedRows.next().get());
            }
        }
    }

    @Override
    synchronized void doClose() {
        if (memoryPool != null) {
            memoryPool.destroy();
            memoryPool = null;
        }
        super.doClose();
        this.remainData = null;
    }

    public static class CacheSortedRow {

        List<Chunk> chunks;

        private long chunkSize = -1;

        ChunkBuilder rowBuilder;

        MemoryAllocatorCtx allocator;

        private DataType[] dataTypes;

        private ExecutionContext context;

        public CacheSortedRow(
            MemoryAllocatorCtx allocator, DataType[] dataTypes, ExecutionContext context) {
            this.chunks = new ArrayList<>();
            this.allocator = allocator;
            this.dataTypes = dataTypes;
            this.rowBuilder = new ChunkBuilder(
                Arrays.stream(dataTypes).collect(Collectors.toList()), 1, context);
            this.context = context;
        }

        public void addRow(Row row) throws SQLException {
            rowBuilder.declarePosition();
            ResultSetCursorExec.buildOneRow(
                row, dataTypes, rowBuilder.getBlockBuilders(), context);
            Chunk chunk = rowBuilder.build();
            if (chunkSize == -1) {
                chunkSize = chunk.getSizeInBytes();
            }
            allocator.allocateReservedMemory(chunkSize);
            chunks.add(chunk);
            rowBuilder.reset();
        }

        public Iterator<Row> getSortedRows() {
            return new AbstractIterator<Row>() {
                private Iterator<Chunk> chunkIterator = chunks.iterator();

                @Override
                public Row computeNext() {
                    if (chunkIterator.hasNext()) {
                        Chunk chunk = chunkIterator.next();
                        allocator.releaseReservedMemory(chunkSize, false);
                        chunkIterator.remove();
                        return chunk.rowAt(0);
                    } else {
                        return endOfData();
                    }
                }
            };
        }

        @VisibleForTesting
        public List<Chunk> getChunks() {
            return chunks;
        }
    }

    private class ResultIterator implements Iterator<Row> {
        TableScanClient.SplitResultSet input;
        private final ChunkBuilder chunkBuilder;
        private Chunk chunkRow = null;

        public ResultIterator(SplitResultSet input) {
            this.input = input;
            this.chunkBuilder =
                input.isPureAsyncMode() ? new ChunkBuilder(Lists.newArrayList(dataTypes), 1, context) : null;
        }

        @Override
        public boolean hasNext() {
            if (input.next()) {
                if (input.isOnlyXResult()) {
                    try {
                        chunkBuilder.declarePosition();
                        input.fillChunk(dataTypes, chunkBuilder.getBlockBuilders(), 1);
                        chunkRow = chunkBuilder.build();
                        chunkBuilder.reset();
                    } catch (Exception e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, e);
                    }
                }
                return true;
            } else {
                if (input.count >= ((StreamJdbcSplit) input.jdbcSplit).expectCount()) {
                    if (scanClient.getSplitList().size() > 1) {
                        suspendSortedRow = true;
                    }
                } else {
                    ((StreamJdbcSplit) input.jdbcSplit).setFinish(true);
                }
                //close connection early
                input.closeConnection();
                return false;
            }
        }

        @Override
        public Row next() {
            if (input.isOnlyXResult()) {
                return chunkRow.rowAt(0);
            }
            return input.current();
        }
    }
}

