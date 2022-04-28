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

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_RESULT;

public class MergeSortWithBufferTableScanClient extends TableScanClient {

    public static final Logger log = LoggerFactory.getLogger(MergeSortWithBufferTableScanClient.class);

    private MemoryPool memoryPool;
    private MemoryAllocatorCtx allocator;
    private DataType[] dataTypeList;

    public MergeSortWithBufferTableScanClient(
        ExecutionContext context, CursorMeta meta, boolean useTransaction, int prefetchNum) {
        super(context, meta, useTransaction, prefetchNum);
        long limit = context.getParamManager().getLong(ConnectionParams.MERGE_SORT_BUFFER_SIZE);
        String memoryName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        this.memoryPool = context.getMemoryPool().getOrCreatePool(
            memoryName, limit, MemoryType.OPERATOR);
        this.allocator = memoryPool.getMemoryAllocatorCtx();
        final List<ColumnMeta> columns = meta.getColumns();
        this.dataTypeList = new DataType[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            this.dataTypeList[i] = columns.get(i).getDataType();
        }
    }

    @Override
    public void addSplitResultSet(SplitResultSet splitResultSet) {
        ((BufferSplitResultSet) splitResultSet).advanceCacheData();
        synchronized (this) {
            readyResultSet.add(splitResultSet);
            executePrefetchThread(false);
            if (readyResultSet.size() == getSplitNum()) {
                notifyBlockedCallers();
            }
        }
    }

    @Override
    public int connectionCount() {
        //对于merge-sort，使用滑动窗口
        return pushdownSplitIndex.get() - readyResultSet.size();
    }

    @Override
    public synchronized void close(SourceExec sourceExec) {
        if (memoryPool != null) {
            memoryPool.destroy();
            memoryPool = null;
        }
        super.close(sourceExec);
    }

    @Override
    public SplitResultSet newSplitResultSet(JdbcSplit jdbcSplit) {
        return new BufferSplitResultSet(jdbcSplit, Lists.newArrayList(dataTypeList));
    }

    public class BufferSplitResultSet extends SplitResultSet {
        private List<Chunk> bufferData = new LinkedList<>();
        private Row current = null;
        private ChunkBuilder chunkBuilder;
        private boolean notEnough;

        public BufferSplitResultSet(JdbcSplit jdbcSplit,
                                    List<DataType> dataTypes) {
            super(jdbcSplit);
            this.chunkBuilder = new ChunkBuilder(dataTypes, 1, context);
        }

        public void advanceCacheData() {
            if (isOnlyXResult()) {
                return; // Do not buffer when using X-Protocol.
            }
            long chunkSize = 0;
            try {
                while (super.next()) {
                    chunkBuilder.declarePosition();
                    ResultSetCursorExec.buildOneRow(super.current(), dataTypeList, chunkBuilder.getBlockBuilders());
                    Chunk chunk = chunkBuilder.build();
                    bufferData.add(chunk);
                    chunkSize = chunk.getSizeInBytes();
                    allocator.allocateReservedMemory(chunkSize);
                    chunkBuilder.reset();
                }
                super.closeConnection();
            } catch (MemoryNotEnoughException t) {
                notEnough = true;
                log.debug("Expect the not enough memory here", t);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }

        @Override
        protected Row current() {
            if (isOnlyXResult()) {
                throw new TddlRuntimeException(ERR_X_PROTOCOL_RESULT, "Should use chunk2chunk to fetch data.");
            }
            return current;
        }

        @Override
        protected boolean next() {
            if (isOnlyXResult()) {
                assert 0 == bufferData.size();
                if (!super.next()) {
                    super.closeConnection();
                    return false;
                }
                return true;
            }
            current = null;
            if (bufferData.size() > 0) {
                Chunk chunk = bufferData.remove(0);
                current = chunk.rowAt(0);
                if (notEnough && bufferData.size() == 0) {
                    //ignore release the memory of last row.
                } else {
                    allocator.releaseReservedMemory(chunk.getSizeInBytes(), false);
                }
                return true;
            } else {
                boolean ret = super.next();
                if (ret) {
                    current = super.current();
                } else {
                    super.closeConnection();
                }
                return ret;
            }
        }

        @Override
        public ResultSet getResultSet() {
            throw new UnsupportedOperationException();
        }
    }

}
