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

package com.alibaba.polardbx.server.conn;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.operator.CacheCursor;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;

import java.sql.ResultSet;
import java.util.List;

/**
 * @author yaozhili
 */
public class ResultSetCachedObj {
    private final Logger logger;
    private final ResultSet resultSet;
    private final CacheResultCursor cacheCursor;
    private boolean firstRow;
    private boolean lastRow;
    private long sqlSelectLimit;

    public ResultSetCachedObj(ResultSet resultSet, String traceId, MemoryPool memoryPool, Logger logger,
                              long sqlSelectLimit) {
        TResultSet tResultSet = (TResultSet) resultSet;
        ResultCursor resultCursor = tResultSet.getResultCursor();
        this.cacheCursor = new CacheResultCursor(resultCursor, traceId, sqlSelectLimit, memoryPool);
        this.resultSet = new TResultSet(cacheCursor, tResultSet.getExtraCmd());
        this.logger = logger;
        this.sqlSelectLimit = sqlSelectLimit;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public boolean isFirstRow() {
        return firstRow;
    }

    public boolean isLastRow() {
        return lastRow;
    }

    public void setFirstRow(boolean firstRow) {
        this.firstRow = firstRow;
    }

    public void setLastRow(boolean lastRow) {
        this.lastRow = lastRow;
    }

    public void close() {
        // Close the result set.
        if (null != resultSet) {
            try {
                resultSet.close();
            } catch (Throwable t) {
                logger.warn(t.getMessage());
            }
        }
    }

    public long getRowCount() {
        // Should be called after first doNext()
        return cacheCursor.getCacheCursor().getRowCount();
    }

    private static class CacheResultCursor extends ResultCursor {
        private final CacheCursor cacheCursor;
        private final CursorMeta cursorMeta;
        private final List<ColumnMeta> returnColumns;
        private long sqlSelectLimit;

        public CacheResultCursor(ResultCursor cursor, String traceId, long sqlSelectLimit, MemoryPool parentPool) {
            super();
            this.cursorMeta = cursor.getCursorMeta();
            this.returnColumns = cursor.getReturnColumns();
            this.sqlSelectLimit = sqlSelectLimit;

            MemoryPool memoryPool = parentPool.getOrCreatePool(traceId, MemoryType.CURSOR_FETCH);
            long estimateRowSize = returnColumns.stream().mapToLong(MemoryEstimator::estimateColumnSize).sum();
            this.cacheCursor =
                new CacheCursor(ServiceProvider.getInstance().getServer().getSpillerFactory(), cursor, memoryPool,
                    estimateRowSize, new QuerySpillSpaceMonitor(traceId));
        }

        @Override
        public Row doNext() {
            // CacheCursor will cache all results on first next, which will be called in resultSetToHeaderPacket
            if (sqlSelectLimit == 0) {
                return null;
            }
            sqlSelectLimit--;

            Row row = cacheCursor.next();
            if (row != null && cursorMeta != null) {
                row.setCursorMeta(cursorMeta);
            }
            return row;
        }

        @Override
        public List<Throwable> doClose(List<Throwable> exceptions) {
            return cacheCursor.close(exceptions);
        }

        @Override
        public List<ColumnMeta> getReturnColumns() {
            return returnColumns;
        }

        @Override
        public CursorMeta getCursorMeta() {
            return cursorMeta;
        }

        public CacheCursor getCacheCursor() {
            return cacheCursor;
        }
    }
}
