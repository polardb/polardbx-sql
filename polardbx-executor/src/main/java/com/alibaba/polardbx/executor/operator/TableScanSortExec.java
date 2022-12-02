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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;
import static com.alibaba.polardbx.executor.mpp.operator.WorkProcessor.mergeSorted;

public class TableScanSortExec extends TableScanExec {

    protected boolean mergeSort = false;

    protected long fetched;
    protected long skipped;

    protected Iterator<Optional<Row>> sortedRows;

    protected Comparator<Row> orderComparator;

    public TableScanSortExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                             long maxRowCount, long skipped, long fetched, SpillerFactory spillerFactory,
                             List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, maxRowCount, spillerFactory, dataTypeList);
        this.skipped = skipped;
        this.fetched = fetched;
    }

    @Override
    void doOpen() {
        if (!scanClient.noMoreSplit()) {
            throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "TableScanSortExec input split not ready");
        }

        if ((this.scanClient.getSplitNum() > 1) || (fetched < Long.MAX_VALUE || skipped > 0)) {
            mergeSort = true;
        }

        if (this.scanClient.getSplitNum() == 0) {
            mergeSort = false;
        }

        if (fetched > 0) {
            super.doOpen();
        }
    }

    @Override
    public void addSplit(Split split) {
        getJdbcByDeletegate(split);
        if (fetched > 0) {
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            jdbcSplit.setLimit(skipped + fetched);
        }
        super.addSplit(split);
    }

    @Override
    protected void appendRow(TableScanClient.SplitResultSet consumeResultSet) throws SQLException {
        ResultSetCursorExec.buildOneRow(consumeResultSet.current(), dataTypes, blockBuilders, context);
    }

    @Override
    protected Chunk fetchChunk() {
        if (fetched <= 0) {
            isFinish = true;
        }

        if (isFinish) {
            //stop early, so close the connection in time.
            scanClient.cancelAllThreads(false);
            return null;
        }

        if (mergeSort) {
            if (!scanClient.noMorePrefetchSplit()) {
                return null;
            }

            try {
                if (sortedRows == null) {
                    if (orderComparator == null) {
                        Collection<RelCollation> collations = logicalView.getCollations();
                        if (collations == null || collations.isEmpty()) {
                            GeneralUtil.nestedException("logicalview should provided one sort collation at least");
                        }
                        RelCollation collation = collations.iterator().next();
                        if (log.isDebugEnabled()) {
                            log.debug(logicalView.getRelatedId() + " table scan need sort by:" + collation);
                        }
                        List<RelFieldCollation> sortList = collation.getFieldCollations();
                        List<OrderByOption> orderBys = ExecUtils.convertFrom(sortList);
                        this.orderComparator = ExecUtils.getComparator(orderBys,
                            dataTypeList
                        );
                    }

                    List<WorkProcessor<Row>> sortedStreams = buildSortedStream();
                    this.sortedRows = mergeSorted(sortedStreams, orderComparator).yieldingIterator();
                }
                buildSortedChunk();
            } catch (Throwable ex) {
                TddlRuntimeException exception =
                    new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex, ex.getMessage());
                scanClient.setException(exception);
                if (isFinish) {
                    log.debug(context.getTraceId() + " here occur error, but current scan is closed!", ex);
                    return null;
                } else {
                    scanClient.throwIfFailed();
                }
            }
            if (blockBuilders[0].getPositionCount() == 0) {
                isFinish = true;
                return null;
            } else {
                Chunk ret = buildChunkAndReset();
                return ret;
            }
        } else {
            return super.fetchChunk();
        }
    }

    protected List<WorkProcessor<Row>> buildSortedStream() {
        List<WorkProcessor<Row>> sortedStreams = new ArrayList<>();
        TableScanClient.SplitResultSet splitResultSet = null;
        while ((splitResultSet = scanClient.popResultSet()) != null) {
            sortedStreams.add(WorkProcessor.fromIterator(new ResultIterator(splitResultSet)));
        }
        return sortedStreams;
    }

    protected void buildSortedChunk() throws SQLException {
        while (true) {
            if (!sortedRows.hasNext()) {
                isFinish = true;
                break;
            }
            Optional<Row> row = this.sortedRows.next();
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

    @Override
    public boolean produceIsFinished() {
        if (log.isDebugEnabled()) {
            log.debug(this.hashCode() + ":produceIsFinished=" + isFinish);
        }
        scanClient.throwIfFailed();
        return scanClient.isClosed || isFinish;
    }

    private class ResultIterator implements Iterator<Row> {
        TableScanClient.SplitResultSet input;
        private final ChunkBuilder chunkBuilder;
        private Chunk chunkRow = null;

        public ResultIterator(TableScanClient.SplitResultSet input) {
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
