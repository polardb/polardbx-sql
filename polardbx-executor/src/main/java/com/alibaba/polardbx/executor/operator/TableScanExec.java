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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.statistics.RuntimeStatistics;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;

public class TableScanExec extends SourceExec implements Closeable {

    protected static final Logger log = LoggerFactory.getLogger(TableScanExec.class);

    protected final LogicalView logicalView;
    protected DataType[] dataTypes;
    protected List<DataType> dataTypeList;

    protected AtomicLong fetchTimeCost = new AtomicLong();

    //reset
    protected TableScanClient scanClient;
    protected TableScanClient.SplitResultSet consumeResultSet;
    protected volatile boolean isFinish = false;
    private final SpillerFactory spillerFactory;
    private final boolean useParameterDelegate;

    public TableScanExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                         long maxRowCount, SpillerFactory spillerFactory, List<DataType> dataTypeList) {
        super(context);
        this.chunkLimit = (int) Math.max(Math.min(maxRowCount, chunkLimit), 1);
        this.logicalView = logicalView;
        this.scanClient = scanClient;
        this.scanClient.registerSouceExec(this);
        this.spillerFactory = spillerFactory;
        this.dataTypeList = dataTypeList;
        this.useParameterDelegate = ExecUtils.useParameterDelegate(context);

    }

    @Override
    public void addSplit(Split split) {
        getJdbcByDeletegate(split);
        if (log.isDebugEnabled()) {
            log.debug(context.getTraceId() + ":lv=" + this.logicalView.getRelatedId() + " addSplit:" + split);
        }
        scanClient.addSplit(split);
    }

    protected void getJdbcByDeletegate(Split split) {
        if (useParameterDelegate) {
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            List<List<ParameterContext>> params = jdbcSplit.getParams();
            for (List<ParameterContext> parameterContexts : params) {
                for (ParameterContext parameterContext : parameterContexts) {
                    if (parameterContext.getParameterMethod() == ParameterMethod.setDelegate) {
                        Object[] rets = parameterContext.getArgs();
                        Integer index = (Integer) rets[0];
                        ParameterContext rel = context.getParams().getCurrentParameter().get(index);
                        parameterContext.setParameterMethod(rel.getParameterMethod());
                        parameterContext.setArgs(rel.getArgs());
                    }
                }
            }
        }
    }

    @Override
    public void noMoreSplits() {
        if (log.isDebugEnabled()) {
            log.debug(
                context.getTraceId() + ":lv=" + this.logicalView.getRelatedId() + " noMoreSplits:" + scanClient
                    .getSplitNum());
        }
        scanClient.incrementNoMoreSplit();
    }

    @Override
    public Integer getSourceId() {
        return logicalView.getRelatedId();
    }

    @Override
    void doOpen() {
        try {
            if (!scanClient.noMoreSplit()) {
                throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "input splits are not ready!");
            }

            if (dataTypes == null) {
                createBlockBuilders();
                List<DataType> columns = getDataTypes();
                dataTypes = new DataType[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    dataTypes[i] = columns.get(i);
                }
            }
            if (scanClient.getSplitNum() != 0) {
                scanClient.executePrefetchThread(false);
            } else {
                log.warn(
                    "TableScanExec open with empty splits, logicalView=" + logicalView.getRelatedId() + ",tableName="
                        + logicalView.getTableNames());
            }
        } catch (Throwable e) {
            TddlRuntimeException exception =
                new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, e.getMessage());
            this.isFinish = true;
            scanClient.setException(exception);
            scanClient.throwIfFailed();
        }
    }

    @Override
    Chunk doSourceNextChunk() {
        long fetchStartNano = System.nanoTime();
        try {
            return fetchChunk();
        } finally {
            fetchTimeCost.addAndGet(System.nanoTime() - fetchStartNano);
        }
    }

    protected void notifyFinish() {
        if (scanClient.compeleteAllExecuteSplit()) {
            //下发给mysql的所有split都执行结束
            isFinish = true;
        } else if (scanClient.noMorePushDownSplit()) {
            //所有的split都完全下发给mysql
            if (scanClient.noMorePrefetchSplit()) {
                consumeResultSet = scanClient.popResultSet();
                if (consumeResultSet == null) {
                    //所有的split都完全下发给mysql,且mysql都执行完了，可是依然获取不到consumeResultSet
                    isFinish = true;
                }
            } else if (scanClient.connectionCount() > 0) {
                //当前client还有残余连接存在，继续等待吧！
            } else {
                //当前算子可以结束了
                isFinish = true;
            }
        } else {
            if (scanClient.connectionCount() > 0) {
                if (scanClient.beingConnectionCount() > 0) {
                    //存在正在建连的split，这时我们只需要根据prefetch值判断要不要继续下发split
                    scanClient.executePrefetchThread(false);
                } else {
                    //虽然还有活的连接，但是都已经分配给其他tableScan，而且现在不存在正在建连的split了
                    //那么这个时候需要强制建连，确保当前tableScan有可用的ResultSet
                    scanClient.executePrefetchThread(true);
                }
            } else {
                //下发给mysql的split都返回了，可是依然获取不到consumeResultSet
                //强制下推split给mysql
                scanClient.executePrefetchThread(true);
            }
        }
    }

    protected Chunk fetchChunk() {
        int count = 0;
        try {
            if (consumeResultSet == null) {
                consumeResultSet = scanClient.popResultSet();
                if (consumeResultSet == null) {
                    notifyFinish();
                    if (consumeResultSet == null) {
                        return null;
                    }
                }
            }

            while (count < chunkLimit && !isFinish) {
                if (!consumeResultSet.next()) {
                    consumeResultSet.close();
                    consumeResultSet = scanClient.popResultSet();
                    if (consumeResultSet == null) {
                        notifyFinish();
                        if (consumeResultSet == null) {
                            break;
                        }
                    }
                    if (!consumeResultSet.next()) {
                        continue;
                    }
                }
                if (consumeResultSet.isPureAsyncMode()) {
                    final int filled = consumeResultSet.fillChunk(dataTypes, blockBuilders, chunkLimit - count);
                    count += filled;
                } else {
                    appendRow(consumeResultSet);
                    count++;
                }
            }
        } catch (Exception ex) {
            TddlRuntimeException exception = new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex, ex.getMessage());
            scanClient.setException(exception);
            if (isFinish) {
                log.debug(context.getTraceId() + " here occur error, but current scan is closed!", ex);
                return null;
            } else {
                scanClient.throwIfFailed();
            }
        }
        if (count == 0) {
            return null;
        } else {
            Chunk ret = buildChunkAndReset();
            return ret;
        }
    }

    protected void appendRow(TableScanClient.SplitResultSet consumeResultSet) throws SQLException {
        ResultSetCursorExec.buildOneRow(consumeResultSet.getResultSet(), dataTypes, blockBuilders, context);
    }

    @Override
    synchronized void doClose() {
        if (targetPlanStatGroup != null) {
            targetPlanStatGroup.fetchJdbcResultSetDuration.addAndGet(fetchTimeCost.getAndSet(0));
        }
        forceClose();
        scanClient.throwIfFailed();
    }

    @Override
    public synchronized void forceClose() {
        scanClient.close(this);
        if (consumeResultSet != null) {
            consumeResultSet.close();
            consumeResultSet = null;
        }
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypeList;
    }

    @Override
    public synchronized ListenableFuture<?> produceIsBlocked() {
        if (consumeResultSet != null && !consumeResultSet.closed.get()) {
            //Avoid to generate much more block futures, and it maybe invoke this method
            // although this scan isn't finished!
            return NOT_BLOCKED;
        } else {
            return scanClient.isBlocked();
        }
    }

    @Override
    public boolean produceIsFinished() {
        if (log.isDebugEnabled()) {
            log.debug(this.hashCode() + ":produceIsFinished=" + isFinish);
        }
        return scanClient.isFinished() || isFinish || (consumeResultSet == null && scanClient.noMoreExecuteSplit());
    }

    @Override
    public void setTargetPlanStatGroup(RuntimeStatistics.OperatorStatisticsGroup targetPlanStatGroup) {
        scanClient.setTargetPlanStatGroup(targetPlanStatGroup);
        super.setTargetPlanStatGroup(targetPlanStatGroup);
    }
}
