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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.LoadDataContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert.HandlerParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.SimpleShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.optimizer.context.LoadDataContext.END;

public class LogicalLoadDataHandler extends LogicalInsertHandler {

    public String DEFAULT_GROUP = "DEFAULT_GROUP";

    public PropUtil.LOAD_NULL_MODE null_mode;

    public LogicalLoadDataHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        this.null_mode = PropUtil.LOAD_NULL_MODE.valueOf(executionContext.getParamManager()
            .getString(ConnectionParams.LOAD_DATA_HANDLE_EMPTY_CHAR));
        HandlerParams handlerParams = new HandlerParams();

        LogicalInsert logicalInsert = (LogicalInsert) logicalPlan;
        checkInsertLimitation(logicalInsert, executionContext);
        String schemaName = logicalInsert.getSchemaName();
        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }
        if (!OptimizerContext.getContext(schemaName).getPartitionInfoManager()
            .isNewPartDbTable(logicalInsert.getLogicalTableName())) {
            buildSimpleShard(logicalInsert, executionContext.getLoadDataContext());
        }
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);

        int affectRows;
        long oldLastInsertId = executionContext.getConnection().getLastInsertId();
        try {
            affectRows = executeInsert(logicalInsert, executionContext, handlerParams);

        } catch (Throwable e) {
            // If exception happens, reset last insert id.
            executionContext.getConnection().setLastInsertId(oldLastInsertId);
            throw GeneralUtil.nestedException(e);
        }

        // If it's a single table, only MyJdbcHandler knows last insert id, and
        // it writes the value into Connection.
        // If it's a sharded table, correct last insert id is in LogicalInsert,
        // so overwrite the value MyJdbcHandler wrote.
        if (handlerParams.returnedLastInsertId != 0) {
            executionContext.getConnection().setReturnedLastInsertId(handlerParams.returnedLastInsertId);
        }
        if (handlerParams.lastInsertId != 0) {
            // Using sequence, override the value set by MyJdbcHandler
            executionContext.getConnection().setLastInsertId(handlerParams.lastInsertId);
        } else if (handlerParams.usingSequence) {
            // Using sequence, but all auto increment column values are
            // specified.
            executionContext.getConnection().setLastInsertId(oldLastInsertId);
        } else {
            // Not using sequence. Use the value set by MyJdbcHandler.
        }
        return new AffectRowCursor(affectRows);
    }

    private void buildSimpleShard(LogicalInsert logicalInsert, LoadDataContext loadDataContext) {
        ShardProcessor processor = ShardProcessor.buildSimpleShard(logicalInsert);
        if (processor instanceof SimpleShardProcessor) {
            loadDataContext.setShardProcessor((SimpleShardProcessor) processor);
        }
    }

    @Override
    protected int executeInsert(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                HandlerParams handlerParams) {
        LoadDataContext loadDataContext = executionContext.getLoadDataContext();
        String schemaName = executionContext.getSchemaName();
        boolean hasIndex = GlobalIndexMeta.hasIndex(
            logicalInsert.getLogicalTableName(), schemaName, executionContext);
        boolean ignoreIsSimpleInsert = executionContext.getParamManager().getBoolean(
            ConnectionParams.LOAD_DATA_IGNORE_IS_SIMPLE_INSERT);
        final boolean gsiConcurrentWrite =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(logicalInsert.getLogicalTableName());
        final boolean inSingleDb = or.isTableInSingleDb(logicalInsert.getLogicalTableName());
        loadDataContext.setInSingleDb(inSingleDb);
        if (!isBroadcast &&
            (!hasIndex ||
                (gsiConcurrentWrite &&
                    logicalInsert.isSimpleInsert(ignoreIsSimpleInsert &&
                        null != logicalInsert.getPrimaryInsertWriter())))) {
            boolean useBatchMode =
                executionContext.getParamManager().getBoolean(ConnectionParams.LOAD_DATA_USE_BATCH_MODE);
            if (inSingleDb) {
                useBatchMode = false;
            }
            loadDataContext.setUseBatch(useBatchMode);
            Pair<List<ShardConsumer>, List<AdaptiveLoadDataCursor>> cursors = null;
            List<ListenableFuture<?>> waitFutures = new ArrayList<>();
            try {

                cursors = concurrentCursors(executionContext, logicalInsert);

                for (int i = 0; i < cursors.getKey().size(); i++) {
                    waitFutures.add(cursors.getKey().get(i).doInit());
                }

                for (int i = 0; i < cursors.getValue().size(); i++) {
                    cursors.getValue().get(i).doInit();
                }

                for (int i = 0; i < waitFutures.size(); i++) {
                    waitFutures.get(i).get();
                }

                for (int i = 0; i < cursors.getValue().size(); i++) {
                    cursors.getValue().get(i).getRelNodeBlockingQueue().add(END);
                }

                int totalAffectRows = ExecUtils.getAffectRowsByCursors(
                    Lists.newArrayList(cursors.getValue()), false);
                if (hasIndex) {
                    totalAffectRows /= GlobalIndexMeta.getGsiIndexNum(logicalInsert.getLogicalTableName(),
                        executionContext.getSchemaName(), executionContext) + 1;
                }
                loadDataContext.getLoadDataAffectRows().set(totalAffectRows);
                if (loadDataContext.getThrowable() != null) {
                    throw loadDataContext.getThrowable();
                }
                return totalAffectRows;
            } catch (Throwable t) {
                loadDataContext.finish(t);
                for (int i = 0; i < cursors.getValue().size(); i++) {
                    cursors.getValue().get(i).getRelNodeBlockingQueue().clear();
                    cursors.getValue().get(i).getRelNodeBlockingQueue().add(END);
                }
                throw new TddlNestableRuntimeException(t);
            }
        } else {
            try {
                int totalAffectRows = 0;
                while (true) {
                    List<String> lines = loadDataContext.getParameters().take();
                    if (lines == END) {
                        break;
                    }
                    List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
                    for (int i = 0; i < lines.size(); i++) {
                        String line = lines.get(i);
                        List<String> fields =
                            Lists.newArrayList(Splitter.on(loadDataContext.getFieldTerminatedBy()).split(line));
                        if (loadDataContext.getAutoFillColumnIndex() != -1) {
                            fields.add(loadDataContext.getAutoFillColumnIndex(),
                                loadDataContext.isInSingleDb() ? "NULL" :
                                    SequenceManagerProxy.getInstance().nextValue(executionContext.getSchemaName(),
                                        ISequenceManager.AUTO_SEQ_PREFIX + loadDataContext.getTableName()).toString());
                        }
                        Map<Integer, ParameterContext> parameterContexts = Transformer.buildColumnParam(
                            loadDataContext.getMetaList(), fields, loadDataContext.getCharset(), null_mode);
                        batchParams.add(parameterContexts);
                        long length = SizeOf.sizeOfCharArray(line.length());
                        loadDataContext.getDataCacheManager().releaseMemory(length);
                    }
                    if (batchParams.size() > 0) {
                        Parameters parameters = new Parameters();
                        parameters.setBatchParams(batchParams);
                        executionContext.setParams(parameters);
                        totalAffectRows += super.executeInsert(logicalInsert, executionContext, handlerParams);
                    }
                }
                if (hasIndex) {
                    totalAffectRows /= GlobalIndexMeta.getGsiIndexNum(logicalInsert.getLogicalTableName(),
                        executionContext.getSchemaName(), executionContext) + 1;
                }
                return totalAffectRows;
            } catch (Throwable t) {
                // Can't commit
                executionContext.getTransaction()
                    .setCrucialError(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL);
                throw GeneralUtil.nestedException(t);
            }
        }
    }

    public Pair<List<ShardConsumer>, List<AdaptiveLoadDataCursor>> concurrentCursors(
        ExecutionContext executionContext, LogicalInsert logicalInsert) {
        executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, 0);
        String schemaName = logicalInsert.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        boolean useTrans = executionContext.getTransaction() instanceof IDistributedTransaction;
        int realParallism = executionContext.getParamManager().getInt(ConnectionParams.PARALLELISM);
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        boolean isSingleTable = or.isTableInSingleDb(logicalInsert.getLogicalTableName());
        final Set<String> dbNames;
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        if (partitionInfoManager.isNewPartDbTable(logicalInsert.getLogicalTableName())) {
            PartitionInfo partitionInfo =
                partitionInfoManager.getPartitionInfo(logicalInsert.getLogicalTableName());
            dbNames = partitionInfo.getTopology().keySet();
        } else {
            dbNames = or.getTableRule(
                logicalInsert.getLogicalTableName()).getActualTopology().keySet();
        }

        List<String> groupNames = Lists.newArrayList(dbNames);
        List<String> groupConnIdSet = PhyTableOperationUtil.buildGroConnSetFromGroups(executionContext, groupNames);
        CursorMeta cursorMeta = CalciteUtils.buildDmlCursorMeta();

        if (realParallism <= 0) {
            if (isSingleTable) {
                realParallism = 1;
            } else {
                realParallism = groupConnIdSet.size();
            }
        } else {
            if (useTrans) {
                realParallism = Math.min(groupConnIdSet.size(), realParallism);
            }
        }

        List<AdaptiveLoadDataCursor> indexCursors = new ArrayList<>();
        List<ShardConsumer> shardConsumers = new ArrayList<>();
        Map<String, BlockingQueue<Object>> multiQueus = new HashMap<>();
        if (useTrans) {
            List<BlockingQueue<Object>> blockingQueueList = new ArrayList<>();
            for (int i = 0; i < groupConnIdSet.size(); i++) {
                if (i < realParallism) {
                    BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
                    AdaptiveLoadDataCursor cursor = new AdaptiveLoadDataCursor(
                        executionContext, queue, cursorMeta, multiQueus);
                    indexCursors.add(cursor);
                    multiQueus.put(groupConnIdSet.get(i), queue);
                    blockingQueueList.add(queue);
                } else {
                    int index = (i - realParallism) % realParallism;
                    multiQueus.put(groupConnIdSet.get(i), blockingQueueList.get(index));
                }
            }

            for (int i = 0; i < realParallism; i++) {
                ShardConsumer shardConsumer = new ShardConsumer(
                    executionContext.getLoadDataContext(), executionContext, logicalInsert, multiQueus);
                shardConsumers.add(shardConsumer);
            }
        } else {
            BlockingQueue<Object> relNodeBlockingQueue = new LinkedBlockingQueue<>();
            multiQueus.put(DEFAULT_GROUP, relNodeBlockingQueue);
            for (int i = 0; i < realParallism; i++) {
                ShardConsumer shardConsumer = new ShardConsumer(
                    executionContext.getLoadDataContext(), executionContext, logicalInsert, multiQueus);
                shardConsumers.add(shardConsumer);
                AdaptiveLoadDataCursor cursor = new AdaptiveLoadDataCursor(
                    executionContext, relNodeBlockingQueue, cursorMeta, multiQueus);
                indexCursors.add(cursor);
            }
        }
        return new Pair<>(shardConsumers, indexCursors);
    }

    public class AdaptiveLoadDataCursor extends AbstractCursor {
        private final Logger
            logger = LoggerFactory.getLogger(AdaptiveLoadDataCursor.class);

        private final List<Throwable> exceptionsWhenClose = new ArrayList<>();
        private final ExecutionContext executionContext;
        private final BlockingQueue<Object> relNodeBlockingQueue;
        private final CursorMeta cursorMeta;
        private AtomicInteger affectRows = new AtomicInteger(-1);
        private int affectNum = 0;
        private ListenableFuture<?> future;
        private volatile Cursor currentCusor;
        private LoadDataContext loadDataContext;
        private Map<String, BlockingQueue<Object>> concurrentQueues;

        public AdaptiveLoadDataCursor(
            ExecutionContext executionContext, BlockingQueue<Object> relNodeBlockingQueue, CursorMeta cursorMeta,
            Map<String, BlockingQueue<Object>> concurrentQueues) {
            super(false);
            this.executionContext = executionContext.copy();
            this.cursorMeta = cursorMeta;
            this.returnColumns = cursorMeta.getColumns();
            this.loadDataContext = executionContext.getLoadDataContext();
            this.relNodeBlockingQueue = relNodeBlockingQueue;
            this.concurrentQueues = concurrentQueues;
        }

        @Override
        public void doInit() {
            if (this.inited) {
                return;
            }
            super.doInit();

            final Map mdcContext = MDC.getCopyOfContextMap();
            this.future = executionContext.getExecutorService().submitListenableFuture(
                executionContext.getSchemaName(), executionContext.getTraceId(), -1,
                () -> {
                    this.takePhyOpAndThenExec(mdcContext);
                    return null;
                }, executionContext.getRuntimeStatistics());
        }

        private void takePhyOpAndThenExec(Map mdcContext) {
            MDC.setContextMap(mdcContext);
            try {
                while (true) {
                    if (cursorClosed) {
                        break;
                    }

                    if (loadDataContext.getThrowable() != null) {
                        throw loadDataContext.getThrowable();
                    }

                    Object object = relNodeBlockingQueue.take();
                    if (object == END) {
                        relNodeBlockingQueue.add(END);
                        break;
                    }

                    if (object instanceof Long) {
                        loadDataContext.getDataCacheManager().releaseMemory((Long) object);
                        continue;
                    }
                    currentCusor = ExecutorContext.getContext(
                        executionContext.getSchemaName()).getTopologyExecutor().execByExecPlanNode(
                        (PhyTableOperation) object, executionContext);
                    int num =
                        ExecUtils.getAffectRowsByCursor(currentCusor);
                    affectNum += num;
                    loadDataContext.getLoadDataAffectRows().addAndGet(num);
                }
            } catch (Throwable t) {
                exceptionsWhenClose.add(t);
                logger.error("AdaptiveParallelLoadDataCursor failed", t);
                try {
                    loadDataContext.finish(t);
                } catch (Throwable ignore) {
                    //ignore
                }
                if (concurrentQueues != null) {
                    concurrentQueues.values().stream().forEach(q -> {
                        q.clear();
                        q.add(END);
                    });
                }
            }
        }

        @Override
        public Row doNext() {
            try {
                future.get();
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
            if (exceptionsWhenClose.size() > 0) {
                throw GeneralUtil.nestedException(exceptionsWhenClose.get(0));
            }
            if (affectRows.compareAndSet(-1, affectNum)) {
                ArrayRow arrayRow = new ArrayRow(1, cursorMeta);
                arrayRow.setObject(0, affectNum);
                arrayRow.setCursorMeta(cursorMeta);
                return arrayRow;
            } else {
                return null;
            }
        }

        @Override
        public List<Throwable> doClose(List<Throwable> exs) {
            this.isFinished = true;
            exs.addAll(exceptionsWhenClose);
            if (future != null) {
                future.cancel(true);
            }
            if (currentCusor != null) {
                currentCusor.close(exs);
            }
            return exs;
        }

        public BlockingQueue<Object> getRelNodeBlockingQueue() {
            return relNodeBlockingQueue;
        }
    }

    private String getGroupConnIdStr(PhyTableOperation phyOp, ExecutionContext ec) {
        return PhyTableOperationUtil.buildGroConnIdStr(phyOp, ec);
    }

    public class ShardConsumer {

        private final Logger
            logger = LoggerFactory.getLogger(ShardConsumer.class);

        private LoadDataContext loadDataContext;
        private ExecutionContext executionContext;
        private LogicalInsert logicalInsert;
        private Map<String, BlockingQueue<Object>> concurrentQueues;

        public ShardConsumer(LoadDataContext loadDataContext,
                             ExecutionContext executionContext, LogicalInsert logicalInsert,
                             Map<String, BlockingQueue<Object>> concurrentQueues) {
            this.loadDataContext = loadDataContext;
            this.executionContext = executionContext.copy();
            this.logicalInsert = logicalInsert;
            this.concurrentQueues = concurrentQueues;
        }

        public ListenableFuture<?> doInit() {

            final Map mdcContext = MDC.getCopyOfContextMap();
            return executionContext.getExecutorService().submitListenableFuture(
                executionContext.getSchemaName(), executionContext.getTraceId(), -1,
                () -> {
                    MDC.setContextMap(mdcContext);
                    return run();
                }, executionContext.getRuntimeStatistics());
        }

        public Object run() {
            try {
                while (true) {

                    if (loadDataContext.getThrowable() != null) {
                        throw loadDataContext.getThrowable();
                    }

                    List<String> lines = loadDataContext.getParameters().take();
                    if (lines == END) {
                        loadDataContext.getParameters().add(END);
                        break;
                    }
                    List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
                    long totalMemory = 0L;
                    for (int i = 0; i < lines.size(); i++) {
                        String line = lines.get(i);
                        totalMemory += SizeOf.sizeOfCharArray(line.length());
                        List<String> fields =
                            Lists.newArrayList(Splitter.on(loadDataContext.getFieldTerminatedBy()).split(line));
                        if (loadDataContext.getAutoFillColumnIndex() != -1) {
                            fields.add(loadDataContext.getAutoFillColumnIndex(),
                                loadDataContext.isInSingleDb() ? "NULL" :
                                    SequenceManagerProxy.getInstance().nextValue(executionContext.getSchemaName(),
                                        ISequenceManager.AUTO_SEQ_PREFIX + loadDataContext.getTableName()).toString());
                        }
                        Map<Integer, ParameterContext> parameterContexts = Transformer.buildColumnParam(
                            loadDataContext.getMetaList(), fields, loadDataContext.getCharset(), null_mode);
                        batchParams.add(parameterContexts);
                    }
                    if (batchParams.size() > 0) {
                        Parameters parameters = new Parameters();
                        parameters.setBatchParams(batchParams);
                        executionContext.setParams(parameters);
                        List<RelNode> allPhyPlan = getAllRelNode(
                            logicalInsert, executionContext);
                        if (concurrentQueues.size() > 1) {
                            //use trans
                            long averageSize = totalMemory / allPhyPlan.size();
                            long div = totalMemory % allPhyPlan.size();
                            for (int i = 0; i < allPhyPlan.size(); i++) {
                                long calcSize = 0;
                                if (i == allPhyPlan.size() - 1) {
                                    calcSize = div + averageSize;
                                } else {
                                    calcSize = averageSize;
                                }
                                PhyTableOperation phyTableOperation = (PhyTableOperation) allPhyPlan.get(i);
                                BlockingQueue<Object> blockingQueue =
                                    concurrentQueues.get(getGroupConnIdStr(phyTableOperation, executionContext));
                                blockingQueue.add(phyTableOperation);
                                blockingQueue.add(calcSize);
                            }
                        } else {
                            BlockingQueue<Object> blockingQueue =
                                concurrentQueues.get(DEFAULT_GROUP);
                            blockingQueue.addAll(allPhyPlan);
                            blockingQueue.add(totalMemory);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("ShardConsumer failed", t);

                try {
                    loadDataContext.finish(t);
                } catch (Throwable ignore) {
                    //ignore
                }
                if (concurrentQueues != null) {
                    concurrentQueues.values().stream().forEach(q -> {
                        q.clear();
                        q.add(END);
                    });
                }
            }
            return null;
        }

        private List<RelNode> getAllRelNode(
            LogicalInsert logicalInsert,
            ExecutionContext executionContext) {
            List<RelNode> allPhyPlan = new ArrayList<>();

            if (null != logicalInsert.getPrimaryInsertWriter()) {
                // Get plan for primary
                final InsertWriter primaryWriter = logicalInsert.getPrimaryInsertWriter();
                List<RelNode> inputs = primaryWriter.getInput(executionContext);

                allPhyPlan.addAll(inputs);

                executionContext.getLoadDataContext().setGsiInsertTurn(true);
                final List<InsertWriter> gsiWriters = logicalInsert.getGsiInsertWriters();
                gsiWriters.stream()
                    .map(gsiWriter -> gsiWriter.getInput(executionContext))
                    .filter(w -> !w.isEmpty())
                    .forEach(w -> {
                        allPhyPlan.addAll(w);
                    });
                executionContext.getLoadDataContext().setGsiInsertTurn(false);
            } else {

                List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>();
                PhyTableInsertSharder insertSharder = new PhyTableInsertSharder(logicalInsert,
                    executionContext.getParams(),
                    SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
                allPhyPlan.addAll(logicalInsert.getInput(insertSharder, shardResults, executionContext));
            }
            return allPhyPlan;
        }

    }

}
