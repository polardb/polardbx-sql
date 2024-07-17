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

package com.alibaba.polardbx.executor.columns;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import lombok.Value;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_COUNT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_WAIT;

public class ColumnChecker {
    private static final Logger logger = LoggerFactory.getLogger(Checker.class);
    private static final int TASK_CANCELLED = -1;

    @Value
    public static class Params {
        long batchSize;
        long parallelism;
        long earlyFailNumber;

        public static ColumnChecker.Params buildFromExecutionContext(ExecutionContext ec) {
            ParamManager pm = ec.getParamManager();

            return new ColumnChecker.Params(
                pm.getLong(ConnectionParams.GSI_CHECK_BATCH_SIZE),
                pm.getLong(ConnectionParams.GSI_CHECK_PARALLELISM),
                pm.getLong(ConnectionParams.GSI_EARLY_FAIL_NUMBER)
            );
        }
    }

    private final String schemaName;
    private final String tableName;
    private final String sourceColumn;
    private final String targetColumn;
    private final String checkerColumn;
    private final List<String> primaryKeys;

    private final Params params;
    private final ConcurrentPolicy concurrentPolicy;
    PhyTableOperation selectPlan;
    private final ITransactionManager tm;

    enum ConcurrentPolicy {
        TABLE_CONCURRENT,
        DB_CONCURRENT,
        INSTANCE_CONCURRENT;

        public static ConcurrentPolicy fromValue(String value) {
            if (StringUtils.isEmpty(value)) {
                return null;
            }
            try {
                return ConcurrentPolicy.valueOf(value.toUpperCase());
            } catch (Exception e) {
                return null;
            }
        }
    }

    public ColumnChecker(String schemaName, String tableName, String checkerColumn, String sourceColumn,
                         String targetColumn, boolean simpleChecker, Params params, ExecutionContext ec) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.checkerColumn = checkerColumn;
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
        this.params = params;

        PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        this.primaryKeys =
            tableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).collect(Collectors.toList());
        // TODO(qianjing): for now we just check if there is inconsistent, so batch size is 1
        this.selectPlan =
            builder.buildSelectForColumnCheck(tableMeta, primaryKeys, 1, checkerColumn, sourceColumn, targetColumn,
                simpleChecker);
        this.tm = ExecutorContext.getContext(schemaName).getTransactionManager();
        // by default, we use group concurrent
        this.concurrentPolicy = Optional.ofNullable(
                ConcurrentPolicy.fromValue(ec.getParamManager().getString(ConnectionParams.OMC_CHECKER_CONCURRENT_POLICY)))
            .orElse(ConcurrentPolicy.INSTANCE_CONCURRENT);
    }

    public void checkInBackfill(ExecutionContext ec) {
        SQLRecorderLogger.ddlLogger.warn(
            MessageFormat.format("[{0}] Checker job: {1}[{2}] source {3} target {4} start.", ec.getTraceId(),
                schemaName, tableName, sourceColumn, targetColumn));

        Map<String, Set<String>> topology = GsiUtils.getPhyTables(schemaName, tableName);
        int taskCount = topology.values().stream().map(Set::size).reduce(0, Integer::sum);
        final AtomicInteger startedTaskCount = new AtomicInteger(0);
        final CountDownLatch taskFinishBarrier = new CountDownLatch(taskCount);
        final AtomicInteger taskId = new AtomicInteger(0);

        // Group tasks
        final List<List<Pair<String, String>>> taskGroups = new ArrayList<>();
        switch (concurrentPolicy) {
        case INSTANCE_CONCURRENT:
            Map<String, List<Pair<String, String>>> taskByInstance = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
                String dbIndex = entry.getKey();
                TGroupDataSource groupDataSource =
                    (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                        .getGroupExecutor(dbIndex).getDataSource();
                String instanceId = groupDataSource.getMasterSourceAddress();
                for (String tbIndex : entry.getValue()) {
                    taskByInstance.computeIfAbsent(instanceId, k -> new ArrayList<>())
                        .add(new Pair<>(dbIndex, tbIndex));
                }
            }

            taskGroups.addAll(taskByInstance.values());
            break;
        case DB_CONCURRENT:
            for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
                List<Pair<String, String>> taskGroup = new ArrayList<>();
                String dbIndex = entry.getKey();
                for (String tbIndex : entry.getValue()) {
                    taskGroup.add(new Pair<>(dbIndex, tbIndex));
                }
                taskGroups.add(taskGroup);
            }
            break;
        case TABLE_CONCURRENT:
            for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
                String dbIndex = entry.getKey();
                for (String tbIndex : entry.getValue()) {
                    List<Pair<String, String>> taskGroup = new ArrayList<>();
                    taskGroup.add(new Pair<>(dbIndex, tbIndex));
                    taskGroups.add(new ArrayList<>(taskGroup));
                }
            }
            break;
        default:
            break;
        }

        // Use a bounded blocking queue to control the parallelism.
        final BlockingQueue<Object> blockingQueue =
            params.getParallelism() <= 0 ? null : new ArrayBlockingQueue<>((int) params.getParallelism());
        Exception exception = null;
        while (!taskGroups.isEmpty()) {
            final List<FutureTask<Void>> futures = new ArrayList<>(taskCount);
            for (List<Pair<String, String>> taskGroup : taskGroups) {
                if (!taskGroup.isEmpty()) {
                    Pair<String, String> dbTbIndex = taskGroup.get(taskGroup.size() - 1);
                    taskGroup.remove(taskGroup.size() - 1);
                    futures.add(new FutureTask<>(() -> {
                        int ret =
                            startedTaskCount.getAndUpdate(origin -> (origin == TASK_CANCELLED) ? origin : (origin + 1));
                        if (ret == TASK_CANCELLED) {
                            return;
                        }
                        try {
                            forEachPhyTable(dbTbIndex.getKey(), dbTbIndex.getValue(), ec);
                        } finally {
                            taskFinishBarrier.countDown();
                            // Poll in finally to prevent dead lock on putting blockingQueue.
                            if (blockingQueue != null) {
                                blockingQueue.poll(); // Parallelism control notify.
                            }
                        }
                    }, null));
                }
            }
            taskGroups.removeIf(List::isEmpty);
            Collections.shuffle(futures);
            exception = Checker.runTasks(futures, blockingQueue, params.getParallelism());
            if (exception != null) {
                break;
            }
        }

        // To ensure that all tasks have finished at this moment, otherwise we may leak resources in execution context,
        // such as memory pool.
        try {
            int notStartedTaskCount = taskCount - startedTaskCount.getAndUpdate(origin -> TASK_CANCELLED);
            for (int i = 0; i < notStartedTaskCount; i++) {
                taskFinishBarrier.countDown();
            }
            taskFinishBarrier.await();
        } catch (Throwable t) {
            logger.error("Failed to waiting for checker task finish.", t);
        }

        // Throw if have exceptions.
        if (exception != null) {
            throw GeneralUtil.nestedException(exception);
        }

        SQLRecorderLogger.ddlLogger.warn(
            MessageFormat.format("[{0}] Checker job: {1}[{2}] source {3} target {4} finish.", ec.getTraceId(),
                schemaName, tableName, sourceColumn, targetColumn));
    }

    private void forEachPhyTable(String dbIndex, String tbIndex, ExecutionContext baseEc) {
        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Column checker start phy for {1}[{2}]",
            baseEc.getTraceId(),
            dbIndex,
            tbIndex));

        // Build parameter
        final Map<Integer, ParameterContext> params = new HashMap<>(1);
        params.put(1, PlannerUtils.buildParameterContextForTableName(tbIndex, 1));

        // Build plan
        PhyTableOperation targetPhyOp = this.selectPlan;
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(tbIndex)));
        buildParams.setDynamicParams(params);
        PhyTableOperation plan =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

        Function<ExecutionContext, List<List<Pair<ParameterContext, byte[]>>>> select = (ec) -> {
            final Cursor cursor = ExecutorHelper.executeByCursor(plan, ec, false);
            List<List<Pair<ParameterContext, byte[]>>> result = new ArrayList<>();
            try {
                Row row;
                while ((row = cursor.next()) != null) {
                    result.add(Checker.row2objects(row));
                }
            } finally {
                cursor.close(new ArrayList<>());
            }
            ec.getTransaction().commit();
            return result;
        };

        ParamManager.setVal(
            baseEc.getParamManager().getProps(),
            ConnectionParams.SOCKET_TIMEOUT,
            Integer.toString(1000 * 60 * 60 * 24 * 7),
            true
        );

        List<List<Pair<ParameterContext, byte[]>>> result =
            GsiUtils.retryOnException(() -> GsiUtils.wrapWithSingleDbTrx(tm, baseEc, select),
                e -> Boolean.TRUE,
                (e, retryCount) -> errConsumer(plan, baseEc, e, retryCount));

        if (!result.isEmpty()) {
            throw GeneralUtil.nestedException(
                String.format("Column checker found error after backfill. Error record: %s.Will rollback this job now.",
                    detailString(result.get(0))));
        }

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Column checker finish phy for {1}[{2}]",
            baseEc.getTraceId(),
            dbIndex,
            tbIndex));
    }

    /**
     * Print log for error
     *
     * @param plan Select plan
     * @param ec ExecutionContext
     * @param e Exception object
     * @param retryCount Times of retried
     */
    protected static void errConsumer(PhyTableOperation plan, ExecutionContext ec,
                                      TddlNestableRuntimeException e, int retryCount) {
        final String dbIndex = plan.getDbIndex();
        final String phyTable = plan.getTableNames().get(0).get(0);

        if (retryCount < RETRY_COUNT) {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] error occurred while checking {1}[{2}] retry count[{3}]: {4}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                retryCount,
                e.getMessage()));
            try {
                TimeUnit.MILLISECONDS.sleep(RETRY_WAIT[retryCount]);
            } catch (InterruptedException ex) {
                // ignore
            }
        } else {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] error occurred while checking {1}[{2}] retry count[{3}]: {4}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                retryCount,
                e.getMessage()));
            throw GeneralUtil.nestedException(e);
        }
    }

    private String detailString(List<Pair<ParameterContext, byte[]>> primaryRow) {
        final Map<String, Map<String, Object>> details = new HashMap<>();
        Map<String, Object> pk = new HashMap<>();
        for (int i = 0; i < primaryKeys.size(); i++) {
            pk.put(primaryKeys.get(i), primaryRow.get(i).getKey().getArgs()[1]);
        }
        details.put("primary key", pk);

        Map<String, Object> col = new HashMap<>();
        int idx = primaryKeys.size();
        col.put(sourceColumn, primaryRow.get(idx).getKey().getArgs()[1]);
        idx++;
        col.put(targetColumn, primaryRow.get(idx).getKey().getArgs()[1]);
        details.put("inconsistent columns", col);

        return JSON.toJSONString(details, SerializerFeature.WriteMapNullValue);
    }
}
