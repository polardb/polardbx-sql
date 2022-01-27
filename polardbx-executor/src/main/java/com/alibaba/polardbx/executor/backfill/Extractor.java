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

package com.alibaba.polardbx.executor.backfill;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.workqueue.PriorityWorkQueue;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.StringEscapeUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus.SUCCESS;
import static com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus.UNFINISHED;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_COUNT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_WAIT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_BACKFILL_EXCEPTION;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class Extractor {

    protected final String schemaName;
    protected final String sourceTableName;
    private final String targetTableName;
    private final long batchSize;
    private volatile RateLimiter rateLimiter;
    private volatile long nowSpeedLimit;
    private final long parallelism;
    private final GsiBackfillManager backfillManager;

    /* Templates, built once, used every time. */
    /**
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_primary_table}
     * WHERE (pk0, ... , pkn) <= (?, ... , ?) => DNF
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE
     * </pre>
     */
    private final PhyTableOperation planSelectWithMax;
    /**
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_primary_table}
     * WHERE (pk0, ... , pkn) > (?, ... , ?) => DNF
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE
     * </pre>
     */
    private final PhyTableOperation planSelectWithMin;
    /**
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_primary_table}
     * WHERE (pk0, ... , pkn) > (?, ... , ?) => DNF
     *   AND (pk0, ... , pkn) <= (?, ... , ?) => DNF
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE
     * </pre>
     */
    private final PhyTableOperation planSelectWithMinAndMax;
    /**
     * <pre>
     * SELECT IFNULL(MAX(pk0), 0), ... , IFNULL(MAX(pkn), 0)
     * FROM (
     *  SELECT pk0, ... , pkn
     *  FROM {physical_table}
     *  ORDER BY pk0 DESC, ... , pkn DESC LIMIT 1
     * ) T1
     * </pre>
     */
    private final PhyTableOperation planSelectMaxPk;
    //private final BitSet primaryKeys;
    private final List<Integer> primaryKeysId;
    /**
     * map the column id to primary key order
     * E.G. the 2-th column is the 1-st key in primary key list, thus there is an entry {2}->{1}
     */
    private final Map<Long, Long> primaryKeysIdMap;

    private final ITransactionManager tm;

    private final Reporter reporter;
    private com.alibaba.polardbx.executor.backfill.Throttle t;

    protected Extractor(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                        long speedMin,
                        long speedLimit,
                        long parallelism,
                        PhyTableOperation planSelectWithMax, PhyTableOperation planSelectWithMin,
                        PhyTableOperation planSelectWithMinAndMax,
                        PhyTableOperation planSelectMaxPk,
                        List<Integer> primaryKeysId) {
        this.schemaName = schemaName;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.batchSize = batchSize;
        // Use RateLimiter in spark_project, because RateLimiter have some unknown bug in Guava.
        this.rateLimiter = speedLimit <= 0 ? null : RateLimiter.create(speedLimit);
        this.nowSpeedLimit = speedLimit;
        this.parallelism = parallelism;
        this.planSelectWithMax = planSelectWithMax;
        this.planSelectWithMin = planSelectWithMin;
        this.planSelectWithMinAndMax = planSelectWithMinAndMax;
        this.planSelectMaxPk = planSelectMaxPk;
        //this.primaryKeys = primaryKeys;
        this.primaryKeysId = primaryKeysId;
        this.primaryKeysIdMap = new HashMap<>();
        for (int i = 0; i < primaryKeysId.size(); i++) {
            primaryKeysIdMap.put((long)primaryKeysId.get(i), (long)i);
        }
        this.tm = ExecutorContext.getContext(schemaName).getTransactionManager();
        this.backfillManager = new GsiBackfillManager(schemaName);
        this.reporter = new Reporter(backfillManager);
        this.t = new com.alibaba.polardbx.executor.backfill.Throttle(speedMin, speedLimit, schemaName);
    }

    /**
     * Load latest position mark
     *
     * @param ec Id of parent DDL job
     * @return this
     */
    public Extractor loadBackfillMeta(ExecutionContext ec) {
        Long backfillId = ec.getBackfillId();

        final String positionMarkHint = ec.getParamManager().getString(ConnectionParams.GSI_BACKFILL_POSITION_MARK);

        if (TStringUtil.isEmpty(positionMarkHint)) {
            // Init position mark with upper bound
            final List<GsiBackfillManager.BackfillObjectRecord> initBfoList = initAllUpperBound(ec, backfillId);

            // Insert ignore
            backfillManager.initBackfillMeta(ec, initBfoList);
        } else {
            // Load position mark from HINT
            final List<GsiBackfillManager.BackfillObjectRecord> backfillObjects = JSON
                .parseArray(StringEscapeUtils.unescapeJava(positionMarkHint),
                    GsiBackfillManager.BackfillObjectRecord.class);

            // Insert ignore
            backfillManager
                .initBackfillMeta(ec, backfillId, schemaName, sourceTableName, targetTableName, backfillObjects);
        }

        // Load from system table
        this.reporter.loadBackfillMeta(backfillId);
        SQLRecorderLogger.ddlLogger.info(
            String.format("loadBackfillMeta for backfillId %d: %s", backfillId, this.reporter.backfillBean));
        return this;
    }

    /**
     * Init upper bound for all physical primary table
     *
     * @param ec Execution context
     * @param ddlJobId Ddl job id
     * @return BackfillObjectRecord with upper bound initialized, one for each physical table and primary key
     */
    private List<GsiBackfillManager.BackfillObjectRecord> initAllUpperBound(ExecutionContext ec, long ddlJobId) {

        return getSourcePhyTables().entrySet()
            .stream()
            .flatMap(e -> e.getValue()
                .stream()
                .flatMap(phyTable -> initUpperBound(ec, ddlJobId, e.getKey(), phyTable, primaryKeysId).stream()))
            .collect(Collectors.toList());
    }

    /**
     * Get max primary key value from physical table
     *
     * @param baseEc Execution context
     * @param ddlJobId Ddl job id
     * @param dbIndex Group key
     * @param phyTable Physical table name
     * @param primaryKeysId Index of primary keys for ResultSet of data extracted from source
     * @return BackfillObjectRecord with upper bound initialized, one for each primary key
     */
    private List<GsiBackfillManager.BackfillObjectRecord> initUpperBound(final ExecutionContext baseEc,
                                                                         final long ddlJobId,
                                                                         final String dbIndex, final String phyTable,
                                                                         final List<Integer> primaryKeysId) {
        // Build parameter
        final Map<Integer, ParameterContext> params = new HashMap<>(1);
        params.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        // Build plan
        final PhyTableOperation plan = new PhyTableOperation(this.planSelectMaxPk);
        plan.setDbIndex(dbIndex);
        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
        plan.setParam(params);

        // Execute query
        final List<Map<Integer, ParameterContext>> upperBound = GsiUtils.wrapWithSingleDbTrx(tm,
            baseEc,
            (ec) -> {
                final Cursor cursor = ExecutorHelper.execute(plan, ec);
                try {
                    return Transformer.convertUpperBoundWithDefault(cursor, (columnMeta, i) -> {
                        // Generate default parameter context for upper bound of empty source table
                        ParameterMethod defaultMethod = ParameterMethod.setString;
                        Object defaultValue = "0";

                        final DataType columnType = columnMeta.getDataType();
                        if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.DateType, DataTypes.TimestampType,
                            DataTypes.DatetimeType, DataTypes.TimeType, DataTypes.YearType)) {
                            // For time data type, use number zero as upper bound
                            defaultMethod = ParameterMethod.setLong;
                            defaultValue = 0L;
                        }

                        return new ParameterContext(defaultMethod, new Object[] {i, defaultValue});
                    });
                } finally {
                    cursor.close(new ArrayList<>());
                }
            });

        SQLRecorderLogger.ddlLogger.warn(MessageFormat
            .format("[{0}] Backfill upper bound [{1}, {2}] {3}",
                baseEc.getTraceId(),
                dbIndex,
                phyTable,
                GsiUtils.rowToString(upperBound.get(0))));

        // Convert to BackfillObjectRecord
        final AtomicInteger srcIndex = new AtomicInteger(0);
        return primaryKeysId.stream().mapToInt(columnIndex -> columnIndex).mapToObj(columnIndex -> {
            if (upperBound.isEmpty()) {
                // Table is empty, no upper bound needed
                return GsiUtils.buildBackfillObjectRecord(ddlJobId,
                    schemaName,
                    sourceTableName,
                    targetTableName,
                    dbIndex,
                    phyTable,
                    columnIndex);
            } else {
                final ParameterContext pc = upperBound.get(0).get(srcIndex.getAndIncrement() + 1);
                return GsiUtils.buildBackfillObjectRecord(ddlJobId,
                    schemaName,
                    sourceTableName,
                    targetTableName,
                    dbIndex,
                    phyTable,
                    columnIndex,
                    pc.getParameterMethod().name(),
                    com.alibaba.polardbx.executor.gsi.utils.Transformer.serializeParam(pc));
            }
        }).collect(Collectors.toList());
    }

    /**
     * Lock and read batch from logical table, feed them to consumer
     *
     * @param ec base execution context
     * @param consumer next step
     */
    public void foreachBatch(ExecutionContext ec,
                             BiConsumer<List<Map<Integer, ParameterContext>>, Pair<ExecutionContext, String>> consumer) {
        // For each physical table there is a backfill object which represents a row in
        // system table

        List<Future> futures = new ArrayList<>(16);

        // Re-balance by physicalDb.
        List<List<GsiBackfillManager.BackfillObjectBean>> tasks = reporter.getBackfillBean().backfillObjects.values()
            .stream()
            .filter(v -> v.get(0).status.is(UNFINISHED))
            .collect(Collectors.toList());
        Collections.shuffle(tasks);

        if (!tasks.isEmpty()) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] Backfill job: {1} start with {2} task(s) parallelism {3}.",
                    ec.getTraceId(), tasks.get(0).get(0).jobId, tasks.size(), parallelism));
        }

        AtomicReference<Exception> excep = new AtomicReference<>(null);
        if (parallelism <= 0 || parallelism >= PriorityWorkQueue.getInstance().getCorePoolSize()) {
            // Full queued.
            tasks.forEach(v -> {
                FutureTask<Void> task = new FutureTask<>(
                    () -> foreachPhyTableBatch(v.get(0).physicalDb, v.get(0).physicalTable, v, ec, consumer), null);
                futures.add(task);
                PriorityWorkQueue.getInstance()
                    .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_BACKFILL_TASK);
            });
        } else {

            // Use a bounded blocking queue to control the parallelism.
            BlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>((int) parallelism);
            tasks.forEach(v -> {
                try {
                    blockingQueue.put(new Object());
                } catch (Exception e) {
                    excep.set(e);
                }
                if (null == excep.get()) {
                    FutureTask<Void> task = new FutureTask<>(() -> {
                        try {
                            foreachPhyTableBatch(v.get(0).physicalDb, v.get(0).physicalTable, v, ec, consumer);
                        } finally {
                            // Poll in finally to prevent dead lock on putting blockingQueue.
                            blockingQueue.poll();
                        }
                        return null;
                    });
                    futures.add(task);
                    PriorityWorkQueue.getInstance()
                        .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_BACKFILL_TASK);
                }
            });
        }

        if (excep.get() != null) {
            // Interrupt all.
            futures.forEach(f -> {
                try {
                    f.cancel(true);
                } catch (Throwable ignore) {
                }
            });
        }

        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                futures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignore) {
                    }
                });
                if (null == excep.get()) {
                    excep.set(e);
                }
            }
        }

        t.stop();

        if (excep.get() != null) {
            throw GeneralUtil.nestedException(excep.get());
        }

        // After all physical table finished
        reporter.updateBackfillStatus(ec, GsiBackfillManager.BackfillStatus.SUCCESS);
    }

    /**
     * Lock and read batch from physical table, feed them to consumer
     *
     * @param dbIndex physical db key
     * @param phyTable physical table name
     * @param ec base execution context
     * @param loader next step
     */
    private void foreachPhyTableBatch(String dbIndex, String phyTable,
                                      List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                      ExecutionContext ec,
                                      BiConsumer<List<Map<Integer, ParameterContext>>, Pair<ExecutionContext, String>> loader) {
        // Load upper bound
        List<ParameterContext> upperBoundParam = buildUpperBoundParam(backfillObjects.size(), backfillObjects, primaryKeysIdMap);
        final boolean withUpperBound = GeneralUtil.isNotEmpty(upperBoundParam);

        // Init historical position mark
        long successRowCount = backfillObjects.get(0).successRowCount;
        List<ParameterContext> lastPk = initSelectParam(backfillObjects, primaryKeysIdMap);

        List<Map<Integer, ParameterContext>> lastBatch = null;
        boolean finished = false;
        do {
            if (rateLimiter != null) {
                rateLimiter.acquire((int) batchSize);
            }
            long start = System.currentTimeMillis();

            // Dynamic adjust lower bound of rate.
            final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
            if (dynamicRate > 0) {
                t.resetMaxRate(dynamicRate);
            }

            // For next batch, build select plan and parameters
            final PhyTableOperation selectPlan = buildSelectPlanWithParam(dbIndex,
                phyTable,
                batchSize,
                Stream.concat(lastPk.stream(), upperBoundParam.stream()).collect(Collectors.toList()),
                GeneralUtil.isNotEmpty(lastPk),
                withUpperBound);

            lastBatch = GsiUtils.retryOnException(
                // 1. Lock rows within trx1 (single db transaction)
                // 2. Fill into index table within trx2 (XA transaction)
                // 3. Trx1 commit, if (success) {trx2 commit} else {trx2 rollback}
                () -> GsiUtils.wrapWithSingleDbTrx(tm, ec, (selectEc) -> extract(selectPlan, selectEc, loader)),
                e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                    || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT))
                    || e.getMessage().contains("Loader check error."),
                (e, retryCount) -> deadlockErrConsumer(selectPlan, ec, e, retryCount));

            // For status recording
            List<ParameterContext> beforeLastPk = lastPk;

            // Build parameter for next batch
            lastPk = buildSelectParam(lastBatch, primaryKeysId);

            finished = lastBatch.size() != batchSize;

            // Update position mark
            successRowCount += lastBatch.size();
            reporter.updatePositionMark(ec, backfillObjects, successRowCount, lastPk, beforeLastPk, finished, primaryKeysIdMap);
            ec.getStats().backfillRows.addAndGet(lastBatch.size());

            if (!finished) {
                t.feedback(new com.alibaba.polardbx.executor.backfill.Throttle.FeedbackStats(
                    System.currentTimeMillis() - start, start, lastBatch.size()));
            }

            if (rateLimiter != null) {
                // Limit rate.
                rateLimiter.setRate(t.getNewRate());
            }

            // Check DDL is ongoing.
            if (CrossEngineValidator.isJobInterrupted(ec)) {
                long jobId = ec.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }
        } while (!finished);

        reporter.addBackfillCount(successRowCount);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Last backfill row for {1}[{2}][{3}]: {4}",
            ec.getTraceId(),
            dbIndex,
            phyTable,
            successRowCount,
            GsiUtils.rowToString(lastBatch.isEmpty() ? null : lastBatch.get(lastBatch.size() - 1))));
    }

    /**
     * Print log for deadlock found
     *
     * @param plan Select plan
     * @param ec ExecutionContext
     * @param e Exception object
     * @param retryCount Times of retried
     */
    private static void deadlockErrConsumer(PhyTableOperation plan, ExecutionContext ec,
                                            TddlNestableRuntimeException e, int retryCount) {
        final String dbIndex = plan.getDbIndex();
        final String phyTable = plan.getTableNames().get(0).get(0);
        final String row = GsiUtils.rowToString(plan.getParam());

        if (retryCount < RETRY_COUNT) {

            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while extracting backfill data from {1}[{2}][{3}] retry count[{4}]: {5}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                row,
                retryCount,
                e.getMessage()));

            try {
                TimeUnit.MILLISECONDS.sleep(RETRY_WAIT[retryCount]);
            } catch (InterruptedException ex) {
                // ignore
            }
        } else {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while extracting backfill data from {1}[{2}][{3}] throw: {4}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                row,
                e.getMessage()));

            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Extract data and apply to next pipeline
     *
     * @param extractPlan select plan
     * @param extractEc execution context for select plan
     * @param next next pipeline
     * @return converted data of current batch
     */
    private static List<Map<Integer, ParameterContext>> extract(PhyTableOperation extractPlan,
                                                                ExecutionContext extractEc,
                                                                BiConsumer<List<Map<Integer, ParameterContext>>, Pair<ExecutionContext, String>> next) {
        // Extract
        Cursor extractCursor = ExecutorHelper.execute(extractPlan, extractEc);

        // Transform
        final List<Map<Integer, ParameterContext>> result;
        try {
            result = com.alibaba.polardbx.executor.gsi.utils.Transformer.buildBatchParam(extractCursor);
        } finally {
            extractCursor.close(new ArrayList<>());
        }

        FailPoint.injectRandomExceptionFromHint(FP_RANDOM_BACKFILL_EXCEPTION, extractEc);

        // Load
        next.accept(result, Pair.of(extractEc, extractPlan.getDbIndex()));

        return result;
    }

    private static List<ParameterContext> buildUpperBoundParam(int offset,
                                                               List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                                               Map<Long, Long> primaryKeysIdMap) {
        // Value of Primary Key can never be null
        return backfillObjects.stream().sorted(Comparator.comparingLong(o -> primaryKeysIdMap.get(o.columnIndex)))
            .filter(bfo -> Objects.nonNull(bfo.maxValue)).map(
            bfo -> com.alibaba.polardbx.executor.gsi.utils.Transformer
                .buildParamByType(offset + bfo.columnIndex, bfo.parameterMethod, bfo.maxValue)).collect(
            Collectors.toList());
    }

    /**
     * Build params for physical select
     *
     * @param batchResult Result of last batch
     * @param primaryKeysId Pk index in parameter list
     * @return built plan
     */
    private static List<ParameterContext> buildSelectParam(List<Map<Integer, ParameterContext>> batchResult,
                                                           List<Integer> primaryKeysId) {
        List<ParameterContext> lastPk = null;
        if (GeneralUtil.isNotEmpty(batchResult)) {
            Map<Integer, ParameterContext> lastRow = batchResult.get(batchResult.size() - 1);
            lastPk = primaryKeysId.stream().mapToInt(i -> i).mapToObj(i -> lastRow.get(i + 1)).collect(Collectors.toList());
        }

        return lastPk;
    }

    /**
     * Build params for physical select from loaded backfill objects
     *
     * @param backfillObjects Backfill objects loaded from meta table
     */
    private static List<ParameterContext> initSelectParam(List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                                          Map<Long, Long> primaryKeysIdMap) {
        return backfillObjects.stream()
            // Primary key is null only when it is initializing
            .filter(bfo -> Objects.nonNull(bfo.lastValue))
            .sorted(Comparator.comparingLong(o -> primaryKeysIdMap.get(o.columnIndex)))
            .map(bfo -> Transformer.buildParamByType(bfo.columnIndex, bfo.parameterMethod, bfo.lastValue))
            .collect(Collectors.toList());
    }

    /**
     * Build plan for physical select.
     *
     * @param params pk column value of last batch
     * @return built plan
     */
    private PhyTableOperation buildSelectPlanWithParam(String dbIndex, String phyTable, long batchSize,
                                                       List<ParameterContext> params, boolean withLowerBound,
                                                       boolean withUpperBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        // Get ExecutionPlan
        PhyTableOperation plan;
        if (!withLowerBound) {
            plan = new PhyTableOperation(planSelectWithMax);
        } else {
            plan = new PhyTableOperation(withUpperBound ? planSelectWithMinAndMax : planSelectWithMin);
        }

        int nextParamIndex = 2;

        // Parameters for where(DNF)
        final int pkNumber = params.size() / ((withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0));
        if (withLowerBound) {
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }
        if (withUpperBound) {
            final int base = withLowerBound ? pkNumber : 0;
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(base + j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(base + j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }

        // Parameters for limit
        planParams.put(nextParamIndex,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {nextParamIndex, batchSize}));

        plan.setDbIndex(dbIndex);
        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
        plan.setParam(planParams);
        return plan;
    }

    private static class Reporter {
        private final GsiBackfillManager backfillManager;

        /**
         * Extractor position mark
         */
        private GsiBackfillManager.BackfillBean backfillBean;

        /**
         * Calcualte backfill speed.
         */
        private long startTime = System.currentTimeMillis();
        private AtomicInteger taskCount = new AtomicInteger(0);
        private AtomicLong backfillCount = new AtomicLong(0);

        private Reporter(GsiBackfillManager backfillManager) {
            this.backfillManager = backfillManager;
        }

        public void loadBackfillMeta(long ddlJobId) {
            backfillBean = backfillManager.loadBackfillMeta(ddlJobId);
        }

        public GsiBackfillManager.BackfillBean getBackfillBean() {
            return backfillBean;
        }

        public void addBackfillCount(long count) {
            taskCount.incrementAndGet();
            backfillCount.addAndGet(count);
        }

        public void updateBackfillStatus(ExecutionContext ec, GsiBackfillManager.BackfillStatus status) {
            if (status == SUCCESS) {
                backfillBean.setProgress(100);

                // Log the backfill speed.
                double time = ((System.currentTimeMillis() - startTime) / 1000.0);
                double speed = backfillCount.get() / time;
                SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                    "[{0}] Backfill job: {1} total: {2} time: {3}s speed: {4}row/s with {5} task(s) PWQ total {6} thread(s).",
                    ec.getTraceId(), backfillBean.jobId, backfillCount.get(), time, speed, taskCount,
                    PriorityWorkQueue.getInstance().getPoolSize()));
            }
            backfillManager.updateLogicalBackfillObject(backfillBean, status);
        }

        public void updatePositionMark(ExecutionContext ec, List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                       long successRowCount, List<ParameterContext> lastPk,
                                       List<ParameterContext> beforeLastPk, boolean finished, Map<Long, Long> primaryKeysIdMap) {

            final GsiBackfillManager.BackfillStatus
                status =
                finished ? GsiBackfillManager.BackfillStatus.SUCCESS : GsiBackfillManager.BackfillStatus.RUNNING;
            final List<ParameterContext> pk = (finished && null == lastPk) ? beforeLastPk : lastPk;

            // Update position mark and status
            final Integer partial = backfillManager.updateBackfillObject(backfillObjects, pk, successRowCount, status, primaryKeysIdMap);

            assert backfillBean.backfillObjects != null;
            final GsiBackfillManager.BackfillObjectKey key = backfillObjects.get(0).key();
            final int objectsCount = backfillBean.backfillObjects.size();
            final AtomicInteger total = new AtomicInteger(partial);
            backfillBean.backfillObjects.forEach((k, v) -> {
                if (!k.equals(key)) {
                    total.addAndGet(v.get(0).progress);
                } else {
                    v.forEach(bfo -> bfo.setProgress(partial));
                }
            });

            final Integer progress = total.get() / objectsCount;

            // Update progress
            final int totalProgress;
            if (ec.getParamManager().getBoolean(ConnectionParams.GSI_CHECK_AFTER_CREATION)) {
                totalProgress = progress / 2;
            } else {
                totalProgress = progress;
            }
            backfillManager.updateLogicalBackfillProcess(String.valueOf(totalProgress), ec.getBackfillId());
        }

    }

    public Map<String, Set<String>> getSourcePhyTables() {
        return Maps.newHashMap();
    }

    public static class ExtractorInfo {
        TableMeta sourceTableMeta;
        List<String> targetTableColumns;
        List<String> primaryKeys;
        List<Integer> primaryKeysId;

        public ExtractorInfo(TableMeta sourceTableMeta, List<String> targetTableColumns, List<String> primaryKeys,
                             List<Integer> appearedKeysId) {
            this.sourceTableMeta = sourceTableMeta;
            this.targetTableColumns = targetTableColumns;
            this.primaryKeys = primaryKeys;
            this.primaryKeysId = appearedKeysId;
        }

        public TableMeta getSourceTableMeta() {
            return sourceTableMeta;
        }

        public List<String> getTargetTableColumns() {
            return targetTableColumns;
        }

        public List<String> getPrimaryKeys() {
            return primaryKeys;
        }

        public List<Integer> getPrimaryKeysId() {
            return primaryKeysId;
        }
    }

    public static ExtractorInfo buildExtractorInfo(ExecutionContext ec,
                                                   String schemaName,
                                                   String sourceTableName,
                                                   String targetTableName) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta sourceTableMeta = sm.getTable(sourceTableName);
        final TableMeta targetTableMeta = sm.getTable(targetTableName);
        final List<String> targetTableColumns = targetTableMeta.getWriteColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        Map<String, Integer> targetColumnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < targetTableColumns.size(); i++) {
            targetColumnMap.put(targetTableColumns.get(i), i);
        }
        // order reserved primary keys
        List<String> primaryKeys;
        if (sourceTableMeta.isHasPrimaryKey()) {
            primaryKeys = sourceTableMeta.getPrimaryIndex().getKeyColumns().stream().map(ColumnMeta::getName)
                .collect(Collectors.toList());
        } else {
            primaryKeys = new ArrayList<>();
        }
        // primary keys appeared in select list with reserved order
        List<String> appearedKeys = new ArrayList<>();
        List<Integer> appearedKeysId = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            if (targetColumnMap.containsKey(primaryKey)) {
                appearedKeys.add(primaryKey);
                appearedKeysId.add(targetColumnMap.get(primaryKey));
            }
        }

        return new ExtractorInfo(sourceTableMeta, targetTableColumns, appearedKeys, appearedKeysId);
    }
}
