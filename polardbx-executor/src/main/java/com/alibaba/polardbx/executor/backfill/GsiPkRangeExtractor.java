package com.alibaba.polardbx.executor.backfill;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.BackfillExtraFieldJSON;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlSelect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus.UNFINISHED;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_BACKFILL_EXCEPTION;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class GsiPkRangeExtractor extends Extractor {

    private boolean needBuildSubBoundList = true;

    private Map<String, List<Map<Integer, ParameterContext>>> backfillSubBoundList = new HashMap<>();

    static private final Integer maxRandomInterval = 10000;

    protected Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> pkRange;
    protected TableMeta targetTableMeta;
    protected int estimatedBatchSize;

    public static Extractor create(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                                   long speedMin, long speedLimit, long parallelism, boolean useBinary,
                                   List<String> modifyStringColumns,
                                   Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> pkRange,
                                   ExecutionContext ec) {
        Extractor.ExtractorInfo
            info = Extractor.buildExtractorInfo(ec, schemaName, sourceTableName, targetTableName, true);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, useBinary, modifyStringColumns, ec);

        TableMeta targetTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(targetTableName);
        Boolean lockRead = ec.getParamManager().getBoolean(ConnectionParams.GSI_PK_RANGE_LOCK_READ);
        SqlSelect.LockMode lockMode = null;
        if (lockRead) {
            lockMode = SqlSelect.LockMode.SHARED_LOCK;
        }
        return new GsiPkRangeExtractor(schemaName,
            sourceTableName,
            targetTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            modifyStringColumns,
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, false, lockMode, null, true, true),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, true, lockMode, null, true, true),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, false,
                lockMode, null, true, true),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, true,
                lockMode, null, true, true),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys()),
            info.getPrimaryKeysId(),
            pkRange,
            targetTableMeta,
            (int) ec.getEstimatedBackfillBatchRows());
    }

    public GsiPkRangeExtractor(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                               long speedMin, long speedLimit, long parallelism, boolean useBinary,
                               List<String> modifyStringColumns,
                               PhyTableOperation planSelectWithoutMinAndMax,
                               PhyTableOperation planSelectWithMax, PhyTableOperation planSelectWithMin,
                               PhyTableOperation planSelectWithMinAndMax,
                               PhyTableOperation planSelectMaxPk,
                               PhyTableOperation planSelectSample,
                               List<Integer> primaryKeysId,
                               Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> pkRange,
                               TableMeta targetTableMeta, int estimatedBatchSize) {
        super(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit, parallelism, false,
            null, planSelectWithMax,
            planSelectWithMin, planSelectWithMinAndMax, planSelectMaxPk, null, primaryKeysId);
        this.pkRange = pkRange;
        this.targetTableMeta = targetTableMeta;
        this.estimatedBatchSize = estimatedBatchSize;
        this.planSelectWithoutMinAndMax = planSelectWithoutMinAndMax;
    }

    @Override
    protected List<GsiBackfillManager.BackfillObjectRecord> initAllUpperBound(ExecutionContext ec, long ddlJobId,
                                                                              long taskId) {
        return splitAndInitUpperBound(ec, ddlJobId, taskId, primaryKeysId, pkRange);
    }

    protected List<GsiBackfillManager.BackfillObjectRecord> splitAndInitUpperBound(final ExecutionContext baseEc,
                                                                                   final long ddlJobId,
                                                                                   final long taskId,
                                                                                   final List<Integer> primaryKeysId,
                                                                                   final Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> pkRange) {

        // TODO:(yijin), split and init upper bound by sample result, for example, each range should contains only 16MB sample,
        // and we need to assign the approximateRowCount
        // we will optimize this after profile.

        Long approximateRowCount = (long) estimatedBatchSize;
        // convert pkRange to BackfillObjectBound
        Map<Integer, ParameterContext> lowerBound = pkRange.getKey() != null ? pkRange.getKey() : new HashMap<>();
        Map<Integer, ParameterContext> upperBound = pkRange.getValue() != null ? pkRange.getValue() : new HashMap<>();

        return genBackfillObjectRecordByUpperBound(ddlJobId, taskId, lowerBound, upperBound
            , approximateRowCount, 1);
    }

    private List<GsiBackfillManager.BackfillObjectRecord> genBackfillObjectRecordByUpperBound(final long ddlJobId,
                                                                                              final long taskId,
                                                                                              Map<Integer, ParameterContext> lowerBound,
                                                                                              Map<Integer, ParameterContext> upperBound,
                                                                                              long approximateRowCount,
                                                                                              int splitLevel) {
        List<GsiBackfillManager.BackfillObjectRecord> backfillBoundRecords = new ArrayList<>();

        // Convert to BackfillObjectRecord
        BackfillExtraFieldJSON extra = new BackfillExtraFieldJSON();
        extra.setApproximateRowCount(String.valueOf(approximateRowCount));
        extra.setSplitLevel(String.valueOf(splitLevel));
        extra.setLogical(false);
        // TODO: split by lastItem

        for (int i = 0; i < primaryKeysId.size(); ++i) {
            int columnIndex = primaryKeysId.get(i);
            String paramMethod = null;
            if (!lowerBound.isEmpty() || !upperBound.isEmpty()) {
                paramMethod = lowerBound.isEmpty() ? upperBound.get(i).getParameterMethod().toString() :
                    lowerBound.get(i).getParameterMethod().toString();
            }
            backfillBoundRecords.add(GsiUtils.buildBackfillObjectRecord(ddlJobId,
                taskId,
                schemaName,
                sourceTableName,
                targetTableName,
                //for logical level record.
                "",
                "",
                columnIndex,
                paramMethod,
                Transformer.serializeParam(lowerBound.get(i)),
                Transformer.serializeParam(upperBound.get(i)),
                BackfillExtraFieldJSON.toJson(extra)));
        }
        return backfillBoundRecords;
    }

    /**
     * Lock and read batch from logical table, feed them to consumer
     *
     * @param ec base execution context
     * @param consumer next step
     */
    @Override
    public void foreachBatch(ExecutionContext ec,
                             BatchConsumer consumer) {
        // For each physical table there is a backfill object which represents a row in
        // system table

        // set KILL_CLOSE_STREAM = true
        ec.getExtraCmds().put(ConnectionParams.KILL_CLOSE_STREAM.toString(), true);
        // set RC
        ec.setTxIsolation(Connection.TRANSACTION_READ_COMMITTED);

        // interrupted
        AtomicReference<Boolean> interrupted = new AtomicReference<>(false);

        List<Future> futures = new ArrayList<>(16);

        List<List<GsiBackfillManager.BackfillObjectBean>> tasks = reporter.getBackfillBean().backfillObjects.values()
            .stream()
            .filter(v -> v.get(0).status.is(UNFINISHED))
            .collect(Collectors.toList());
        Collections.shuffle(tasks);

        if (!tasks.isEmpty()) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] Backfill job: {1} start with {2} task(s) parallelism {3}.",
                    ec.getTraceId(), tasks.get(0).get(0).jobId, tasks.size(), parallelism));
        } else {
            ec.setEstimatedBackfillBatchRows(-1);
        }

        AtomicReference<Exception> excep = new AtomicReference<>(null);
        if (parallelism <= 0 || parallelism >= BackFillThreadPool.getInstance().getCorePoolSize()) {
            // Full queued.
            tasks.forEach(v -> {
                FutureTask<Void> task = new FutureTask<>(
                    () -> foreachPhyTableBatch(Optional.ofNullable(v.get(0).physicalDb).orElse(""),
                        Optional.ofNullable(v.get(0).physicalTable).orElse(""), v, ec, consumer,
                        interrupted), null);
                futures.add(task);
                BackFillThreadPool.getInstance()
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
                            foreachPhyTableBatch(v.get(0).physicalDb, v.get(0).physicalTable, v, ec, consumer,
                                interrupted);
                        } finally {
                            // Poll in finally to prevent dead lock on putting blockingQueue.
                            blockingQueue.poll();
                        }
                        return null;
                    });
                    futures.add(task);
                    BackFillThreadPool.getInstance()
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
                if (null == excep.get()) {
                    excep.set(e);
                }

                // set interrupt
                interrupted.set(true);
            }
        }

        if (excep.get() != null) {
            if (!excep.get().getMessage().contains("need to be split into smaller batches")) {
                throttle.stop();
            } else {
                SQLRecorderLogger.ddlLogger.warn("all backfill feature task was interrupted by split");
            }
            throw GeneralUtil.nestedException(excep.get());
        }

        throttle.stop();

        // After all physical table finished
        reporter.updateBackfillStatus(ec, GsiBackfillManager.BackfillStatus.SUCCESS);
    }

    /**
     * Lock and read batch from physical table, feed them to consumer
     *
     * @param dbIndex physical db key actually useless...
     * @param phyTable physical table name actually useless...
     * @param ec base execution context
     * @param loader next step
     */
    @Override
    protected void foreachPhyTableBatch(String dbIndex, String phyTable,
                                        List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                        ExecutionContext ec,
                                        BatchConsumer loader,
                                        AtomicReference<Boolean> interrupted) {
        String physicalTableName = TddlSqlToRelConverter.unwrapPhysicalTableName(phyTable);

        // Load upper bound
        List<ParameterContext> upperBoundParam =
            buildUpperBoundParam(backfillObjects.size(), backfillObjects, primaryKeysIdMap);
        final boolean withUpperBound = GeneralUtil.isNotEmpty(upperBoundParam);

        // Init historical position mark
        long successRowCount = backfillObjects.get(0).successRowCount;
        List<ParameterContext> lastPk = initSelectParam(backfillObjects, primaryKeysIdMap);

        long rangeBackfillStartTime = System.currentTimeMillis();

        BackfillSampleManager backfillSampleManager = BackfillSampleManager.loadBackfillSampleManager(ec.getTaskId());
        List<Map<Integer, ParameterContext>> lastBatch = null;
        boolean finished = false;
        long actualBatchSize = batchSize;
        IServerConfigManager serverConfigManager = GsiUtils.getServerConfigManager();
        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] [{1}][{2}] load pk range start with ({3})",
            ec.getTraceId(),
            String.valueOf(ec.getTaskId()),
            String.valueOf(ec.getBackfillId()),
            GsiUtils.rowToString(lastPk)));
        do {
            try {
                if (rateLimiter != null) {
                    rateLimiter.acquire((int) actualBatchSize);
                }
                long start = System.currentTimeMillis();

                // Dynamic adjust lower bound of rate.
                final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
                if (dynamicRate > 0) {
                    throttle.resetMaxRate(dynamicRate);
                }

                // For next batch, build select plan and parameters
                final PhyTableOperation selectPlan = buildSelectWithParam(
                    schemaName,
                    sourceTableName,
                    true,
                    actualBatchSize,
                    Stream.concat(lastPk.stream(), upperBoundParam.stream()).collect(Collectors.toList()),
                    GeneralUtil.isNotEmpty(lastPk),
                    withUpperBound,
                    planSelectWithoutMinAndMax,
                    planSelectWithMax,
                    planSelectWithMin,
                    planSelectWithMinAndMax);

                List<ParameterContext> finalLastPk = lastPk;
                lastBatch = GsiUtils.retryOnException(
                    // 1. Lock rows within trx1 (distributed db transaction)
                    // 2. Fill into index table within trx2 (XA transaction)
                    // 3. Trx1 commit, if (success) {trx2 commit} else {trx2 rollback}
                    () -> GsiUtils.wrapWithDistributedTrxForPkExtractor(tm, ec,
                        serverConfigManager,
                        (ecAndConn) -> {
                            try {
                                return extract(dbIndex, physicalTableName, selectPlan, ecAndConn, loader, finalLastPk,
                                    upperBoundParam);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }),
                    e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                        || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT))
                        || e.getMessage().contains("Loader check error."),
                    // keep it as before, no need to change.
                    (e, retryCount) -> deadlockErrConsumer(selectPlan, ec, e, retryCount),
                    ec);

                // For status recording
                List<ParameterContext> beforeLastPk = lastPk;

                // Build parameter for next batch
                lastPk = buildSelectParam(lastBatch, primaryKeysId);

                finished = lastBatch.size() != actualBatchSize;

                // Update position mark
                successRowCount += lastBatch.size();

                SQLRecorderLogger.ddlLogger.warn(
                    MessageFormat.format(
                        "[{0}] [{1}][{2}] pk range last backfill success count and pks: [{3}], ({4}), finished is [{5}]",
                        ec.getTraceId(),
                        String.valueOf(ec.getTaskId()),
                        String.valueOf(ec.getBackfillId()),
                        successRowCount,
                        GsiUtils.rowToString(lastPk), finished));
//                backfillSampleManager.updatePositionMark(ec.getBackfillId(), reporter, ec, backfillObjects,
//                    successRowCount, lastPk, beforeLastPk, finished, primaryKeysIdMap);
//                SQLRecorderLogger.ddlLogger.warn(
//                    MessageFormat.format(
//                        "[{0}] [{1}][{2}] pk range last backfill success count and pks: [{3}], ({4}), finished is [{5}]",
//                        ec.getTraceId(),
//                        String.valueOf(ec.getTaskId()),
//                        String.valueOf(ec.getBackfillId()),
//                        successRowCount,
//                        GsiUtils.rowsToString(lastBatch), finished));
                reporter.updatePositionMark(ec, backfillObjects, successRowCount, lastPk, beforeLastPk, finished,
                    primaryKeysIdMap);
                ec.getStats().backfillRows.addAndGet(lastBatch.size());
                DdlEngineStats.METRIC_BACKFILL_ROWS_FINISHED.update(lastBatch.size());

                if (!finished) {
                    throttle.feedback(new Throttle.FeedbackStats(
                        System.currentTimeMillis() - start, start, lastBatch.size()));
                }
//                DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set((long) throttle.getActualRateLastCycle());

                if (rateLimiter != null) {
                    // Limit rate.
                    rateLimiter.setRate(throttle.getNewRate());
                }

                // Check DDL is ongoing.
                if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()
                    || interrupted.get()) {
                    long jobId = ec.getDdlJobId();
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        "The job '" + jobId + "' has been cancelled");
                }
                if (actualBatchSize < batchSize) {
                    actualBatchSize = Math.min(actualBatchSize * 2, batchSize);
                }
            } catch (TddlRuntimeException e) {
                boolean retry = (e.getErrorCode() == ErrorCode.ERR_X_PROTOCOL_BAD_PACKET.getCode() ||
                    (e.getErrorCode() == 1153 && e.getMessage().toLowerCase().contains("max_allowed_packet")) ||
                    (e.getSQLState() != null && e.getSQLState().equalsIgnoreCase("S1000") && e.getMessage()
                        .toLowerCase().contains("max_allowed_packet"))) && actualBatchSize > 1;
                if (retry) {
                    actualBatchSize = Math.max(actualBatchSize / 8, 1);
                } else {
                    throw e;
                }
            }

            // yijin:disable sliding window of split, because we don't need this
//            checkAndSplitBackfillObject(
//                dbIndex, phyTable, successRowCount, ec, rangeBackfillStartTime, lastBatch, backfillObjects);
        } while (!finished);

//        DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set(0);
        reporter.addBackfillCount(successRowCount);

        SQLRecorderLogger.ddlLogger.warn(
            MessageFormat.format("[{0}] [{1}][{2}] pk range last backfill success count and rows: [{3}], ({4})",
                ec.getTraceId(),
                String.valueOf(ec.getTaskId()),
                String.valueOf(ec.getBackfillId()),
                successRowCount,
                GsiUtils.rowToString(lastBatch.isEmpty() ? null : lastBatch.get(lastBatch.size() - 1))));
    }

    protected List<Map<Integer, ParameterContext>> extract(String dbIndex, String phyTableName,
                                                           PhyTableOperation logicalSelectSql,
                                                           Pair<ExecutionContext, Connection> ecAndConn,
                                                           BatchConsumer next,
                                                           List<ParameterContext> lowerBound,
                                                           List<ParameterContext> upperBound) throws SQLException {

        Pair<ExecutionContext, Connection> extractEcAndConn = (Pair<ExecutionContext, Connection>) ecAndConn;
        ExecutionContext extractEc = extractEcAndConn.getKey();
        Connection connection = extractEcAndConn.getValue();
        // Transform
        final List<Map<Integer, ParameterContext>> result;

        ResultSet resultSet = null;
        String hint = String.format(" /* trace_id(%s) */ ", extractEc.getTraceId());
        // TODO(yijin): support hint passed to logical sql.
//        ParamManager pm = extractEc.getParamManager();
//        String mergeUnionSize = ConnectionProperties.MERGE_UNION_SIZE;
//        String hint =
//            String.format("/*+TDDL:cmd_extra(%s=%d)*/", mergeUnionSize, pm.getInt(ConnectionParams.MERGE_UNION_SIZE));
        // Extract

        try (PreparedStatement statement = connection.prepareStatement(hint + logicalSelectSql.getNativeSql())) {
            ParameterMethod.setParameters(statement, logicalSelectSql.getParam());
            resultSet = statement.executeQuery();

            result =
                com.alibaba.polardbx.executor.gsi.utils.Transformer.buildBatchParam(targetTableMeta.getAllColumns(),
                    resultSet, useBinary,
                    notConvertColumns);
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format(
                    "[{0}] [{1}][{2}] select for update get row count [{3}], pk range is [{4}], [{5}], param is {6}",
                    extractEc.getTraceId(),
                    extractEc.getTaskId().toString(),
                    extractEc.getBackfillId().toString(),
                    result.size(),
                    result.isEmpty() ? "" : GsiUtils.rowToString(result.get(0)),
                    result.isEmpty() ? "" : GsiUtils.rowToString(result.get(result.size() - 1)),
                    GsiUtils.rowToString(logicalSelectSql.getParam())));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            // TODO: finally end.
        }
        if (resultSet != null) {
            resultSet.close();
        }

        FailPoint.injectRandomExceptionFromHint(FP_RANDOM_BACKFILL_EXCEPTION, extractEc);

        // Load
        next.consume(result, Pair.of(extractEc, Pair.of(dbIndex, phyTableName)));

        return result;
    }

    /**
     * Build plan for physical select.
     *
     * @param params pk column value of last batch
     * @return built plan
     */
    public static PhyTableOperation buildSelectWithParam(String tableSchema, String tableName,
                                                         Boolean isLogical,
                                                         long batchSize,
                                                         List<ParameterContext> params, boolean withLowerBound,
                                                         boolean withUpperBound,
                                                         PhyTableOperation planSelectWithoutMinAndMax,
                                                         PhyTableOperation planSelectWithMax,
                                                         PhyTableOperation planSelectWithMin,
                                                         PhyTableOperation planSelectWithMinAndMax) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        int nextParamIndex = 1;
        if (isLogical) {
            // this is for logical table, we would fill this later
        } else {
            planParams.put(nextParamIndex, PlannerUtils.buildParameterContextForTableName(tableName, 1));
            nextParamIndex++;
        }

        // Parameters for where(DNF)
        int pkNumber = 0;
        PhyTableOperation targetPhyOp = planSelectWithoutMinAndMax;
        if (withLowerBound || withUpperBound) {
            pkNumber = params.size() / ((withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0));
            targetPhyOp =
                !withLowerBound ? planSelectWithMax : (withUpperBound ? planSelectWithMinAndMax : planSelectWithMin);
        }
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

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(tableSchema);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(tableName)));
        buildParams.setDynamicParams(planParams);
//        select * from t1 where a < 1 and a > 0 order by a for update;

        PhyTableOperation phyTableOperation =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
        return phyTableOperation;
    }
}