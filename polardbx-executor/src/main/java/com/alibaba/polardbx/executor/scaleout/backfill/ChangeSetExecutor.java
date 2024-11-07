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

package com.alibaba.polardbx.executor.scaleout.backfill;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlSelect;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_BAD_PACKET;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_COUNT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_WAIT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;
import static com.alibaba.polardbx.executor.gsi.utils.Transformer.buildColumnParam;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_BACKFILL_EXCEPTION;

public class ChangeSetExecutor extends Extractor {
    private final Map<String, Set<String>> sourcePhyTables;
    private final PhyTableOperation planSelect;

    protected ChangeSetExecutor(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                                long speedMin, long speedLimit, long parallelism, boolean useBinary,
                                List<String> modifyStringColumns,
                                PhyTableOperation planSelectWithMax,
                                PhyTableOperation planSelectWithMin,
                                PhyTableOperation planSelectWithMinAndMax,
                                PhyTableOperation planSelect,
                                PhyTableOperation planSelectMaxPk,
                                PhyTableOperation planSelectSample,
                                List<Integer> primaryKeysId,
                                Map<String, Set<String>> sourcePhyTables) {
        super(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit, parallelism, useBinary,
            modifyStringColumns, planSelectWithMax, planSelectWithMin, planSelectWithMinAndMax, planSelectMaxPk,
            planSelectSample, primaryKeysId);
        this.sourcePhyTables = sourcePhyTables;
        this.planSelect = planSelect;
    }

    @Override
    protected void foreachPhyTableBatch(String dbIndex, String phyTable,
                                        List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                        ExecutionContext ec,
                                        BatchConsumer loader,
                                        AtomicReference<Boolean> interrupted) {
        AtomicLong actualBatchSize = new AtomicLong(batchSize);

        GsiUtils.retryOnException(
            // 1. Fetch rows within trx1 (single db transaction)
            // 2. Fill into index table within trx2 (XA transaction)
            () -> GsiUtils.wrapWithSingleDbTrx(tm, ec,
                (selectEc) -> extract(dbIndex, phyTable, selectEc, actualBatchSize, loader, backfillObjects,
                    interrupted)),
            e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT)
                || e.getErrorCode() == ERR_X_PROTOCOL_BAD_PACKET.getCode()
                || (e.getErrorCode() == 1153 && e.getMessage().toLowerCase().contains("max_allowed_packet"))
                || (e.getSQLState() != null && e.getSQLState().equalsIgnoreCase("S1000")
                && e.getMessage().toLowerCase().contains("max_allowed_packet"))
                && actualBatchSize.get() > 1),
            (e, retryCount) -> changesetErrConsumer(dbIndex, phyTable, ec, e, retryCount, actualBatchSize));
    }

    protected PhyTableOperation buildSelectPlanWithParamChangeSet(String dbIndex, String phyTable,
                                                                  List<ParameterContext> params,
                                                                  boolean withLowerBound,
                                                                  boolean withUpperBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

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

        PhyTableOperation targetPhyOp =
            !withLowerBound ? planSelectWithMax : (withUpperBound ? planSelectWithMinAndMax : planSelectWithMin);
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(planParams);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
    }

    protected PhyTableOperation buildSelectPlanWithParam(String dbIndex, String phyTable,
                                                         List<ParameterContext> params, boolean withLowerBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        int nextParamIndex = 2;
        if (withLowerBound) {
            for (int i = 0; i < params.size(); ++i) {
                planParams.put(nextParamIndex,
                    new ParameterContext(params.get(i).getParameterMethod(),
                        new Object[] {nextParamIndex, params.get(i).getArgs()[1]}));
                nextParamIndex++;
            }
        }

        // Get ExecutionPlan
        PhyTableOperation targetPhyOp = withLowerBound ? planSelectWithMin : planSelect;

        // Get ExecutionPlan
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(planParams);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
    }

    protected void changesetErrConsumer(String dbIndex, String phyTable, ExecutionContext ec,
                                        TddlNestableRuntimeException e, int retryCount, AtomicLong actualBatchSize) {
        if (!(GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
            || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT))) {
            // adjust actualBatchSize
            actualBatchSize.set(Math.max(actualBatchSize.get() / 4, 1));
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] {1}[{2}] adjust actualBatchSize to[{3}] retry count[{4}]: {5}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                actualBatchSize,
                retryCount,
                e.getMessage()));
            return;
        }

        if (retryCount < RETRY_COUNT) {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while extracting backfill(changeset) data from {1}[{2}] retry count[{3}]: {4}",
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
                "[{0}] Deadlock found while extracting backfill(changeset) data from {1}[{2}] throw: {3}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                e.getMessage()));

            throw GeneralUtil.nestedException(e);
        }
    }

    protected List<Map<Integer, ParameterContext>> extract(String dbIndex, String phyTable,
                                                           ExecutionContext extractEc,
                                                           AtomicLong actualBatchSize,
                                                           BatchConsumer next,
                                                           List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                                           AtomicReference<Boolean> interrupted) {
        String physicalTableName = TddlSqlToRelConverter.unwrapPhysicalTableName(phyTable);

        // Load upper bound
        List<ParameterContext> upperBoundParam =
            buildUpperBoundParam(backfillObjects.size(), backfillObjects, primaryKeysIdMap);
        final boolean withUpperBound = GeneralUtil.isNotEmpty(upperBoundParam);

        // Init historical position mark
        List<ParameterContext> lastPk = initSelectParam(backfillObjects, primaryKeysIdMap);

        // build select plan and parameters
        PhyTableOperation extractPlan;
        if (extractEc.getParamManager().getInt(ConnectionParams.PHYSICAL_TABLE_BACKFILL_PARALLELISM) <= 1) {
            extractPlan =
                buildSelectPlanWithParam(dbIndex, physicalTableName, lastPk, GeneralUtil.isNotEmpty(lastPk));
        } else {
            extractPlan = buildSelectPlanWithParamChangeSet(dbIndex,
                physicalTableName,
                Stream.concat(lastPk.stream(), upperBoundParam.stream()).collect(Collectors.toList()),
                GeneralUtil.isNotEmpty(lastPk),
                withUpperBound);
        }

        Cursor extractCursor = null;
        try {
            // Extract
            extractCursor = ExecutorHelper.execute(extractPlan, extractEc);

            buildBatchParamAndLoad(extractCursor, dbIndex, phyTable, extractPlan, extractEc, backfillObjects, next,
                interrupted, actualBatchSize);
        } finally {
            if (extractCursor != null) {
                extractCursor.close(new ArrayList<>());
            }
            extractEc.getTransaction().commit();
        }

        return null;
    }

    public void buildBatchParamAndLoad(Cursor cursor, String dbIndex, String phyTable,
                                       PhyTableOperation extractPlan, ExecutionContext extractEc,
                                       List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                       BatchConsumer next, AtomicReference<Boolean> interrupted,
                                       AtomicLong actualBatchSize) {
        String physicalTableName = TddlSqlToRelConverter.unwrapPhysicalTableName(phyTable);
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
        Map<Integer, ParameterContext> lastParam = null;

        // Init historical position mark
        long successRowCount = backfillObjects.get(0).successRowCount;
        List<ParameterContext> lastPk = initSelectParam(backfillObjects, primaryKeysIdMap);

        Row row;
        boolean finished = false;
        long rangeBackfillStartTime = System.currentTimeMillis();
        do {
            if (rateLimiter != null) {
                rateLimiter.acquire(actualBatchSize.intValue());
            }
            long start = System.currentTimeMillis();

            // Dynamic adjust lower bound of rate.
            final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
            if (dynamicRate > 0) {
                throttle.resetMaxRate(dynamicRate);
            }

            for (long j = 0; j < actualBatchSize.get() && (row = cursor.next()) != null; ++j) {
                final List<ColumnMeta> columns = row.getParentCursorMeta().getColumns();

                final Map<Integer, ParameterContext> params = new HashMap<>(columns.size());
                for (int i = 0; i < columns.size(); i++) {
                    ColumnMeta columnMeta = columns.get(i);
                    String colName = columnMeta.getName();
                    boolean canConvert =
                        useBinary && (notConvertColumns == null || !notConvertColumns.contains(colName));

                    final ParameterContext parameterContext = buildColumnParam(row, i, canConvert);

                    params.put(i + 1, parameterContext);
                }
                batchParams.add(params);
            }

            finished = batchParams.size() != actualBatchSize.get();

            // Load
            next.consume(batchParams, Pair.of(extractEc, Pair.of(extractPlan.getDbIndex(), physicalTableName)));

            FailPoint.injectRandomExceptionFromHint(FP_RANDOM_BACKFILL_EXCEPTION, extractEc);

            successRowCount += batchParams.size();
            lastParam = batchParams.isEmpty() ? null : batchParams.get(batchParams.size() - 1);
            statsAddBackFillRows(extractEc, backfillObjects, batchParams, successRowCount, start, lastPk, extractPlan,
                phyTable, actualBatchSize.get());

            // Check DDL is ongoing.
            if (CrossEngineValidator.isJobInterrupted(extractEc) || Thread.currentThread().isInterrupted()
                || interrupted.get()) {
                long jobId = extractEc.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }

            // for sliding window of split
            checkAndSplitBackfillObject(dbIndex, phyTable, successRowCount, extractEc, rangeBackfillStartTime,
                batchParams, backfillObjects);

            // adjust batch size
            actualBatchSize.set(Math.min(actualBatchSize.get() * 2, batchSize));

            // clear batchParams
            batchParams.clear();
        } while (!finished);

//        DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set(0);
        reporter.addBackfillCount(successRowCount);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Last backfill row for {1}[{2}][{3}]: {4}",
            extractEc.getTraceId(),
            dbIndex,
            phyTable,
            successRowCount,
            GsiUtils.rowToString(lastParam)));
    }

    public void statsAddBackFillRows(ExecutionContext ec, List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                     List<Map<Integer, ParameterContext>> lastBatch, long successRowCount, long start,
                                     List<ParameterContext> lastPk, PhyTableOperation extractPlan,
                                     String phyTableName, Long actualBatchSize) {
        if (lastBatch == null || lastBatch.isEmpty()) {
            return;
        }

        // For status recording
        List<ParameterContext> beforeLastPk = lastPk;

        // Build parameter for next batch
        lastPk = buildSelectParam(lastBatch, primaryKeysId);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("backfill batch last row for {0}[{1}][{2}]: {3}",
            extractPlan.getDbIndex(),
            phyTableName,
            successRowCount,
            GsiUtils.rowToString(lastBatch.get(lastBatch.size() - 1))));

        boolean finished = lastBatch.size() != actualBatchSize;

        // Update position mark
        reporter.updatePositionMark(ec, backfillObjects, successRowCount, lastPk, beforeLastPk, finished,
            primaryKeysIdMap);
        ec.getStats().backfillRows.addAndGet(lastBatch.size());
        DdlEngineStats.METRIC_BACKFILL_ROWS_FINISHED.update(lastBatch.size());

        if (!finished) {
            throttle.feedback(new com.alibaba.polardbx.executor.backfill.Throttle.FeedbackStats(
                System.currentTimeMillis() - start, start, lastBatch.size()));
        }
//        DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set((long) throttle.getActualRateLastCycle());

        if (rateLimiter != null) {
            // Limit rate.
            rateLimiter.setRate(throttle.getNewRate());
        }
    }

    public static Extractor create(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                                   long speedMin, long speedLimit, long parallelism, boolean useBinary,
                                   List<String> modifyStringColumns, Map<String, Set<String>> sourcePhyTables,
                                   boolean onlineModifyColumn, ExecutionContext ec) {
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, useBinary, modifyStringColumns, ec);

        ExtractorInfo info =
            Extractor.buildExtractorInfo(ec, schemaName, sourceTableName, targetTableName, false, false,
                onlineModifyColumn);

        SqlSelect.LockMode lockMode = SqlSelect.LockMode.UNDEF;

        return new ChangeSetExecutor(schemaName,
            sourceTableName,
            targetTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            modifyStringColumns,
            builder.buildSelectForBackfillNotLimit(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false,
                true,
                lockMode),
            builder.buildSelectForBackfillNotLimit(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true,
                false,
                lockMode),
            builder.buildSelectForBackfillNotLimit(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true,
                true,
                lockMode),
            builder.buildSelectForBackfillNotLimit(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false,
                false,
                lockMode),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys()),
            info.getPrimaryKeysId(),
            sourcePhyTables);
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        return sourcePhyTables;
    }
}
