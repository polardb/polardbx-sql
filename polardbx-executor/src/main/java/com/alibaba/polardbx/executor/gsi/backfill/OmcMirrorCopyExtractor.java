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

package com.alibaba.polardbx.executor.gsi.backfill;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;

/**
 * @author wumu
 */
public class OmcMirrorCopyExtractor extends Extractor {
    private final Map<String, String> tableNameMapping;
    private final Map<String, Set<String>> sourcePhyTables;

    private final PhyTableOperation planInsertSelectWithMin;
    private final PhyTableOperation planInsertSelectWithMax;
    private final PhyTableOperation planInsertSelectWithMinAndMax;

    final List<String> tableColumns;
    final List<String> primaryKeys;
    final Set<Integer> selectKeySet;

    public OmcMirrorCopyExtractor(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                                  long speedMin,
                                  long speedLimit,
                                  long parallelism,
                                  boolean useBinary,
                                  PhyTableOperation planSelectBatchWithMax,
                                  PhyTableOperation planSelectBatchWithMin,
                                  PhyTableOperation planSelectBatchWithMinAndMax,
                                  PhyTableOperation planSelectMaxPk,
                                  PhyTableOperation planSelectSample,
                                  PhyTableOperation planInsertSelectWithMax,
                                  PhyTableOperation planInsertSelectWithMin,
                                  PhyTableOperation planInsertSelectWithMinAndMax,
                                  List<Integer> primaryKeysId,
                                  List<String> primaryKeys, List<String> tableColumns,
                                  Map<String, String> tableNameMapping,
                                  Map<String, Set<String>> sourcePhyTables) {
        super(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit, parallelism, useBinary,
            null, planSelectBatchWithMax, planSelectBatchWithMin, planSelectBatchWithMinAndMax, planSelectMaxPk,
            planSelectSample, primaryKeysId);

        this.planInsertSelectWithMin = planInsertSelectWithMin;
        this.planInsertSelectWithMax = planInsertSelectWithMax;
        this.planInsertSelectWithMinAndMax = planInsertSelectWithMinAndMax;

        this.primaryKeys = primaryKeys;
        this.tableColumns = tableColumns;
        this.tableNameMapping = tableNameMapping;
        this.sourcePhyTables = sourcePhyTables;
        this.selectKeySet = new HashSet<>(primaryKeysId);
    }

    public static OmcMirrorCopyExtractor create(String schemaName, String sourceTableName, String targetTableName,
                                                long batchSize, long speedMin, long speedLimit, long parallelism,
                                                Map<String, String> tableNameMapping,
                                                Map<String, Set<String>> sourcePhyTables,
                                                boolean useChangeSet, boolean useBinary, boolean onlineModifyColumn,
                                                ExecutionContext ec) {
        Extractor.ExtractorInfo info =
            Extractor.buildExtractorInfo(ec, schemaName, sourceTableName, targetTableName, true, false,
                onlineModifyColumn);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, useBinary, ec);

        final TableMeta tableMeta = info.getSourceTableMeta();
        final List<String> tableColumns = tableMeta.getWriteColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        List<String> targetTableColumns = info.getRealTargetTableColumns();
        List<String> sourceTableColumns = info.getTargetTableColumns();
        List<String> primaryKeys = info.getPrimaryKeys();

        SqlSelect.LockMode lockMode = useChangeSet ? SqlSelect.LockMode.UNDEF : SqlSelect.LockMode.SHARED_LOCK;
        boolean isInsertIgnore = !useChangeSet;

        return new OmcMirrorCopyExtractor(schemaName,
            sourceTableName,
            targetTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            builder.buildSelectUpperBoundForInsertSelectBackfill(info.getSourceTableMeta(),
                info.getTargetTableColumns(), info.getPrimaryKeys(),
                false, true),
            builder.buildSelectUpperBoundForInsertSelectBackfill(info.getSourceTableMeta(),
                info.getTargetTableColumns(), info.getPrimaryKeys(),
                true, false),
            builder.buildSelectUpperBoundForInsertSelectBackfill(info.getSourceTableMeta(),
                info.getTargetTableColumns(), info.getPrimaryKeys(),
                true, true),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys()),
            builder.buildInsertSelectForOMCBackfill(tableMeta, targetTableColumns, sourceTableColumns,
                primaryKeys, false, true, lockMode, isInsertIgnore),
            builder.buildInsertSelectForOMCBackfill(tableMeta, targetTableColumns, sourceTableColumns,
                primaryKeys, true, false, lockMode, isInsertIgnore),
            builder.buildInsertSelectForOMCBackfill(tableMeta, targetTableColumns, sourceTableColumns,
                primaryKeys, true, true, lockMode, isInsertIgnore),
            info.getPrimaryKeysId(),
            primaryKeys,
            tableColumns,
            tableNameMapping,
            sourcePhyTables
        );
    }

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
        AtomicReference<Long> currentSuccessRowCount = new AtomicReference<>(0L);
        List<ParameterContext> lastPk = initSelectParam(backfillObjects, primaryKeysIdMap);

        long rangeBackfillStartTime = System.currentTimeMillis();

        List<Map<Integer, ParameterContext>> lastBatch = null;
        AtomicReference<Boolean> finished = new AtomicReference<>(false);
        long actualBatchSize = batchSize;
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
                final PhyTableOperation selectPlan = buildSelectPlanWithParam(dbIndex,
                    physicalTableName,
                    actualBatchSize,
                    Stream.concat(lastPk.stream(), upperBoundParam.stream()).collect(Collectors.toList()),
                    GeneralUtil.isNotEmpty(lastPk),
                    withUpperBound);

                List<ParameterContext> finalLastPk = lastPk;
                lastBatch = GsiUtils.retryOnException(
                    // 1. Lock rows within trx1 (single db transaction)
                    // 2. Fill into index table within trx2 (XA transaction)
                    // 3. Trx1 commit, if (success) {trx2 commit} else {trx2 rollback}
                    () -> GsiUtils.wrapWithSingleDbTrx(tm, ec,
                        (selectEc) -> extract(dbIndex, physicalTableName, selectPlan, selectEc, finalLastPk,
                            upperBoundParam, currentSuccessRowCount, finished)),
                    e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                        || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT))
                        || e.getMessage().contains("Loader check error."),
                    (e, retryCount) -> deadlockErrConsumer(selectPlan, ec, e, retryCount));

                // For status recording
                List<ParameterContext> beforeLastPk = lastPk;

                // Build parameter for next batch
                lastPk = buildSelectParam(lastBatch, primaryKeysId);

                successRowCount += currentSuccessRowCount.get();

                reporter.updatePositionMark(ec, backfillObjects, successRowCount, lastPk, beforeLastPk,
                    finished.get(), primaryKeysIdMap);
                // 估算速度
                ec.getStats().backfillRows.addAndGet(currentSuccessRowCount.get());
                DdlEngineStats.METRIC_BACKFILL_ROWS_FINISHED.update(currentSuccessRowCount.get());

                if (!finished.get()) {
                    throttle.feedback(new com.alibaba.polardbx.executor.backfill.Throttle.FeedbackStats(
                        System.currentTimeMillis() - start, start, currentSuccessRowCount.get()));
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

            // for sliding window of split
            checkAndSplitBackfillObject(
                dbIndex, phyTable, successRowCount, ec, rangeBackfillStartTime, lastBatch, backfillObjects);
        } while (!finished.get());

//        DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set(0);
        reporter.addBackfillCount(successRowCount);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Last backfill row for {1}[{2}][{3}]: {4}",
            ec.getTraceId(),
            dbIndex,
            phyTable,
            successRowCount,
            GsiUtils.rowToString(lastBatch.isEmpty() ? null : lastBatch.get(lastBatch.size() - 1))));
    }

    protected List<Map<Integer, ParameterContext>> extract(String dbIndex, String phyTableName,
                                                           PhyTableOperation extractPlan, ExecutionContext extractEc,
                                                           List<ParameterContext> lowerBound,
                                                           List<ParameterContext> upperBound,
                                                           AtomicReference<Long> successRowCount,
                                                           AtomicReference<Boolean> finished) {
        Cursor extractCursor = null;
        // Transform
        final List<Map<Integer, ParameterContext>> result;
        try {
            // Extract
            extractCursor = ExecutorHelper.execute(extractPlan, extractEc);
            result = com.alibaba.polardbx.executor.gsi.utils.Transformer.buildBatchParam(extractCursor, useBinary,
                null);
        } finally {
            if (extractCursor != null) {
                extractCursor.close(new ArrayList<>());
            }
        }

        if (result.isEmpty()) {
            finished.set(true);
        } else {
            // build new upperBound
            upperBound = buildSelectParam(result, primaryKeysId);
        }

        long affectRows =
            executeInsertSelect(dbIndex, tableNameMapping.get(phyTableName), phyTableName, lowerBound, upperBound,
                extractEc);
        successRowCount.set(affectRows);

        extractEc.getTransaction().commit();

        return result;
    }

    private long executeInsertSelect(String dbIndex, String targetPhyTable,
                                     String sourcePhyTable,
                                     List<ParameterContext> lowerBound,
                                     List<ParameterContext> upperBound,
                                     ExecutionContext executionContext) {
        boolean withLowerBound = GeneralUtil.isNotEmpty(lowerBound);
        boolean withUpperBound = GeneralUtil.isNotEmpty(upperBound);
        if (!withUpperBound) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "backfill failed because the upperbound of the primary key could not be found.");
        }

        PhyTableOperation updatePlan = buildInsertSelectPlanWithParam(dbIndex, sourcePhyTable, targetPhyTable,
            Stream.concat(lowerBound.stream(), upperBound.stream()).collect(Collectors.toList()), withLowerBound,
            true);

        Cursor extractCursor = ExecutorHelper.execute(updatePlan, executionContext);

        return ExecUtils.getAffectRowsByCursor(extractCursor);
    }

    protected PhyTableOperation buildInsertSelectPlanWithParam(String dbIndex, String sourcePhyTable,
                                                               String targetPhyTable,
                                                               List<ParameterContext> params,
                                                               boolean withLowerBound,
                                                               boolean withUpperBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(targetPhyTable, 1));
        planParams.put(2, PlannerUtils.buildParameterContextForTableName(sourcePhyTable, 2));

        int nextParamIndex = 3;

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

        PhyTableOperation targetPhyOp = !withLowerBound ? planInsertSelectWithMax :
            (withUpperBound ? planInsertSelectWithMinAndMax : planInsertSelectWithMin);
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(targetPhyTable, sourcePhyTable)));
        buildParams.setDynamicParams(planParams);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        return sourcePhyTables;
    }
}
