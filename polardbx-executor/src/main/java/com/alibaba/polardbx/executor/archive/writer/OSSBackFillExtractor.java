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

package com.alibaba.polardbx.executor.archive.writer;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.meta.FileStorageBackFillAccessor;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlSelect;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_BACKFILL_EXCEPTION;

public class OSSBackFillExtractor extends Extractor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    // phy schema - {phy tables}
    private Map<String, Set<String>> sourcePhyTables;

    private Engine sourceEngine;
    private Engine targetEngine;

    protected OSSBackFillExtractor(String schemaName, String sourceTableName, String targetTableName,
                                   long batchSize,
                                   long speedMin,
                                   long speedLimit,
                                   long parallelism,
                                   PhyTableOperation planSelectWithMax,
                                   PhyTableOperation planSelectWithMin,
                                   PhyTableOperation planSelectWithMinAndMax,
                                   PhyTableOperation planSelectMaxPk,
                                   List<Integer> primaryKeysId,
                                   Map<String, Set<String>> sourcePhyTables,
                                   Engine sourceEngine,
                                   Engine targetEngine) {
        super(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit, parallelism, false,
            null, planSelectWithMax,
            planSelectWithMin, planSelectWithMinAndMax, planSelectMaxPk, null, primaryKeysId);
        this.sourcePhyTables = sourcePhyTables;
        this.sourceEngine = GeneralUtil.coalesce(sourceEngine, Engine.INNODB);
        this.targetEngine = GeneralUtil.coalesce(targetEngine, Engine.INNODB);
    }

    public static OSSBackFillExtractor create(String schemaName, String sourceTableName, String targetTableName,
                                              long batchSize,
                                              long speedMin, long speedLimit, long parallelism,
                                              Map<String, Set<String>> sourcePhyTables,
                                              ExecutionContext ec, String physicalPartition, Engine sourceEngine,
                                              Engine targetEngine) {

        // we use sourceTableName instead of targetTableName, because targetTableMeta couldn't be fetched during backfill.
        ExtractorInfo info = Extractor.buildExtractorInfo(ec, schemaName, sourceTableName, sourceTableName, false);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new OSSBackFillExtractor(schemaName,
            sourceTableName,
            targetTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, true,
                SqlSelect.LockMode.SHARED_LOCK, physicalPartition),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, false,
                SqlSelect.LockMode.SHARED_LOCK, physicalPartition),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, true,
                SqlSelect.LockMode.SHARED_LOCK, physicalPartition),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            info.getPrimaryKeysId(),
            sourcePhyTables, sourceEngine, targetEngine);
    }

    @Override
    protected List<Map<Integer, ParameterContext>> extract(String dbIndex, String phyTableName,
                                                           PhyTableOperation extractPlan,
                                                           ExecutionContext extractEc,
                                                           BatchConsumer batchConsumer,
                                                           List<ParameterContext> lowerBound,
                                                           List<ParameterContext> upperBound) {
        // Extract
        Cursor extractCursor = doExtract(extractPlan, extractEc);

        // Consume
        return doConsume(extractPlan, extractEc, batchConsumer, extractCursor);
    }

    private Cursor doExtract(PhyTableOperation extractPlan, ExecutionContext extractEc) {
        switch (sourceEngine) {
        case OSS:
        case S3:
        case LOCAL_DISK:
        case EXTERNAL_DISK:
        case NFS:
            RelNode fileStorePlan =
                OSSTableScan.fromPhysicalTableOperation(extractPlan, extractEc, this.sourceTableName, 1);

            return ExecutorHelper.execute(fileStorePlan, extractEc);
        case INNODB:
        default:
            return ExecutorHelper.execute(extractPlan, extractEc);
        }
    }

    @NotNull
    private List<Map<Integer, ParameterContext>> doConsume(PhyTableOperation extractPlan, ExecutionContext extractEc,
                                                           BatchConsumer batchConsumer, Cursor extractCursor) {
        switch (targetEngine) {
        case S3:
        case LOCAL_DISK:
        case OSS:
        case EXTERNAL_DISK:
        case NFS:
            return consumeFileStore(extractPlan, extractEc, batchConsumer, extractCursor);
        case INNODB:
        default:
            return consumeInnodb(extractPlan, extractEc, batchConsumer, extractCursor);
        }
    }

    @NotNull
    private List<Map<Integer, ParameterContext>> consumeInnodb(PhyTableOperation extractPlan,
                                                               ExecutionContext extractEc, BatchConsumer batchConsumer,
                                                               Cursor extractCursor) {
        final List<Map<Integer, ParameterContext>> result;
        try {
            result = com.alibaba.polardbx.executor.gsi.utils.Transformer.buildBatchParam(extractCursor, false, null);
        } finally {
            extractCursor.close(new ArrayList<>());
        }

        FailPoint.injectRandomExceptionFromHint(FP_RANDOM_BACKFILL_EXCEPTION, extractEc);

        // Load
        batchConsumer.consume(result, Pair.of(extractEc, Pair.of(extractPlan.getDbIndex(), null)));

        return result;
    }

    @NotNull
    private List<Map<Integer, ParameterContext>> consumeFileStore(PhyTableOperation extractPlan,
                                                                  ExecutionContext extractEc,
                                                                  BatchConsumer next,
                                                                  Cursor extractCursor) {
        final List<Map<Integer, ParameterContext>> mockResult = new ArrayList<>();
        try {
            FailPoint.injectRandomExceptionFromHint(FP_RANDOM_BACKFILL_EXCEPTION, extractEc);

            // Load
            String sourcePhySchema = extractPlan.getDbIndex();
            // build tables by plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
            String sourcePhyTable = extractPlan.getTableNames().get(0).get(0);

            // consume rows and gen oss data
            next.consume(sourcePhySchema, sourcePhyTable, extractCursor, extractEc, mockResult);

        } finally {
            extractCursor.close(new ArrayList<>());
        }
        return mockResult;
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        return this.sourcePhyTables;
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
    @Override
    protected List<GsiBackfillManager.BackfillObjectRecord> splitAndInitUpperBound(final ExecutionContext baseEc,
                                                                                   final long ddlJobId,
                                                                                   final String dbIndex,
                                                                                   final String phyTable,
                                                                                   final List<Integer> primaryKeysId) {
        // Build parameter
        final Map<Integer, ParameterContext> params = new HashMap<>(1);
        params.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        // Build plan
        final PhyTableOperation phyTableOperation = this.planSelectMaxPk;
        phyTableOperation.setDbIndex(dbIndex);
        phyTableOperation.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
        phyTableOperation.setParam(params);

        RelNode plan;
        switch (sourceEngine) {
        case OSS:
        case S3:
        case LOCAL_DISK:
        case EXTERNAL_DISK:
        case NFS:
            plan = OSSTableScan.fromPhysicalTableOperation(phyTableOperation, baseEc, this.sourceTableName, 1);
            break;
        case INNODB:
        default:
            plan = phyTableOperation;
            break;
        }

        final List<Map<Integer, ParameterContext>> upperBound = executePhysicalPlan(baseEc, plan);

        return getBackfillObjectRecords(baseEc, ddlJobId, dbIndex, phyTable, primaryKeysId, upperBound, "");
    }

    /**
     * Load latest position mark
     *
     * @param ec Id of parent DDL job
     * @return this
     */
    public Extractor loadBackfillMetaRestart(ExecutionContext ec) {
        Long backfillId = ec.getBackfillId();

        // Init position mark with upper bound
        final List<GsiBackfillManager.BackfillObjectRecord> initBfoList = initAllUpperBound(ec, backfillId);

        doRestartWithRetry(5, backfillId, initBfoList);

        // Load from system table
        this.reporter.loadBackfillMeta(backfillId);
        SQLRecorderLogger.ddlLogger.info(
            String.format("loadBackfillMeta for backfillId %d: %s", backfillId, this.reporter.getBackfillBean()));
        return this;
    }

    private void doRestartWithRetry(int retryLimit, Long backfillId,
                                    List<GsiBackfillManager.BackfillObjectRecord> initBfoList) {
        int retryCount = 0;
        do {
            try {
                doRestart(backfillId, initBfoList);
                break;
            } catch (Throwable t) {
                // retry when deadlock found
                if (t.getCause() instanceof SQLException
                    && t.getCause().getMessage().startsWith("Deadlock found")) {
                    if (retryCount > retryLimit) {
                        throw t;
                    }
                    ++retryCount;
                } else {
                    throw t;
                }
            }
        } while (true);
    }

    private void doRestart(Long backfillId, List<GsiBackfillManager.BackfillObjectRecord> initBfoList) {
        FileStorageBackFillAccessor fileStorageBackFillAccessor = new FileStorageBackFillAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                fileStorageBackFillAccessor.setConnection(metaDbConn);
                MetaDbUtil.beginTransaction(metaDbConn);
                // insert ignore the backfill_object to file_storage_backfill_object
                fileStorageBackFillAccessor.insertIgnoreFileBackfillMeta(initBfoList);

                List<GsiBackfillManager.BackfillObjectRecord> initBackfillObjects =
                    fileStorageBackFillAccessor.selectBackfillObjectFromFileStorageById(backfillId);
                // init progress in the first step
                if (fileStorageBackFillAccessor.selectBackfillProgress(backfillId).isEmpty()) {
                    final GsiBackfillManager.BackfillObjectRecord bfo = initBackfillObjects.get(0);
                    final GsiBackfillManager.BackfillObjectRecord logicalBfo = bfo.copy();
                    logicalBfo.setPhysicalDb(null);
                    logicalBfo.setPhysicalTable(null);
                    initBackfillObjects.add(0, logicalBfo);
                }

                fileStorageBackFillAccessor.replaceBackfillMeta(initBackfillObjects);
                // insert file_storage_backfill_object to backfill_object
                MetaDbUtil.commit(metaDbConn);

            } catch (Exception e) {
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(e), LOGGER, "update back-fill object");
                throw GeneralUtil.nestedException(e);
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
