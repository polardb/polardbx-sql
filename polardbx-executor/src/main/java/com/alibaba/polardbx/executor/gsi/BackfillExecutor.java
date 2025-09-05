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

package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.backfill.GsiPartitionExtractor;
import com.alibaba.polardbx.executor.backfill.GsiPkRangeExtractor;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.backfill.CdasLoader;
import com.alibaba.polardbx.executor.gsi.backfill.GsiChangeSetLoader;
import com.alibaba.polardbx.executor.gsi.backfill.GsiExtractor;
import com.alibaba.polardbx.executor.gsi.backfill.GsiLoader;
import com.alibaba.polardbx.executor.gsi.backfill.OmcMirrorCopyExtractor;
import com.alibaba.polardbx.executor.gsi.backfill.Updater;
import com.alibaba.polardbx.executor.scaleout.backfill.ChangeSetExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class BackfillExecutor {

    private static final Logger logger = LoggerFactory
        .getLogger(BackfillExecutor.class);

    private static final long EXTRACTOR_TRANSACTION_TIMEOUT = 3600000L;

    private final BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc;

    public BackfillExecutor(BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc) {
        this.executeFunc = executeFunc;
    }

    public int backfill(String schemaName, String primaryTable, String indexName, boolean useBinary,
                        boolean useChangeSet, boolean canUseReturning, List<String> modifyStringColumns,
                        boolean onlineModifyColumn, ExecutionContext baseEc) {
        return backfill(schemaName, primaryTable, indexName, useBinary,
            useChangeSet, canUseReturning, modifyStringColumns, null, null,
            onlineModifyColumn, 1, baseEc);
    }

    public int backfill(String schemaName, String primaryTable, String indexName, boolean useBinary,
                        boolean useChangeSet, boolean canUseReturning, List<String> modifyStringColumns,
                        Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> pkRange,
                        List<String> partitionList,
                        Boolean onlineModifyColumn, int totalThreadCount,  ExecutionContext baseEc) {
        final long batchSize = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_BATCH_SIZE);
        final long speedLimit = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_LIMITATION);
        final long speedMin = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_MIN);
        final long parallelism = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_PARALLELISM);

        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }

        // Init extractor and loader

        final Extractor extractor;
        final Loader loader;

        if (useChangeSet) {
            Map<String, Set<String>> sourcePhyTables = GsiUtils.getPhyTables(schemaName, primaryTable);
            Map<String, String> tableNameMapping =
                GsiUtils.getPhysicalTableMapping(schemaName, primaryTable, indexName, null, null);

            extractor = ChangeSetExecutor
                .create(schemaName, primaryTable, indexName, batchSize, speedMin, speedLimit, parallelism,
                    useBinary, modifyStringColumns, sourcePhyTables, onlineModifyColumn, baseEc);
            loader =
                GsiChangeSetLoader.create(schemaName, primaryTable, indexName, this.executeFunc, baseEc.isUseHint(),
                    baseEc, tableNameMapping);
        } else if (pkRange != null) {
            long pkRangeSpeedLimit = speedLimit / totalThreadCount;
            extractor =
                GsiPkRangeExtractor.create(schemaName, primaryTable, indexName, batchSize, speedMin, pkRangeSpeedLimit,
                    parallelism,
                    useBinary, modifyStringColumns, pkRange, baseEc);
            loader =
                GsiLoader.create(schemaName, primaryTable, indexName, this.executeFunc, baseEc.isUseHint(),
                    canUseReturning, onlineModifyColumn, baseEc);
        } else if (partitionList != null) {
            extractor =
                GsiPartitionExtractor.create(schemaName, primaryTable, indexName, batchSize, speedMin, speedLimit,
                    parallelism,
                    useBinary, modifyStringColumns, partitionList, baseEc);
            loader =
                GsiLoader.create(schemaName, primaryTable, indexName, this.executeFunc, baseEc.isUseHint(),
                    canUseReturning, onlineModifyColumn, baseEc);
        } else {
            extractor =
                GsiExtractor.create(schemaName, primaryTable, indexName, batchSize, speedMin, speedLimit, parallelism,
                    useBinary, modifyStringColumns, onlineModifyColumn, baseEc);
            loader =
                GsiLoader.create(schemaName, primaryTable, indexName, this.executeFunc, baseEc.isUseHint(),
                    canUseReturning, onlineModifyColumn, baseEc);
        }

        boolean finished;
        // Foreach row: lock batch -> fill into index -> release lock
        final AtomicInteger affectRows = new AtomicInteger();
        do {
            finished = true;
            // Load latest extractor position mark
            extractor.loadBackfillMeta(baseEc);
            try {
                extractor.foreachBatch(baseEc, new BatchConsumer() {
                    @Override
                    public void consume(List<Map<Integer, ParameterContext>> batch,
                                        Pair<ExecutionContext, Pair<String, String>> extractEcAndIndexPair) {
                        loader.fillIntoIndex(batch, Pair.of(baseEc, extractEcAndIndexPair.getValue()), () -> {
                            try {
                                // Commit and close extract statement
                                if (!useChangeSet) {
                                    extractEcAndIndexPair.getKey().getTransaction().commit();
                                }
                                return true;
                            } catch (Exception e) {
                                logger.error("Close extract statement failed!", e);
                                return false;
                            }
                        });
                    }
                });
            } catch (TddlNestableRuntimeException e) {
                if (e.getMessage().contains("need to be split into smaller batches")) {
                    finished = false;
                } else {
                    SQLRecorderLogger.ddlLogger.warn(
                        MessageFormat.format(
                            "[{0}] [{1}][{2}] execution get exception {3}",
                            baseEc.getTraceId(),
                            baseEc.getTaskId().toString(),
                            baseEc.getBackfillId().toString(),
                            e)
                    );
                    throw e;
                }
            }
        } while (!finished);

        return affectRows.get();
    }

    public int mirrorCopyGsiBackfill(String schemaName, String primaryTable, String indexName, boolean useChangeSet,
                                     boolean useBinary, boolean onlineModifyColumn, ExecutionContext baseEc) {
        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }

        ExecutionContext executionContext = baseEc.copy();
        PhyTableOperationUtil.disableIntraGroupParallelism(schemaName, executionContext);

        Map<String, Set<String>> sourcePhyTables = GsiUtils.getPhyTables(schemaName, primaryTable);
        Map<String, String> tableNameMapping =
            GsiUtils.getPhysicalTableMapping(schemaName, primaryTable, indexName, null, null);

        OmcMirrorCopyExtractor extractor =
            OmcMirrorCopyExtractor.create(schemaName, primaryTable, indexName, tableNameMapping, sourcePhyTables,
                useChangeSet, useBinary, onlineModifyColumn, baseEc);
        extractor.loadBackfillMeta(executionContext);

        final AtomicInteger affectRows = new AtomicInteger();
        extractor.foreachBatch(executionContext, new BatchConsumer() {
            @Override
            public void consume(List<Map<Integer, ParameterContext>> batch,
                                Pair<ExecutionContext, Pair<String, String>> extractEcAndIndexPair) {
                // pass
            }

            @Override
            public void consume(String sourcePhySchema, String sourcePhyTable, Cursor cursor, ExecutionContext context,
                                List<Map<Integer, ParameterContext>> mockResult) {
                // pass
            }
        });

        return affectRows.get();
    }

    public int logicalTableDataMigrationBackFill(String srcSchemaName, String dstSchemaName,
                                                 String srcTableName, String dstTableName, List<String> dstGsiNames,
                                                 ExecutionContext baseEc) {
        final long batchSize = baseEc.getParamManager().getLong(ConnectionParams.CREATE_DATABASE_AS_BATCH_SIZE);
        final long speedLimit =
            baseEc.getParamManager().getLong(ConnectionParams.CREATE_DATABASE_AS_BACKFILL_SPEED_LIMITATION);
        final long speedMin = baseEc.getParamManager().getLong(ConnectionParams.CREATE_DATABASE_AS_BACKFILL_SPEED_MIN);
        final long parallelism =
            baseEc.getParamManager().getLong(ConnectionParams.CREATE_DATABASE_AS_BACKFILL_PARALLELISM);
        final boolean useBinary = baseEc.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);

        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }

        SQLRecorderLogger.ddlLogger.info(String.format("createDatabaseBackFill srcSchema[%s] dstSchema[%s] table[%s]",
            srcSchemaName, dstSchemaName, srcTableName));

        // Init extractor and loader
        ExecutionContext copiedEc = baseEc.copy();
        copiedEc.setSchemaName(dstSchemaName);
        final Extractor extractor =
            GsiExtractor.create(srcSchemaName, srcTableName, srcTableName, batchSize, speedMin, speedLimit, parallelism,
                useBinary, null, false, baseEc);
        final CdasLoader cdasLoader =
            CdasLoader.create(srcSchemaName, dstSchemaName, srcTableName, dstTableName, this.executeFunc,
                copiedEc.isUseHint(), copiedEc, false);
        final List<CdasLoader> gsiCdasLoaders = new ArrayList<>();
        dstGsiNames.forEach(gsiName -> {
            ExecutionContext gsiCopiedEc = baseEc.copy();
            gsiCopiedEc.setSchemaName(dstSchemaName);
            CdasLoader gsiCdasLoader
                = CdasLoader.create(srcSchemaName, dstSchemaName, srcTableName, gsiName, this.executeFunc,
                gsiCopiedEc.isUseHint(),
                gsiCopiedEc, true);
            gsiCdasLoaders.add(gsiCdasLoader);
        });

        // Foreach row: lock batch -> fill into index -> release lock
        final AtomicInteger affectRows = new AtomicInteger();

        boolean finished;
        do {
            finished = true;
            // Load latest extractor position mark
            extractor.loadBackfillMeta(baseEc);
            try {
                extractor.foreachBatch(baseEc, new BatchConsumer() {
                        @Override
                        public void consume(List<Map<Integer, ParameterContext>> batch,
                                            Pair<ExecutionContext, Pair<String, String>> extractEcAndIndexPair) {

                            cdasLoader.fillIntoIndex(batch, Pair.of(baseEc, extractEcAndIndexPair.getValue()), () -> {
                                try {
                                    extractEcAndIndexPair.getKey().getTransaction().commit();
                                    return true;
                                } catch (Exception e) {
                                    logger.error("Close extract statement failed!", e);
                                    return false;
                                }
                            });

                            for (CdasLoader gsiCdasLoader : gsiCdasLoaders) {
                                gsiCdasLoader.fillIntoIndex(batch, Pair.of(baseEc, extractEcAndIndexPair.getValue()),
                                    () -> {
                                        try {
                                            extractEcAndIndexPair.getKey().getTransaction().commit();
                                            return true;
                                        } catch (Exception e) {
                                            logger.error("Close extract statement failed!", e);
                                            return false;
                                        }
                                    });
                            }
                        }
                    }
                );
            } catch (TddlNestableRuntimeException e) {
                if (e.getMessage().contains("need to be split into smaller batches")) {
                    finished = false;
                } else {
                    throw e;
                }
            }
        } while (!finished);

        return affectRows.get();
    }

    public int addColumnsBackfill(String schemaName, String primaryTable, List<String> indexNames, List<String> columns,
                                  ExecutionContext baseEc) {
        final long batchSize = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_BATCH_SIZE);
        final long speedLimit = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_LIMITATION);
        final long speedMin = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_MIN);
        final long parallelism = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_PARALLELISM);
        final boolean useBinary = baseEc.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);

        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }

        // Init extractor and loader
        SQLRecorderLogger.ddlLogger.info(String.format("addColumnsBackfill primaryTable=%s indexNames=%s columns=%s",
            primaryTable, indexNames, columns));

        // This is for filling clustered indexes and all columns are same, so just use first one.
        final Extractor extractor =
            GsiExtractor
                .create(schemaName, primaryTable, indexNames.get(0), batchSize, speedMin, speedLimit, parallelism,
                    useBinary, null, false, baseEc);
        final Updater updater = Updater.create(schemaName, primaryTable, indexNames, columns, this.executeFunc);

        boolean finished;
        // Foreach row: lock batch -> fill into index -> release lock
        final AtomicInteger affectRows = new AtomicInteger();
        do {
            finished = true;
            // Load latest extractor position mark
            extractor.loadBackfillMeta(baseEc);
            try {
                extractor.foreachBatch(baseEc, new BatchConsumer() {
                    @Override
                    public void consume(List<Map<Integer, ParameterContext>> batch,
                                        Pair<ExecutionContext, Pair<String, String>> extractEcAndIndexPair) {
                        updater.fillUpdateIndex(batch, baseEc, () -> {
                            try {
                                // Commit and close extract statement
                                extractEcAndIndexPair.getKey().getTransaction().commit();
                                return true;
                            } catch (Exception e) {
                                logger.error("Close extract statement failed!", e);
                                return false;
                            }
                        });
                    }
                });
            } catch (TddlNestableRuntimeException e) {
                if (e.getMessage().contains("need to be split into smaller batches")) {
                    finished = false;
                } else {
                    throw e;
                }
            }
        } while (!finished);

        return affectRows.get();
    }
}
