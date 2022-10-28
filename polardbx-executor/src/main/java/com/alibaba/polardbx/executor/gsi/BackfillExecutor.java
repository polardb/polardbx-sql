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
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.backfill.GsiExtractor;
import com.alibaba.polardbx.executor.gsi.backfill.GsiLoader;
import com.alibaba.polardbx.executor.gsi.backfill.Updater;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    public int backfill(String schemaName, String primaryTable, String indexName, ExecutionContext baseEc) {
        final long batchSize = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_BATCH_SIZE);
        final long speedLimit = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_LIMITATION);
        final long speedMin = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_MIN);
        final long parallelism = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_PARALLELISM);

        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }

        // Init extractor and loader
        final Extractor extractor =
            GsiExtractor
                .create(schemaName, primaryTable, indexName, batchSize, speedMin, speedLimit, parallelism, baseEc);
        final Loader loader =
            GsiLoader.create(schemaName, primaryTable, indexName, this.executeFunc, baseEc.isUseHint(), baseEc);

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

    public int addColumnsBackfill(String schemaName, String primaryTable, List<String> indexNames, List<String> columns,
                                  ExecutionContext baseEc) {
        final long batchSize = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_BATCH_SIZE);
        final long speedLimit = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_LIMITATION);
        final long speedMin = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_MIN);
        final long parallelism = baseEc.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_PARALLELISM);

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
                    baseEc);
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
