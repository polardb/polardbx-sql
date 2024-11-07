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

package com.alibaba.polardbx.executor.partitionmanagement;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.partitionmanagement.backfill.AlterTableGroupExtractor;
import com.alibaba.polardbx.executor.partitionmanagement.backfill.AlterTableGroupLoader;
import com.alibaba.polardbx.executor.scaleout.backfill.ChangeSetExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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

    public int backfill(String schemaName,
                        String tableName,
                        ExecutionContext baseEc,
                        Map<String, Set<String>> sourcePhyTables,
                        Map<String, Set<String>> targetPhyTables,
                        boolean movePartitions,
                        boolean useChangeSet) {
        final long batchSize = baseEc.getParamManager().getLong(ConnectionParams.SCALEOUT_BACKFILL_BATCH_SIZE);
        final long speedMin = baseEc.getParamManager().getLong(ConnectionParams.SCALEOUT_BACKFILL_SPEED_MIN);
        final long speedLimit = baseEc.getParamManager().getLong(ConnectionParams.SCALEOUT_BACKFILL_SPEED_LIMITATION);
        final long parallelism = baseEc.getParamManager().getLong(ConnectionParams.SCALEOUT_BACKFILL_PARALLELISM);
        final boolean useBinary = baseEc.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);

        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }

        //key: physicalTableName,
        //val: pair<sourceGroup,targetGroup>
        Map<String, Pair<String, String>> physicalTableGroupMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (movePartitions) {
            for (Map.Entry<String, Set<String>> sourceEntry : sourcePhyTables.entrySet()) {
                for (String sourcePhyTb : GeneralUtil.emptyIfNull(sourceEntry.getValue())) {
                    for (Map.Entry<String, Set<String>> targetEntry : targetPhyTables.entrySet()) {
                        if (targetEntry.getValue().contains(sourcePhyTb)) {
                            physicalTableGroupMap.put(sourcePhyTb, Pair.of(sourceEntry.getKey(), targetEntry.getKey()));
                        }
                    }
                }
            }
        }
        // Init extractor and loader
        Extractor extractor;
        if (useChangeSet) {
            extractor = ChangeSetExecutor
                .create(schemaName, tableName, tableName, batchSize, speedMin, speedLimit, parallelism, useBinary,
                    null, sourcePhyTables, false, baseEc);
        } else {
            extractor = AlterTableGroupExtractor
                .create(schemaName, tableName, tableName, batchSize, speedMin, speedLimit, parallelism, useBinary,
                    sourcePhyTables, baseEc);
        }
        final Loader loader =
            AlterTableGroupLoader
                .create(schemaName, tableName, tableName, this.executeFunc, baseEc.isUseHint(), baseEc,
                    physicalTableGroupMap, movePartitions);

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
                    throw e;
                }
            }
        } while (!finished);

        return affectRows.get();
    }
}
