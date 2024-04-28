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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class OSSBackFillExecutor {

    private static final Logger logger = LoggerFactory
        .getLogger("oss");

    private Engine sourceEngine;
    private Engine targetEngine;

    public OSSBackFillExecutor(Engine sourceEngine, Engine targetEngine) {
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
    }

    public int backFill2FileStore(String schemaName, String sourceTableName, String targetTableName,
                                  ExecutionContext baseEc,
                                  Map<String, Set<String>> sourcePhyTables, int indexStride, long parallelism,
                                  Map<Pair<String, String>, OSSBackFillWriterTask> tasks,
                                  String designatedPhysicalPartition) {
        return backFill2FileStore(schemaName, sourceTableName, targetTableName, baseEc, sourcePhyTables, indexStride,
            parallelism,
            tasks, designatedPhysicalPartition, false);
    }

    public int backFill2FileStore(String schemaName, String sourceTableName, String targetTableName,
                                  ExecutionContext baseEc,
                                  Map<String, Set<String>> sourcePhyTables, int indexStride, long parallelism,
                                  Map<Pair<String, String>, OSSBackFillWriterTask> tasks,
                                  String designatedPhysicalPartition, boolean supportPause) {
        Preconditions.checkArgument(Engine.isFileStore(targetEngine));

        final long batchSize = indexStride;
        final long speedMin = baseEc.getParamManager().getLong(ConnectionParams.OSS_BACKFILL_SPEED_MIN);
        final long speedLimit = baseEc.getParamManager().getLong(ConnectionParams.OSS_BACKFILL_SPEED_LIMITATION);

        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }

        // Init extractor and loader
        final OSSBackFillExtractor extractor =
            OSSBackFillExtractor.create(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit,
                parallelism, sourcePhyTables, baseEc, designatedPhysicalPartition, sourceEngine, targetEngine);

        final BatchConsumer batchConsumer = new OSSBackFillConsumer(tasks);

        if (supportPause) {
            extractor.loadBackfillMetaRestart(baseEc);
        } else {
            // Load latest extractor position mark
            extractor.loadBackfillMeta(baseEc);
        }

        // Foreach row: lock batch -> fill into index -> release lock
        final AtomicInteger affectRows = new AtomicInteger();
        extractor.foreachBatch(baseEc, batchConsumer);

        return affectRows.get();
    }

    public int backFill2Innodb(String schemaName, String sourceTableName, String targetTableName,
                               ExecutionContext baseEc,
                               Map<String, Set<String>> sourcePhyTables, int indexStride, long parallelism,
                               BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                               Map<String, String> sourceTargetDbMap, String designateLogicalPart) {
        Preconditions.checkArgument(targetEngine == Engine.INNODB);

        final long batchSize = indexStride;
        final long speedMin = baseEc.getParamManager().getLong(ConnectionParams.OSS_BACKFILL_SPEED_MIN);
        final long speedLimit = baseEc.getParamManager().getLong(ConnectionParams.OSS_BACKFILL_SPEED_LIMITATION);

        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }

        // Init extractor and loader
        final Extractor extractor =
            OSSBackFillExtractor.create(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit,
                parallelism, sourcePhyTables, baseEc, null, sourceEngine, targetEngine);

        final Loader loader =
            OSSBackFillLoader
                .create(schemaName, targetTableName, targetTableName, executeFunc, baseEc.isUseHint(), baseEc,
                    sourceTargetDbMap, designateLogicalPart);

        // Load latest extractor position mark
        extractor.loadBackfillMeta(baseEc);

        // Foreach row: lock batch -> fill into index -> release lock
        final AtomicInteger affectRows = new AtomicInteger();
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

        return affectRows.get();
    }
}
