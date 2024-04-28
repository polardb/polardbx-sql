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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillExecutor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

@Getter
@TaskName(name = "MoveDataToInnodbTask")
public class MoveDataToInnodbTask extends BaseGmsTask {
    private String sourceTableName;
    private String targetTableName;
    private Engine sourceEngine;
    private Engine targetEngine;

    private List<String> filterPartNames;

    @JSONCreator
    public MoveDataToInnodbTask(String schemaName, String logicalTableName,
                                String sourceTableName, String targetTableName,
                                Engine sourceEngine, Engine targetEngine, List<String> filterPartNames) {
        super(schemaName, logicalTableName);
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
        this.filterPartNames = filterPartNames;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        executionContext.setBackfillId(getTaskId());
        // don't continue the ddl if it was paused
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                MetaDbUtil.beginTransaction(metaDbConn);
                List<FilesRecord> files =
                    TableMetaChanger.lockOssFileMeta(metaDbConn, getTaskId(), schemaName, logicalTableName);
                if (files != null && files.size() > 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CANT_CONTINUE_DDL);
                }
                MetaDbUtil.commit(metaDbConn);
            } catch (Exception e) {
                MetaDbUtil.rollback(metaDbConn, e, null, null);
                e.printStackTrace();
                throw GeneralUtil.nestedException(e);
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, null);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
        // don't load table or generate data here.
        fileStore2Innodb(executionContext);
    }

    private void fileStore2Innodb(ExecutionContext executionContext) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {

                TableMeta sourceTableMeta =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(sourceTableName);

                Map<String, Set<String>> sourcePhyTables = buildSourcePhyTables(sourceTableMeta, filterPartNames);
                final int parallelism =
                    executionContext.getParamManager().getInt(ConnectionParams.OSS_BACKFILL_PARALLELISM);
                final long indexStride =
                    executionContext.getParamManager().getLong(ConnectionParams.OSS_ORC_INDEX_STRIDE);

                BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc =
                    (List<RelNode> inputs, ExecutionContext executionContext1) -> {
                        QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(executionContext1);
                        List<Cursor> inputCursors = new ArrayList<>(inputs.size());
                        executeWithConcurrentPolicy(executionContext1, inputs, queryConcurrencyPolicy, inputCursors,
                            schemaName);
                        return inputCursors;
                    };

                Map<String, String> sourceTargetDbMap = new HashMap<>();
                String designateLogicalPart =
                    !filterPartNames.isEmpty() && filterPartNames.size() == 1 ? filterPartNames.get(0) : null;

                // do back fill: select source table -> fill target orc file
                OSSBackFillExecutor backFillExecutor =
                    new OSSBackFillExecutor(sourceEngine, targetEngine);

                backFillExecutor
                    .backFill2Innodb(schemaName, sourceTableName, targetTableName, executionContext, sourcePhyTables,
                        (int) indexStride, parallelism, executeFunc, sourceTargetDbMap, designateLogicalPart);

            } catch (Exception e) {
                MetaDbUtil.rollback(metaDbConn, e, null, null);

                throw GeneralUtil.nestedException(e);
            } finally {

                MetaDbUtil.endTransaction(metaDbConn, null);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private Map<String, Set<String>> buildSourcePhyTables(TableMeta sourceTableMeta, List<String> filterPartNames) {
        Map<String, Set<String>> sourcePhyTables = new HashMap<>();

        Map<String, List<PhysicalPartitionInfo>> physicalPartitionTopology =
            sourceTableMeta.getPartitionInfo().getPhysicalPartitionTopology(filterPartNames);

        for (Map.Entry<String, List<PhysicalPartitionInfo>> entry : physicalPartitionTopology.entrySet()) {
            for (PhysicalPartitionInfo physicalPartitionInfo : entry.getValue()) {

                String phySchema = physicalPartitionInfo.getGroupKey();
                String phyTable = physicalPartitionInfo.getPhyTable();

                // fill source topology
                if (!sourcePhyTables.containsKey(phySchema)) {
                    sourcePhyTables.put(phySchema, new HashSet<>());
                }
                sourcePhyTables.get(phySchema).add(phyTable);
            }
        }

        return sourcePhyTables;
    }
}
