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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupRenamePartition;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupRenamePartition;

import java.sql.Connection;
import java.util.List;

public class LogicalAlterTableGroupRenamePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupRenamePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlAlterTableGroup sqlAlterTableGroup =
            (SqlAlterTableGroup) (((LogicalAlterTableGroupRenamePartition) logicalDdlPlan).relDdl.getSqlNode());
        SqlAlterTableGroupRenamePartition sqlAlterTableGroupRenamePartition =
            (SqlAlterTableGroupRenamePartition) sqlAlterTableGroup.getAlters().get(0);
        String tableGroupName = sqlAlterTableGroup.getTableGroupName().toString();
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(executionContext.getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);
        handleRenamePartitionGroup(sqlAlterTableGroupRenamePartition, tableGroupConfig, executionContext);
        return new TransientDdlJob();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) (((LogicalAlterTableGroupRenamePartition) logicalDdlPlan).relDdl.getSqlNode()),
            executionContext);
        return false;
    }

    private void handleRenamePartitionGroup(SqlAlterTableGroupRenamePartition sqlAlterTableGroupRenamePartition,
                                            TableGroupConfig tableGroupConfig,
                                            ExecutionContext executionContext) {

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(executionContext.getSchemaName()).getTableGroupInfoManager();
        try (Connection connection = MetaDbUtil.getConnection()) {
            connection.setAutoCommit(false);
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
            partitionGroupAccessor.setConnection(connection);
            tablePartitionAccessor.setConnection(connection);
            List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
            for (org.apache.calcite.util.Pair<String, String> pair : sqlAlterTableGroupRenamePartition
                .getChangePartitionsPair()) {
                PartitionGroupRecord partitionGroupRecord =
                    partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(pair.left)).findFirst()
                        .orElse(null);
                partitionGroupAccessor.updatePartitioNameById(partitionGroupRecord.id, pair.right);
                tablePartitionAccessor.updatePartitionNameByGroupId(partitionGroupRecord.id, pair.left, pair.right);
            }
            if (!GeneralUtil.isEmpty(tableGroupConfig.getAllTables())) {
                for (TablePartRecordInfoContext recordInfoContext : tableGroupConfig.getAllTables()) {
                    GsiMetaManager.updateTableVersion(recordInfoContext.getLogTbRec().tableSchema,
                        recordInfoContext.getLogTbRec().tableName, connection);
                }
            }
            connection.commit();

            if (!GeneralUtil.isEmpty(tableGroupConfig.getAllTables())) {
                for (TablePartRecordInfoContext recordInfoContext : tableGroupConfig.getAllTables()) {
                    SyncManagerHelper.sync(new TableMetaChangeSyncAction(recordInfoContext.getLogTbRec().tableSchema,
                        recordInfoContext.getLogTbRec().tableName), recordInfoContext.getLogTbRec().tableSchema);
                }

            }
            tableGroupInfoManager.reloadTableGroupByGroupId(tableGroupConfig.getTableGroupRecord().id);

        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

}
