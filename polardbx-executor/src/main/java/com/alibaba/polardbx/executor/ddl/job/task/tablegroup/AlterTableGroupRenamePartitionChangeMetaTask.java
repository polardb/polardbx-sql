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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

@Getter
@TaskName(name = "AlterTableGroupRenamePartitionChangeMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableGroupRenamePartitionChangeMetaTask extends BaseDdlTask {

    protected String tableGroupName;
    protected List<Pair<String, String>> changePartitionsPair;

    @JSONCreator
    public AlterTableGroupRenamePartitionChangeMetaTask(String schemaName, String tableGroupName,
                                                        List<Pair<String, String>> changePartitionsPair) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.changePartitionsPair = changePartitionsPair;

    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        final TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);

        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        List<TablePartitionRecord> tablePartitionRecords = new ArrayList<>();
        for (Pair<String, String> pair : changePartitionsPair) {
            PartitionGroupRecord partitionGroupRecord =
                partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(pair.getKey())).findFirst()
                    .orElse(null);
            partitionGroupAccessor.deletePartitionGroupById(partitionGroupRecord.id);
            List<TablePartitionRecord> tbps =
                tablePartitionAccessor.getTablePartitionsByDbNamePartGroupId(schemaName, partitionGroupRecord.id);
            if (GeneralUtil.isNotEmpty(tbps)) {
                for (TablePartitionRecord tb : tbps) {
                    tb.partName = pair.getValue();
                }
                tablePartitionRecords.addAll(tbps);
            }
            tablePartitionAccessor.deleteTablePartitions(schemaName, partitionGroupRecord.id);
            //partitionGroupAccessor.updatePartitioNameById(partitionGroupRecord.id, pair.getValue());
            //tablePartitionAccessor
            //    .updatePartitionNameByGroupId(partitionGroupRecord.id, pair.getKey(), pair.getValue());
        }

        for (Pair<String, String> pair : changePartitionsPair) {
            PartitionGroupRecord partitionGroupRecord =
                partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(pair.getKey())).findFirst()
                    .orElse(null);
            PartitionGroupRecord copy = partitionGroupRecord.copy();
            copy.setPartition_name(pair.getValue());
            partitionGroupAccessor.addNewPartitionGroupWithId(copy);
        }

        tablePartitionAccessor.addNewTablePartitionsWithId(tablePartitionRecords);

        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        if (!GeneralUtil.isEmpty(tableGroupConfig.getAllTables())) {
            for (TablePartRecordInfoContext recordInfoContext : tableGroupConfig.getAllTables()) {
                try {
                    String tableName = recordInfoContext.getLogTbRec().tableName;
                    TableMeta tableMeta = schemaManager.getTable(tableName);
                    if (tableMeta.isGsi()) {
                        //all the gsi table version change will be behavior by primary table
                        assert
                            tableMeta.getGsiTableMetaBean() != null
                                && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                        tableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                    }
                    TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConnection);
                } catch (Throwable t) {
                    LOGGER.error(String.format(
                        "error occurs while update table version, schemaName:%s, tableName:%s",
                        recordInfoContext.getLogTbRec().tableSchema,
                        recordInfoContext.getLogTbRec().tableName));
                    throw GeneralUtil.nestedException(t);
                }
            }
        }
        updateSupportedCommands(true, false, null);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

}
