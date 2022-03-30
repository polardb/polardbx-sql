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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;

@Getter
@TaskName(name = "CreateTableAddTablesPartitionInfoMetaTask")
public class CreateTableAddTablesPartitionInfoMetaTask extends BaseGmsTask {

    private boolean temporary;

    private TableGroupConfig tableGroupConfig;
    private LocalPartitionDefinitionInfo localPartitionDefinitionInfo;
    private boolean indexAlignWithPrimaryTableGroup;
    private String primaryTable;

    @JSONCreator
    public CreateTableAddTablesPartitionInfoMetaTask(String schemaName,
                                                     String logicalTableName,
                                                     boolean temporary,
                                                     TableGroupConfig tableGroupConfig,
                                                     LocalPartitionDefinitionInfo localPartitionDefinitionInfo,
                                                     boolean indexAlignWithPrimaryTableGroup,
                                                     String primaryTable) {
        super(schemaName, logicalTableName);
        this.temporary = temporary;
        this.tableGroupConfig = tableGroupConfig;

        this.localPartitionDefinitionInfo = localPartitionDefinitionInfo;
        this.indexAlignWithPrimaryTableGroup = indexAlignWithPrimaryTableGroup;
        this.primaryTable = primaryTable;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (!isCreateTableSupported(executionContext)) {
            return;
        }
        if (indexAlignWithPrimaryTableGroup) {
            assert primaryTable != null;
            tableGroupConfig.setTableGroupRecord(null);
            if (tableGroupConfig.getAllTables().size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "unexpected table count");
            }
            TablePartRecordInfoContext tablePartRecordInfoContext = tableGroupConfig.getAllTables().get(0);
            TablePartitionConfig tablePartitionConfig = getTablePartitionConfig(primaryTable, metaDbConnection);
            List<TablePartitionSpecConfig> tablePartitionSpecConfigs = tablePartitionConfig.getPartitionSpecConfigs();
            if (tablePartitionSpecConfigs.size() != tablePartRecordInfoContext.getPartitionRecList().size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "unexpected partition count");
            }

            tablePartRecordInfoContext.getLogTbRec().setGroupId(tablePartitionConfig.getTableConfig().getGroupId());

            for (TablePartitionRecord tablePartitionRecord : tablePartRecordInfoContext.getPartitionRecList()) {
                Optional<TablePartitionSpecConfig> tablePartitionSpecConfig = tablePartitionSpecConfigs.stream()
                    .filter(o -> o.getSpecConfigInfo().partName.equalsIgnoreCase(tablePartitionRecord.partName))
                    .findFirst();
                if (!tablePartitionSpecConfig.isPresent()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "can't found the right partition");
                }
                tablePartitionRecord.setGroupId(tablePartitionSpecConfig.get().getSpecConfigInfo().getGroupId());
            }

        }
        TableMetaChanger.addPartitionInfoMeta(metaDbConnection, tableGroupConfig, executionContext, false);
        if(localPartitionDefinitionInfo!=null){
            new AddLocalPartitionTask(localPartitionDefinitionInfo).executeImpl(metaDbConnection, executionContext);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (!isCreateTableSupported(executionContext)) {
            return;
        }

        TableMetaChanger.removePartitionInfoMeta(metaDbConnection, schemaName, logicalTableName);
        if(localPartitionDefinitionInfo!=null){
            new AddLocalPartitionTask(localPartitionDefinitionInfo).rollbackImpl(metaDbConnection, executionContext);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private boolean isCreateTableSupported(ExecutionContext executionContext) {
        return !(temporary || executionContext.isUseHint());
    }

    public TableGroupConfig getTableGroupConfig() {
        return tableGroupConfig;
    }

    public void setTableGroupConfig(TableGroupConfig tableGroupConfig) {
        this.tableGroupConfig = tableGroupConfig;
    }

    private TablePartitionConfig getTablePartitionConfig(String primaryTable, Connection metaDbConnection) {
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        TablePartitionConfig
            tablePartitionConfig = tablePartitionAccessor.getTablePartitionConfig(schemaName, primaryTable, false);
        return tablePartitionConfig;
    }
}
