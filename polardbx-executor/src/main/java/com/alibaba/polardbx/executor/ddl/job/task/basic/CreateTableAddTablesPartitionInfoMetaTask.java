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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.ImplicitTableGroupUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;

@Getter
@TaskName(name = "CreateTableAddTablesPartitionInfoMetaTask")
public class CreateTableAddTablesPartitionInfoMetaTask extends BaseGmsTask {

    private boolean temporary;

    private TableGroupDetailConfig tableGroupConfig;
    private LocalPartitionDefinitionInfo localPartitionDefinitionInfo;
    private String tableGroupAlignWithTargetTable;
    private String primaryTable;
    private String locality;
    //specified in create table statement
    private String joinGroup;
    private Boolean oss;
    private Boolean withTableGroupImplicit;
    private Boolean autoCreateTg;

    @JSONCreator
    public CreateTableAddTablesPartitionInfoMetaTask(String schemaName,
                                                     String logicalTableName,
                                                     boolean temporary,
                                                     TableGroupDetailConfig tableGroupConfig,
                                                     LocalPartitionDefinitionInfo localPartitionDefinitionInfo,
                                                     String tableGroupAlignWithTargetTable,
                                                     String primaryTable,
                                                     String joinGroup,
                                                     boolean oss,
                                                     boolean withTableGroupImplicit,
                                                     boolean autoCreateTg) {
        super(schemaName, logicalTableName);
        this.temporary = temporary;
        this.tableGroupConfig = tableGroupConfig;

        this.localPartitionDefinitionInfo = localPartitionDefinitionInfo;
        this.tableGroupAlignWithTargetTable = tableGroupAlignWithTargetTable;
        this.primaryTable = primaryTable;
        this.joinGroup = joinGroup;
        this.oss = oss;
        this.autoCreateTg = autoCreateTg;
        this.withTableGroupImplicit = withTableGroupImplicit;
        ImplicitTableGroupUtil.checkAutoCreateTableGroup(tableGroupConfig, oss, withTableGroupImplicit, autoCreateTg);
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (!isCreateTableSupported(executionContext)) {
            return;
        }
        TablePartitionConfig tablePartitionConfig = null;
        if (StringUtils.isNotEmpty(tableGroupAlignWithTargetTable)) {
            tablePartitionConfig =
                getTablePartitionConfig(tableGroupAlignWithTargetTable, metaDbConnection);
        }
        if (StringUtils.isNotEmpty(tableGroupAlignWithTargetTable) && tablePartitionConfig != null) {
            tableGroupConfig.setTableGroupRecord(null);
            if (tableGroupConfig.getAllTables().size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "unexpected table count");
            }
            TablePartRecordInfoContext tablePartRecordInfoContext =
                tableGroupConfig.getTablesPartRecordInfoContext().get(0);
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
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "can't found the right partition");
                }
                tablePartitionRecord.setGroupId(tablePartitionSpecConfig.get().getSpecConfigInfo().getGroupId());
                if (GeneralUtil.isNotEmpty(tablePartRecordInfoContext.getSubPartitionRecMap())) {
                    List<TablePartitionRecord> subTablePartitionRecords =
                        tablePartRecordInfoContext.getSubPartitionRecMap().get(tablePartitionRecord.getPartName());
                    List<TablePartitionSpecConfig> subTablePartitionConfigs =
                        tablePartitionSpecConfig.get().getSubPartitionSpecConfigs();
                    if (GeneralUtil.isEmpty(subTablePartitionRecords) || GeneralUtil.isEmpty(
                        subTablePartitionConfigs)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "can't found the right subpartition");
                    }
                    for (TablePartitionRecord subTablePartitionRecord : subTablePartitionRecords) {
                        Optional<TablePartitionSpecConfig> subTablePartitionSpecConfig =
                            subTablePartitionConfigs.stream()
                                .filter(o -> o.getSpecConfigInfo().partName.equalsIgnoreCase(
                                    subTablePartitionRecord.partName))
                                .findFirst();
                        if (!subTablePartitionSpecConfig.isPresent()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                "can't found the right partition");
                        }
                        subTablePartitionRecord.setGroupId(subTablePartitionSpecConfig.get().getSpecConfigInfo()
                            .getGroupId());
                    }
                }
            }

        } else if (primaryTable == null) {
            JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
            JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();
            joinGroupTableDetailAccessor.setConnection(metaDbConnection);
            joinGroupInfoAccessor.setConnection(metaDbConnection);

            if (tableGroupConfig.getTableGroupRecord() == null) {
                TablePartRecordInfoContext tablePartRecordInfoContext =
                    tableGroupConfig.getTablesPartRecordInfoContext()
                        .get(0);
                Long groupId = tablePartRecordInfoContext.getLogTbRec().groupId;
                boolean isEmptyGroup =
                    tableGroupConfig.getAllTables().size() == 1 && (groupId == null || groupId == -1);
                if (!isEmptyGroup) {
                    TableGroupConfig tgConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                        .getTableGroupConfigById(groupId);

                    if (GeneralUtil.isNotEmpty(tgConfig.getAllTables())) {
                        JoinGroupTableDetailRecord joinGroupTableDetailRecord =
                            joinGroupTableDetailAccessor.getJoinGroupDetailBySchemaTableName(schemaName,
                                tgConfig.getAllTables().get(0));
                        if (joinGroupTableDetailRecord != null) {
                            joinGroupTableDetailAccessor.insertJoingroupTableDetail(schemaName,
                                joinGroupTableDetailRecord.joinGroupId,
                                logicalTableName);
                        }
                    }
                }
            }
            if (tableGroupConfig.getTableGroupRecord() != null && StringUtils.isNotEmpty(joinGroup)) {
                JoinGroupInfoRecord joinGroupInfoRecords =
                    joinGroupInfoAccessor.getJoinGroupInfoByName(schemaName, joinGroup, false);
                joinGroupTableDetailAccessor.insertJoingroupTableDetail(schemaName, joinGroupInfoRecords.id,
                    logicalTableName);
            }
        }
        TableMetaChanger.addPartitionInfoMeta(metaDbConnection, tableGroupConfig, executionContext, false);
        if (localPartitionDefinitionInfo != null) {
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
        if (localPartitionDefinitionInfo != null) {
            new AddLocalPartitionTask(localPartitionDefinitionInfo).rollbackImpl(metaDbConnection, executionContext);
        }
        JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();
        joinGroupTableDetailAccessor.setConnection(metaDbConnection);
        joinGroupTableDetailAccessor.getJoinGroupDetailBySchemaTableName(schemaName, logicalTableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private boolean isCreateTableSupported(ExecutionContext executionContext) {
        return !(temporary || executionContext.isUseHint());
    }

    public TableGroupDetailConfig getTableGroupConfig() {
        return tableGroupConfig;
    }

    public void setTableGroupConfig(TableGroupDetailConfig tableGroupConfig) {
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
