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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesPartitionInfoMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.StoreTableLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTable;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CreatePartitionTableJobFactory extends CreateTableJobFactory {

    public static final String CREATE_TABLE_ADD_TABLES_META_TASK = "CREATE_TABLE_ADD_TABLES_META_TASK";

    private CreateTablePreparedData preparedData;

    private List<Long> tableGroupIds = new ArrayList<>();

    private boolean checkSingleTgNotExists = false;

    private boolean checkBroadcastTgNotExists = false;

    private PartitionInfo partitionInfo;

    public CreatePartitionTableJobFactory(boolean autoPartition, boolean hasTimestampColumnDefault,
                                          Map<String, String> binaryColumnDefaultValues,
                                          PhysicalPlanData physicalPlanData, ExecutionContext executionContext,
                                          CreateTablePreparedData preparedData, PartitionInfo partitionInfo) {
        super(autoPartition, hasTimestampColumnDefault, binaryColumnDefaultValues, physicalPlanData, executionContext);
        this.preparedData = preparedData;
        this.partitionInfo = partitionInfo;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
        boolean isSigleTable = false;
        boolean isBroadCastTable = false;
        if (partitionInfo != null) {
            isSigleTable = partitionInfo.isGsiSingleOrSingleTable();
            isBroadCastTable = partitionInfo.isGsiBroadcastOrBroadcast();
        }

        TableGroupConfig tgConfig = physicalPlanData.getTableGroupConfig();
        for (TablePartRecordInfoContext entry : tgConfig.getTables()) {
            Long tableGroupId = entry.getLogTbRec().getGroupId();
            if (tableGroupId != null && tableGroupId != -1) {
                OptimizerContext oc =
                    Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
                TableGroupRecord record = tableGroupConfig.getTableGroupRecord();
                String tgName = record.getTg_name();
                resources.add(concatWithDot(schemaName, tgName));
                tableGroupIds.add(tableGroupId);
            }
        }

        if (preparedData.getTableGroupName() == null) {
            if (isSigleTable) {
                resources.add(concatWithDot(schemaName, TableGroupNameUtil.SINGLE_DEFAULT_TG_NAME_TEMPLATE));
            } else if (isBroadCastTable) {
                resources.add(concatWithDot(schemaName, TableGroupNameUtil.BROADCAST_TG_NAME_TEMPLATE));
            }
        }

        if (preparedData != null && preparedData.getTableGroupName() != null) {
            String tgName = RelUtils.stringValue(preparedData.getTableGroupName());
            if (TStringUtil.isNotBlank(tgName)) {
                resources.add(concatWithDot(schemaName, tgName));
            }
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = physicalPlanData.getSchemaName();
        FactoryUtils.checkDefaultTableGroup(
            schemaName,
            partitionInfo,
            physicalPlanData,
            preparedData.getTableGroupName() == null,
            checkSingleTgNotExists,
            checkBroadcastTgNotExists
        );

        CreatePartitionTableValidateTask validateTask =
            new CreatePartitionTableValidateTask(schemaName, logicalTableName,
                physicalPlanData.isIfNotExists(), physicalPlanData.getTableGroupConfig(), tableGroupIds,
                checkSingleTgNotExists, checkBroadcastTgNotExists);

        LocalPartitionDefinitionInfo localPartitionDefinitionInfo = preparedData.getLocalPartitionDefinitionInfo();
        CreateTableAddTablesPartitionInfoMetaTask addPartitionInfoTask =
            new CreateTableAddTablesPartitionInfoMetaTask(schemaName, logicalTableName, physicalPlanData.isTemporary(),
                physicalPlanData.getTableGroupConfig(), localPartitionDefinitionInfo, false, null);

        CreateTablePhyDdlTask phyDdlTask = new CreateTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);

        CreateTableAddTablesMetaTask createTableAddTablesMetaTask =
            new CreateTableAddTablesMetaTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(), physicalPlanData.getSequence(),
                physicalPlanData.getTablesExtRecord(), physicalPlanData.isPartitioned(),
                physicalPlanData.isIfNotExists(), physicalPlanData.getKind(), hasTimestampColumnDefault,
                binaryColumnDefaultValues);

        CreateTableShowTableMetaTask showTableMetaTask = new CreateTableShowTableMetaTask(schemaName, logicalTableName);

        CdcDdlMarkTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData);

        LocalityDesc locality = physicalPlanData.getLocalityDesc();
        StoreTableLocalityTask storeLocalityTask = locality == null ?
            null :
            new StoreTableLocalityTask(schemaName, logicalTableName, locality.toString(), false);

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        ExecutableDdlJob4CreatePartitionTable result = new ExecutableDdlJob4CreatePartitionTable();
        result.addSequentialTasks(Lists.newArrayList(
            validateTask,
            addPartitionInfoTask,
            phyDdlTask,
            createTableAddTablesMetaTask,
            cdcDdlMarkTask,
            showTableMetaTask,
            storeLocalityTask,
            tableSyncTask
        ).stream().filter(Objects::nonNull).collect(Collectors.toList()));

        //todo delete me
        result.labelAsHead(validateTask);
        result.labelAsTail(tableSyncTask);
        result.labelTask(CREATE_TABLE_ADD_TABLES_META_TASK, createTableAddTablesMetaTask);
        result.labelTask(CREATE_TABLE_SHOW_TABLE_META_TASK, showTableMetaTask);
        result.labelTask(CREATE_TABLE_SYNC_TASK, tableSyncTask);

        result.setCreatePartitionTableValidateTask(validateTask);
        result.setCreateTableAddTablesPartitionInfoMetaTask(addPartitionInfoTask);
        result.setCreateTablePhyDdlTask(phyDdlTask);
        result.setCreateTableAddTablesMetaTask(createTableAddTablesMetaTask);
        result.setCdcDdlMarkTask(cdcDdlMarkTask);
        result.setCreateTableShowTableMetaTask(showTableMetaTask);
        result.setTableSyncTask(tableSyncTask);

        return result;
    }

}
