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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateArchiveTableEventLogTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateEntitySecurityAttrTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesPartitionInfoMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InsertIntoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupsSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateSelect;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.LikeTableInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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
                                          Map<String, String> specialDefaultValues,
                                          Map<String, Long> specialDefaultValueFlags,
                                          List<ForeignKeyData> addedForeignKeys,
                                          PhysicalPlanData physicalPlanData, ExecutionContext executionContext,
                                          CreateTablePreparedData preparedData, PartitionInfo partitionInfo,
                                          LikeTableInfo likeTableInfo) {
        super(autoPartition, hasTimestampColumnDefault, specialDefaultValues, specialDefaultValueFlags,
            addedForeignKeys, physicalPlanData, preparedData.getDdlVersionId(), executionContext, likeTableInfo);
        this.preparedData = preparedData;
        this.partitionInfo = partitionInfo;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        if (!preparedData.isWithImplicitTableGroup() && isNeedToGetCreateTableGroupLock(true)) {
            resources.add(concatWithDot(schemaName, ConnectionProperties.ACQUIRE_CREATE_TABLE_GROUP_LOCK));
            executionContext.getExtraCmds().put(ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK.getName(), false);
        } else if (preparedData.isWithImplicitTableGroup()) {
            if (preparedData != null && preparedData.getTableGroupName() != null) {
                String tgName = RelUtils.stringValue(preparedData.getTableGroupName());
                TableGroupConfig tgConfig =
                    OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
                        .getTableGroupConfigByName(tgName);
                if (tgConfig == null) {
                    resources.add(concatWithDot(schemaName, tgName));
                }
            }
            super.excludeResources(resources);
        } else {
            super.excludeResources(resources);
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        boolean isSigleTable = false;
        boolean isBroadCastTable = false;
        if (partitionInfo != null) {
            isSigleTable = partitionInfo.isGsiSingleOrSingleTable();
            isBroadCastTable = partitionInfo.isGsiBroadcastOrBroadcast();
        }

        boolean matchTg = false;
        TableGroupDetailConfig tgConfig = physicalPlanData.getTableGroupConfig();
        for (TablePartRecordInfoContext entry : tgConfig.getTablesPartRecordInfoContext()) {
            Long tableGroupId = entry.getLogTbRec().getGroupId();
            if (tableGroupId != null && tableGroupId != -1) {
                OptimizerContext oc =
                    Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                TableGroupConfig tableGroupConfig =
                    oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
                TableGroupRecord record = tableGroupConfig.getTableGroupRecord();
                String tgName = record.getTg_name();
                resources.add(concatWithDot(schemaName, tgName));
                tableGroupIds.add(tableGroupId);
                matchTg = true;
            }
        }

        if (preparedData.getTableGroupName() == null) {
            if (isSigleTable && !matchTg) {
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

        if (preparedData != null && preparedData.getJoinGroupName() != null) {
            String jgName = RelUtils.stringValue(preparedData.getJoinGroupName());
            if (TStringUtil.isNotBlank(jgName)) {
                resources.add(concatWithDot(schemaName, jgName));
            }
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = physicalPlanData.getSchemaName();

        List<String> tableGroups = new ArrayList<>();
        if (needCreateImplicitTableGroup(tableGroups)) {
            DdlTask subJobTask = generateCreateTableJob();
            List<DdlTask> taskList = new ArrayList<>();
            ExecutableDdlJob job = new ExecutableDdlJob();
            CreateTableGroupValidateTask createTableGroupValidateTask =
                new CreateTableGroupValidateTask(preparedData.getSchemaName(),
                    tableGroups);
            taskList.add(createTableGroupValidateTask);
            List<DdlTask> createTableGroupAddMetaTasks = new ArrayList<>();
            for (int i = 0; i < tableGroups.size(); i++) {
                String tableGroupName = tableGroups.get(i);
                CreateTableGroupAddMetaTask createTableGroupAddMetaTask = new CreateTableGroupAddMetaTask(
                    preparedData.getSchemaName(), tableGroupName, null,
                    null, false, true);
                createTableGroupAddMetaTasks.add(createTableGroupAddMetaTask);
            }

            TableGroupsSyncTask tableGroupsSyncTask =
                new TableGroupsSyncTask(preparedData.getSchemaName(), tableGroups);
            taskList.add(tableGroupsSyncTask);
            taskList.add(subJobTask);
            job.addSequentialTasks(taskList);
            for (int i = 0; i < createTableGroupAddMetaTasks.size(); i++) {
                job.addTaskRelationship(createTableGroupValidateTask, createTableGroupAddMetaTasks.get(i));
                job.addTaskRelationship(createTableGroupAddMetaTasks.get(i), tableGroupsSyncTask);
            }
            preparedData.setNeedToGetTableGroupLock(true);
            return job;
        } else if (!preparedData.isWithImplicitTableGroup() && isNeedToGetCreateTableGroupLock(false)) {
            DdlTask ddl = generateCreateTableJob();
            ExecutableDdlJob job = new ExecutableDdlJob();
            job.addSequentialTasks(Lists.newArrayList(ddl));
            preparedData.setNeedToGetTableGroupLock(true);
            return job;
        } else {
            Pair<Boolean, Boolean> checkResult = FactoryUtils.checkDefaultTableGroup(
                schemaName,
                partitionInfo,
                physicalPlanData,
                preparedData.getTableGroupName() == null
            );
            checkSingleTgNotExists = checkResult.getKey();
            checkBroadcastTgNotExists = checkResult.getValue();

            TableGroupRecord tableGroupRecord = physicalPlanData.getTableGroupConfig().getTableGroupRecord();
            if (tableGroupRecord != null && (tableGroupRecord.id == null
                || tableGroupRecord.id == TableGroupRecord.INVALID_TABLE_GROUP_ID)
                && tableGroupRecord.getTg_type() == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG) {
                OptimizerContext oc =
                    Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager()
                    .getTableGroupConfigByName(TableGroupNameUtil.SINGLE_DEFAULT_TG_NAME_TEMPLATE);
                if (tableGroupConfig != null) {
                    tableGroupRecord.setTg_type(TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG);
                }
            }

            String joinGroup = null;
            if (preparedData != null && preparedData.getJoinGroupName() != null) {
                joinGroup = RelUtils.stringValue(preparedData.getJoinGroupName());
            }
            CreatePartitionTableValidateTask validateTask =
                new CreatePartitionTableValidateTask(schemaName, logicalTableName,
                    physicalPlanData.isIfNotExists(), physicalPlanData.getTableGroupConfig(),
                    physicalPlanData.getLocalityDesc(), tableGroupIds,
                    joinGroup, checkSingleTgNotExists, checkBroadcastTgNotExists);

            LocalPartitionDefinitionInfo localPartitionDefinitionInfo = preparedData.getLocalPartitionDefinitionInfo();
            boolean autoCreateTg =
                executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_AUTO_CREATE_TABLEGROUP);
            TtlDefinitionInfo ttlDefinitionInfo = preparedData.getTtlDefinitionInfo();

            CreateTableAddTablesPartitionInfoMetaTask addPartitionInfoTask =
                new CreateTableAddTablesPartitionInfoMetaTask(schemaName, logicalTableName,
                    physicalPlanData.isTemporary(),
                    physicalPlanData.getTableGroupConfig(), localPartitionDefinitionInfo, ttlDefinitionInfo, null,
                    preparedData.isWithImplicitTableGroup(), false, null,
                    joinGroup, false, autoCreateTg);

            CreateTablePhyDdlTask phyDdlTask =
                new CreateTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);

            CreateTableAddTablesMetaTask createTableAddTablesMetaTask =
                new CreateTableAddTablesMetaTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                    physicalPlanData.getDefaultPhyTableName(), physicalPlanData.getSequence(),
                    physicalPlanData.getTablesExtRecord(), physicalPlanData.isPartitioned(),
                    physicalPlanData.isIfNotExists(), physicalPlanData.getKind(), preparedData.getAddedForeignKeys(),
                    hasTimestampColumnDefault, specialDefaultValues, specialDefaultValueFlags, null, null);

            CreateTableShowTableMetaTask showTableMetaTask =
                new CreateTableShowTableMetaTask(schemaName, logicalTableName);

            CreateEntitySecurityAttrTask cesaTask = createCESATask();

            CdcDdlMarkTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, false,
                CollectionUtils.isNotEmpty(addedForeignKeys), versionId);

            CreateArchiveTableEventLogTask createArchiveTableEventLogTask = null;
            // TTL table
            if (localPartitionDefinitionInfo != null) {
                createArchiveTableEventLogTask =
                    new CreateArchiveTableEventLogTask(schemaName, logicalTableName, localPartitionDefinitionInfo);
            }

            TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);

            if (executionContext.getParamManager().getBoolean(ConnectionParams.CREATE_TABLE_SKIP_CDC)) {
                cdcDdlMarkTask = null;
            }
            ExecutableDdlJob4CreatePartitionTable result = new ExecutableDdlJob4CreatePartitionTable();

            List<DdlTask> taskList = new ArrayList<>();
            if (preparedData.isImportTable()) {
                taskList.addAll(
                    Lists.newArrayList(
                        validateTask,
                        addPartitionInfoTask,
                        createTableAddTablesMetaTask,
                        cdcDdlMarkTask,
                        showTableMetaTask,
                        createArchiveTableEventLogTask,
                        cesaTask,
                        tableSyncTask
                    ).stream().filter(Objects::nonNull).collect(Collectors.toList())
                );
            } else {
                taskList.addAll(
                    Lists.newArrayList(
                        validateTask,
                        addPartitionInfoTask,
                        phyDdlTask,
                        createTableAddTablesMetaTask,
                        cdcDdlMarkTask,
                        showTableMetaTask,
                        createArchiveTableEventLogTask,
                        cesaTask,
                        tableSyncTask
                    ).stream().filter(Objects::nonNull).collect(Collectors.toList())
                );
            }

            if (!GeneralUtil.isEmpty(preparedData.getAddedForeignKeys())) {
                // sync foreign key table meta
                for (ForeignKeyData addedForeignKey : addedForeignKeys) {
                    if (schemaName.equalsIgnoreCase(addedForeignKey.refSchema) &&
                        logicalTableName.equalsIgnoreCase(addedForeignKey.refTableName)) {
                        continue;
                    }
                    taskList.add(new TableSyncTask(addedForeignKey.refSchema, addedForeignKey.refTableName));
                }
            }

            result.addSequentialTasks(taskList.stream().filter(Objects::nonNull).collect(Collectors.toList()));

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
            result.setCreateArchiveTableEventLogTask(createArchiveTableEventLogTask);
            result.setTableSyncTask(tableSyncTask);

            if (selectSql != null) {
                InsertIntoTask
                    insertIntoTask = new InsertIntoTask(schemaName, logicalTableName, selectSql, null, 0);
                ExecutableDdlJob insertJob = new ExecutableDdlJob();
                insertJob.addTask(insertIntoTask);
                ExecutableDdlJob4CreateSelect ans = new ExecutableDdlJob4CreateSelect();
                ans.appendJob2(result);
                ans.appendJob2(insertJob);
                ans.setInsertTask(insertIntoTask);
                //insert 只能rollback，无法重试
                insertIntoTask.setExceptionAction(DdlExceptionAction.ROLLBACK);
                return ans;
            }

            return result;
        }
    }

    private SubJobTask generateCreateTableJob() {
        String sql = genSubJobSql();
        SubJobTask subJobTask = new SubJobTask(preparedData.getSchemaName(), sql, null);
        subJobTask.setParentAcquireResource(true);
        return subJobTask;
    }

    private String genSubJobSql() {
        return preparedData.getSourceSql();
    }

    private boolean isNeedToGetCreateTableGroupLock(boolean printLog) {
        boolean needToGetCreateTableGroupLock =
            executionContext.getParamManager().getBoolean(ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK);
        boolean lock = needToGetCreateTableGroupLock && physicalPlanData != null && StringUtils.isNotEmpty(
            preparedData.getSourceSql())
            && physicalPlanData.getTableGroupConfig() != null
            && physicalPlanData.getTableGroupConfig().getTableGroupRecord() != null
            && StringUtils.isEmpty(physicalPlanData.getTableGroupConfig().getTableGroupRecord().tg_name);
        if (!lock && printLog) {
            StringBuilder sb = new StringBuilder();
            sb.append("CreateTableGroupLock is false, detail:[");
            sb.append("needToGetCreateTableGroupLock is ");
            sb.append(needToGetCreateTableGroupLock);
            sb.append(",");
            if (physicalPlanData != null) {
                sb.append("physicalPlanData is not null,");
                if (preparedData.getSourceSql() != null) {
                    sb.append("preparedData.getSourceSql() is(");
                    sb.append(preparedData.getSourceSql());
                    sb.append(")");
                } else {
                    sb.append("preparedData.getSourceSql() is null,");
                }
                if (physicalPlanData.getTableGroupConfig() != null) {
                    sb.append("physicalPlanData.getTableGroupConfig() is not null,");
                    if (physicalPlanData.getTableGroupConfig().getTableGroupRecord() != null) {
                        sb.append("physicalPlanData.getTableGroupConfig().getTableGroupRecord() is not null,");
                        if (StringUtils.isEmpty(physicalPlanData.getTableGroupConfig().getTableGroupRecord().tg_name)) {
                            sb.append("physicalPlanData.getTableGroupConfig().getTableGroupRecord().tg_name is empty");
                        } else {
                            sb.append("physicalPlanData.getTableGroupConfig().getTableGroupRecord().tg_name is ");
                            sb.append(physicalPlanData.getTableGroupConfig().getTableGroupRecord().tg_name);
                        }
                    } else {
                        sb.append("physicalPlanData.getTableGroupConfig().getTableGroupRecord() is null");
                    }
                } else {
                    sb.append("physicalPlanData.getTableGroupConfig() is null");
                }
                sb.append("]");

            } else {
                sb.append("physicalPlanData is null]");
            }
            SQLRecorderLogger.ddlMetaLogger.warn(sb.toString());
        }
        return lock;
    }

    private boolean needCreateImplicitTableGroup(List<String> tableGroups) {
        boolean ret = false;
        for (Map.Entry<String, Boolean> entry : preparedData.getRelatedTableGroupInfo().entrySet()) {
            if (entry.getValue()) {
                tableGroups.add(entry.getKey());
                ret = true;
            }
        }
        return ret;
    }

}
