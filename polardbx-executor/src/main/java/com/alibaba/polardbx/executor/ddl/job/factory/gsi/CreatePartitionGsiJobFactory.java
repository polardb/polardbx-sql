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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreatePartitionGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesPartitionInfoMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.AlterGsiAddLocalIndexTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiPreValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiInsertIndexMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupsSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.twophase.DnStats;
import com.alibaba.polardbx.executor.gsi.corrector.GsiChecker;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.StorageStatusManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import io.airlift.slice.DataSize;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gsi.GsiUtils.columnAst2nameStr;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.getAvaliableNodeNum;
import static com.alibaba.polardbx.gms.metadb.table.IndexStatus.DELETE_ONLY;
import static com.alibaba.polardbx.gms.metadb.table.IndexStatus.WRITE_ONLY;
import static io.airlift.slice.DataSize.*;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * 1. create [unique] global index
 * 2. alter table xxx add [unique] global index
 * <p>
 * for create table with [unique] gsi, see class: CreateTableWithGsiJobFactory
 *
 * @author guxu
 */
public class CreatePartitionGsiJobFactory extends CreateGsiJobFactory {

    List<Long> tableGroupIds = new ArrayList<>();

    final String tableGroupAlignWithTargetTable;
    final CreateGlobalIndexPreparedData globalIndexPreparedData;

    public CreatePartitionGsiJobFactory(CreateGlobalIndexPreparedData globalIndexPreparedData,
                                        PhysicalPlanData physicalPlanData,
                                        PhysicalPlanData physicalPlanDataForLocalIndex,
                                        Boolean create4CreateTableWithGsi,
                                        ExecutionContext executionContext) {
        super(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getIndexTableName(),
            globalIndexPreparedData.getColumns(),
            globalIndexPreparedData.getCoverings(),
            globalIndexPreparedData.isUnique(),
            globalIndexPreparedData.getComment(),
            globalIndexPreparedData.getIndexType(),
            globalIndexPreparedData.isClusteredIndex(),
            globalIndexPreparedData.getIndexTablePreparedData() != null
                && globalIndexPreparedData.getIndexTablePreparedData().isTimestampColumnDefault(),
            globalIndexPreparedData.getIndexTablePreparedData() != null ?
                globalIndexPreparedData.getIndexTablePreparedData().getSpecialDefaultValues() :
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER),
            globalIndexPreparedData.getIndexTablePreparedData() != null ?
                globalIndexPreparedData.getIndexTablePreparedData().getSpecialDefaultValueFlags() :
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER),
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_BACKFILL_BY_PK_RANGE),
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_BACKFILL_BY_PARTITION),
            physicalPlanData,
            physicalPlanDataForLocalIndex,
            create4CreateTableWithGsi,
            globalIndexPreparedData.getAddedForeignKeys(),
            executionContext
        );
        this.tableGroupAlignWithTargetTable = globalIndexPreparedData.getTableGroupAlignWithTargetTable();
        this.globalIndexPreparedData = globalIndexPreparedData;
        this.gsiCdcMark = !globalIndexPreparedData.isRepartition();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        if (!globalIndexPreparedData.isWithImplicitTableGroup() && isNeedToGetCreateTableGroupLock(true)) {
            resources.add(concatWithDot(schemaName, ConnectionProperties.ACQUIRE_CREATE_TABLE_GROUP_LOCK));
            executionContext.getExtraCmds().put(ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK.getName(), false);
        }
        if (isNeedToGetCreateTableGroupLock(true)) {
            resources.add(concatWithDot(schemaName, ConnectionProperties.ACQUIRE_CREATE_TABLE_GROUP_LOCK));
            executionContext.getExtraCmds().put(ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK.getName(), false);
        } else {
            super.excludeResources(resources);
            resources.add(concatWithDot(schemaName, primaryTableName));
            resources.add(concatWithDot(schemaName, indexTableName));

            TableGroupDetailConfig tgConfig = physicalPlanData.getTableGroupConfig();
            for (TablePartRecordInfoContext entry : tgConfig.getTablesPartRecordInfoContext()) {
                Long tableGroupId = entry.getLogTbRec().getGroupId();
                if (tableGroupId != null && tableGroupId != -1) {
                    tableGroupIds.add(tableGroupId);
                    OptimizerContext oc =
                        Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                    TableGroupConfig tableGroupConfig =
                        oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
                    String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
                    resources.add(concatWithDot(schemaName, tgName));
                }
            }
        }

    }

    @Override
    protected ExecutableDdlJob doCreate() {

        List<String> tableGroups = new ArrayList<>();
        if (needCreateImplicitTableGroup(tableGroups)) {
            rerunTask = generateCreateTableJob();
            DdlTask subJobTask = rerunTask;
            List<DdlTask> taskList = new ArrayList<>();
            ExecutableDdlJob job = new ExecutableDdlJob();
            CreateTableGroupValidateTask createTableGroupValidateTask =
                new CreateTableGroupValidateTask(globalIndexPreparedData.getSchemaName(),
                    tableGroups);
            taskList.add(createTableGroupValidateTask);
            List<DdlTask> createTableGroupAddMetaTasks = new ArrayList<>();
            for (int i = 0; i < tableGroups.size(); i++) {
                String tableGroupName = tableGroups.get(i);
                CreateTableGroupAddMetaTask createTableGroupAddMetaTask = new CreateTableGroupAddMetaTask(
                    globalIndexPreparedData.getSchemaName(), tableGroupName, null,
                    null, globalIndexPreparedData.isSingle(), true);
                createTableGroupAddMetaTasks.add(createTableGroupAddMetaTask);
            }

            TableGroupsSyncTask tableGroupsSyncTask =
                new TableGroupsSyncTask(globalIndexPreparedData.getSchemaName(), tableGroups);
            taskList.add(tableGroupsSyncTask);
            taskList.add(subJobTask);
            job.addSequentialTasks(taskList);
            for (int i = 0; i < createTableGroupAddMetaTasks.size(); i++) {
                job.addTaskRelationship(createTableGroupValidateTask, createTableGroupAddMetaTasks.get(i));
                job.addTaskRelationship(createTableGroupAddMetaTasks.get(i), tableGroupsSyncTask);
            }
            globalIndexPreparedData.setNeedToGetTableGroupLock(true);
            return job;
        } else if (isNeedToGetCreateTableGroupLock(false)) {
            DdlTask ddl = generateCreateTableJob();
            ExecutableDdlJob job = new ExecutableDdlJob();
            job.addSequentialTasks(Lists.newArrayList(ddl));
            globalIndexPreparedData.setNeedToGetTableGroupLock(true);
            return job;
        } else {
            CreateGsiPreValidateTask preValidateTask =
                new CreateGsiPreValidateTask(schemaName, primaryTableName, indexTableName, tableGroupIds,
                    physicalPlanData.getTableGroupConfig());
            CreateGsiValidateTask validateTask =
                new CreateGsiValidateTask(schemaName, primaryTableName, indexTableName, tableGroupIds,
                    physicalPlanData.getTableGroupConfig(), removePartitioning, false);

            List<String> columns = columnAst2nameStr(this.columns);
            List<String> coverings = columnAst2nameStr(this.coverings);

            Map<String, String> columnMapping = omcColumnMap == null ? null :
                omcColumnMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

            final String finalStatus =
                executionContext.getParamManager().getString(ConnectionParams.GSI_FINAL_STATUS_DEBUG);
            Boolean buildLocalIndexLater =
                executionContext.getParamManager().getBoolean(ConnectionParams.GSI_BUILD_LOCAL_INDEX_LATER);
            int gsiMaxParallelism = executionContext.getParamManager().getInt(ConnectionParams.GSI_JOB_MAX_PARALLELISM);
            final boolean stayAtDeleteOnly = StringUtils.equalsIgnoreCase(DELETE_ONLY.name(), finalStatus);
            final boolean stayAtWriteOnly = StringUtils.equalsIgnoreCase(WRITE_ONLY.name(), finalStatus);

            List<DdlTask> bringUpGsi = null;
            ExecutableDdlJob bringUpGsiDdlJob = new ExecutableDdlJob();
            if (useChangeSet) {
                // online modify column
                bringUpGsi = GsiTaskFactory.addGlobalIndexTasksChangeSet(
                    schemaName,
                    primaryTableName,
                    backfillSourceTableName,
                    indexTableName,
                    stayAtDeleteOnly,
                    stayAtWriteOnly,
                    stayAtBackFill,
                    srcVirtualColumnMap,
                    dstVirtualColumnMap,
                    dstColumnNewDefinitions,
                    modifyStringColumns,
                    onlineModifyColumn,
                    mirrorCopy,
                    physicalPlanData,
                    globalIndexPreparedData.getIndexPartitionInfo()
                );
            } else if (needOnlineSchemaChange && (splitByPkRange || splitByPartition)) {
                TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(primaryTableName);
                ParamManager pm = executionContext.getParamManager();
                boolean enableSample = pm.getBoolean(ConnectionParams.ENABLE_INNODB_BTREE_SAMPLING);
                String maxPkRangeSizeStr = pm.getString(ConnectionParams.BACKFILL_MAX_PK_RANGE_SIZE);
                String maxTaskPkRangeSizeStr = pm.getString(ConnectionParams.BACKFILL_MAX_TASK_PK_RANGE_SIZE);
                long maxPkRangeSize = convertToByte(maxPkRangeSizeStr);
                long maxTaskPkRangeSize = convertToByte(maxTaskPkRangeSizeStr);
                long maxSampleRows = pm.getLong(ConnectionParams.BACKFILL_MAX_SAMPLE_ROWS);
                long maxPkRangeSampleRows = pm.getLong(ConnectionParams.BACKFILL_MAX_SAMPLE_ROWS_FOR_PK_RANGE);
                int cpuAcquired = pm.getInt(ConnectionParams.GSI_PK_RANGE_CPU_ACQUIRE);
                int maxNodeNum = getAvaliableNodeNum(schemaName, primaryTableName, executionContext);
                // if you use pk range, then control the concurrency by cpuAcquired.
                gsiMaxParallelism = Math.floorDiv(100, cpuAcquired) * maxNodeNum;
                GsiChecker.Params params = GsiChecker.Params.buildFromExecutionContext(executionContext);
                bringUpGsiDdlJob = GsiTaskFactory.addGlobalIndexTasks(
                    schemaName,
                    primaryTableName,
                    backfillSourceTableName,
                    indexTableName,
                    stayAtDeleteOnly,
                    stayAtWriteOnly,
                    stayAtBackFill,
                    srcVirtualColumnMap,
                    null,
                    modifyStringColumns,
                    physicalPlanData,
                    physicalPlanDataForLocalIndex,
                    tableMeta,
                    gsiCdcMark,
                    onlineModifyColumn,
                    mirrorCopy,
                    executionContext.getOriginSql(),
                    params,
                    splitByPkRange,
                    splitByPartition,
                    enableSample,
                    maxTaskPkRangeSize,
                    maxPkRangeSize,
                    maxSampleRows,
                    maxPkRangeSampleRows,
                    gsiMaxParallelism,
                    cpuAcquired
                );
                physicalPlanDataForLocalIndex = null;
            } else if (needOnlineSchemaChange) {
                TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(primaryTableName);
                bringUpGsi = GsiTaskFactory.addGlobalIndexTasks(
                    schemaName,
                    primaryTableName,
                    backfillSourceTableName,
                    indexTableName,
                    stayAtDeleteOnly,
                    stayAtWriteOnly,
                    stayAtBackFill,
                    srcVirtualColumnMap,
                    dstVirtualColumnMap,
                    dstColumnNewDefinitions,
                    modifyStringColumns,
                    physicalPlanData,
                    tableMeta,
                    gsiCdcMark,
                    onlineModifyColumn,
                    mirrorCopy,
                    executionContext.getOriginSql()
                );
            } else {
                bringUpGsi = GsiTaskFactory.createGlobalIndexTasks(
                    schemaName,
                    primaryTableName,
                    indexTableName
                );
            }
            boolean autoCreateTg =
                executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_AUTO_CREATE_TABLEGROUP);
            CreateTableAddTablesPartitionInfoMetaTask createTableAddTablesPartitionInfoMetaTask =
                new CreateTableAddTablesPartitionInfoMetaTask(schemaName, indexTableName,
                    physicalPlanData.isTemporary(),
                    physicalPlanData.getTableGroupConfig(), null, null, tableGroupAlignWithTargetTable,
                    globalIndexPreparedData.isWithImplicitTableGroup(), false, primaryTableName,
                    null, false, autoCreateTg);
            CreateTableAddTablesMetaTask addTablesMetaTask =
                new CreateTableAddTablesMetaTask(
                    schemaName,
                    indexTableName,
                    physicalPlanData.getDefaultDbIndex(),
                    physicalPlanData.getDefaultPhyTableName(),
                    physicalPlanData.getSequence(),
                    physicalPlanData.getTablesExtRecord(),
                    physicalPlanData.isPartitioned(),
                    physicalPlanData.isIfNotExists(),
                    physicalPlanData.getKind(),
                    addedForeignKeys,
                    hasTimestampColumnDefault,
                    specialDefaultValues,
                    specialDefaultValueFlags,
                    columnMapping,
                    addNewColumns
                );
            CreateTableShowTableMetaTask showTableMetaTask =
                new CreateTableShowTableMetaTask(schemaName, indexTableName);
            showTableMetaTask = (CreateTableShowTableMetaTask) showTableMetaTask.onExceptionTryRecoveryThenRollback();
            GsiInsertIndexMetaTask addIndexMetaTask =
                new GsiInsertIndexMetaTask(
                    schemaName,
                    primaryTableName,
                    indexTableName,
                    columns,
                    coverings,
                    unique,
                    indexComment,
                    indexType,
                    IndexStatus.CREATING,
                    clusteredIndex,
                    globalIndexPreparedData.isVisible() ? IndexVisibility.VISIBLE : IndexVisibility.INVISIBLE,
                    needOnlineSchemaChange,
                    columnMapping,
                    addNewColumns
                );
            addIndexMetaTask = (GsiInsertIndexMetaTask) addIndexMetaTask.onExceptionTryRecoveryThenRollback();
            final ExecutableDdlJob4CreatePartitionGsi result = new ExecutableDdlJob4CreatePartitionGsi();

            List<DdlTask> taskList = new ArrayList<>();
            //1. validate
            taskList.add(validateTask);

            //2. create gsi table
            //2.1 insert tablePartition meta for gsi table
            taskList.add(createTableAddTablesPartitionInfoMetaTask);
            //2.2 create gsi physical table
            CreateGsiPhyDdlTask createGsiPhyDdlTask =
                new CreateGsiPhyDdlTask(schemaName, primaryTableName, indexTableName, physicalPlanData);
            taskList.add(createGsiPhyDdlTask);
            //2.3 insert tables meta for gsi table
            taskList.add(addTablesMetaTask);

            taskList.add(showTableMetaTask);
            //3.1 insert indexes meta for primary table
            taskList.add(addIndexMetaTask);
//        taskList.add(new GsiSyncTask(schemaName, primaryTableName, indexTableName));
            //3.2 gsi status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> PUBLIC
            result.addSequentialTasks(taskList);
            if (splitByPkRange || splitByPartition) {
                result.combineTasks(bringUpGsiDdlJob);
                result.addTaskRelationship(taskList.get(taskList.size() - 1), bringUpGsiDdlJob.getHead());
                taskList = new ArrayList<>();
            } else {
                taskList = new ArrayList<>();
                taskList.addAll(bringUpGsi);
            }

            if (buildLocalIndexLater && physicalPlanDataForLocalIndex != null) {
                AlterGsiAddLocalIndexTask alterGsiPhyTable =
                    new AlterGsiAddLocalIndexTask(schemaName, primaryTableName, indexTableName,
                        physicalPlanDataForLocalIndex);
                taskList.add(alterGsiPhyTable);
            }
            DdlTask tableSyncTask = new TableSyncTask(schemaName, indexTableName);
            taskList.add(tableSyncTask);

            Map<String, TableMeta> tableMetaMap = executionContext.getSchemaManager().getCache();
            boolean ugsiWithPk =
                executionContext.getParamManager().getBoolean(ConnectionParams.UNIQUE_GSI_WITH_PRIMARY_KEY);
            if (unique && !ugsiWithPk && !clusteredIndex && tableMetaMap != null
                && tableMetaMap.containsKey(primaryTableName.toLowerCase())) {
                // only create ugsi (not contains clustered ugsi)
                // create table with ugsi will not arrive here
                taskList.add(generateDropLocalIndexJob());
            }

            for (int i = 0; i < taskList.size(); i++) {
                result.appendTask(taskList.get(i));
            }
            result.labelAsHead(validateTask);
            result.labelAsTail(tableSyncTask);

            result.setCreateGsiPreCheckTask(preValidateTask);
            result.setCreateGsiValidateTask(validateTask);
            result.setCreateTableAddTablesPartitionInfoMetaTask(createTableAddTablesPartitionInfoMetaTask);
            result.setCreateTableAddTablesMetaTask(addTablesMetaTask);
            result.setCreateTableShowTableMetaTask(showTableMetaTask);
            result.setGsiInsertIndexMetaTask(addIndexMetaTask);
            result.setCreateGsiPhyDdlTask(createGsiPhyDdlTask);
            if (bringUpGsi != null) {
                result.setLastUpdateGsiStatusTask(bringUpGsi.get(bringUpGsi.size() - 1));
            } else {
                result.setLastUpdateGsiStatusTask(bringUpGsiDdlJob.getTail());
            }

            result.setLastTask(tableSyncTask);
            result.setMaxParallelism(gsiMaxParallelism);

            return result;
        }
    }

    private SubJobTask generateCreateTableJob() {
        String sql = genSubJobSql();
        SubJobTask subJobTask = new SubJobTask(schemaName, sql, null);
        subJobTask.setParentAcquireResource(true);
        return subJobTask;
    }

    private String genSubJobSql() {
        return globalIndexPreparedData.getIndexTablePreparedData().getSourceSql();
    }

    private SubJobTask generateDropLocalIndexJob() {
        String sql = genDropLocalIndexSubJobSql();
        SubJobTask subJobTask = new SubJobTask(schemaName, sql, null);
        subJobTask.setParentAcquireResource(true);
        return subJobTask;
    }

    private String genDropLocalIndexSubJobSql() {
        List<String> params = Lists.newArrayList(
            ConnectionParams.DDL_ON_GSI.getName() + "=true"
        );
        String hint = String.format("/*+TDDL:CMD_EXTRA(%s)*/", StringUtils.join(params, ","));
        String ddl = "alter table " + surroundWithBacktick(indexTableName) + " drop index "
            + TddlConstants.UGSI_PK_UNIQUE_INDEX_NAME;
        return hint + ddl;
    }

    private boolean isNeedToGetCreateTableGroupLock(boolean printLog) {
        boolean needToGetCreateTableGroupLock =
            executionContext.getParamManager().getBoolean(ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK);

        boolean lock = needToGetCreateTableGroupLock && physicalPlanData != null && StringUtils.isNotEmpty(
            globalIndexPreparedData.getIndexTablePreparedData().getSourceSql())
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
                if (globalIndexPreparedData.getIndexTablePreparedData().getSourceSql() != null) {
                    sb.append("globalIndexPreparedData.getIndexTablePreparedData().getSourceSql() is(");
                    sb.append(globalIndexPreparedData.getIndexTablePreparedData().getSourceSql());
                    sb.append(")");
                } else {
                    sb.append("globalIndexPreparedData.getIndexTablePreparedData().getSourceSql() is null,");
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

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          CreateGlobalIndexPreparedData globalIndexPreparedData,
                                          ExecutionContext executionContext) {
        DdlPhyPlanBuilder builder =
            CreateGlobalIndexBuilder.create(ddl, globalIndexPreparedData, null, executionContext).build();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();
        PhysicalPlanData physicalPlanDataForLocalIndex = null;
        if (builder.getPhysicalPlansForLocalIndex() != null) {
            physicalPlanDataForLocalIndex = builder.genPhysicalPlanData(false, true);
        }

        return CreateGsiJobFactory.create(globalIndexPreparedData, physicalPlanData, physicalPlanDataForLocalIndex,
            executionContext).create();
    }

    public static ExecutableDdlJob create4CreateTableWithGsi(@Deprecated DDL ddl,
                                                             CreateGlobalIndexPreparedData globalIndexPreparedData,
                                                             ExecutionContext ec) {
        CreateGlobalIndexBuilder builder =
            new CreatePartitionGlobalIndexBuilder(ddl, globalIndexPreparedData, null, false, ec);
        builder.build();

        boolean autoPartition = globalIndexPreparedData.getIndexTablePreparedData().isAutoPartition();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData(autoPartition);
        ec.getDdlContext().setIgnoreCdcGsiMark(true);
        CreateGsiJobFactory gsiJobFactory = new CreatePartitionGsiJobFactory(
            globalIndexPreparedData,
            physicalPlanData,
            null,
            true,
            ec
        );
        gsiJobFactory.needOnlineSchemaChange = false;
        return gsiJobFactory.create();
    }

    protected boolean needCreateImplicitTableGroup(List<String> tableGroups) {
        boolean ret = false;
        for (Map.Entry<String, Boolean> entry : this.globalIndexPreparedData.getRelatedTableGroupInfo().entrySet()) {
            if (entry.getValue()) {
                tableGroups.add(entry.getKey());
                ret = true;
            }
        }
        return ret;
    }


}
