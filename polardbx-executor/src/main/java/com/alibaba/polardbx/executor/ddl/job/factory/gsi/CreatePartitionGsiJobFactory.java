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
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreatePartitionGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesPartitionInfoMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiPreValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiInsertIndexMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.polardbx.gms.metadb.table.IndexStatus.DELETE_ONLY;
import static com.alibaba.polardbx.gms.metadb.table.IndexStatus.WRITE_ONLY;

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

    final boolean indexAlignWithPrimaryTableGroup;
    final CreateGlobalIndexPreparedData globalIndexPreparedData;

    public CreatePartitionGsiJobFactory(CreateGlobalIndexPreparedData globalIndexPreparedData,
                                        PhysicalPlanData physicalPlanData,
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
            physicalPlanData,
            globalIndexPreparedData.getAddedForeignKeys(),
            executionContext
        );
        this.indexAlignWithPrimaryTableGroup = globalIndexPreparedData.isIndexAlignWithPrimaryTableGroup();
        this.globalIndexPreparedData = globalIndexPreparedData;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        if (isNeedToGetCreateTableGroupLock(true)) {
            resources.add(concatWithDot(schemaName, ConnectionProperties.ACQUIRE_CREATE_TABLE_GROUP_LOCK));
            executionContext.getExtraCmds().put(ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK.getName(), false);
        } else {
            super.excludeResources(resources);
            resources.add(concatWithDot(schemaName, primaryTableName));
            resources.add(concatWithDot(schemaName, indexTableName));

            TableGroupConfig tgConfig = physicalPlanData.getTableGroupConfig();
            for (TablePartRecordInfoContext entry : tgConfig.getTables()) {
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

        if (isNeedToGetCreateTableGroupLock(false)) {
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
                    physicalPlanData.getTableGroupConfig());

            List<String> columns = columnAst2nameStr(this.columns);
            List<String> coverings = columnAst2nameStr(this.coverings);

            final String finalStatus =
                executionContext.getParamManager().getString(ConnectionParams.GSI_FINAL_STATUS_DEBUG);
            final boolean stayAtDeleteOnly = StringUtils.equalsIgnoreCase(DELETE_ONLY.name(), finalStatus);
            final boolean stayAtWriteOnly = StringUtils.equalsIgnoreCase(WRITE_ONLY.name(), finalStatus);

            List<DdlTask> bringUpGsi = null;
            if (needOnlineSchemaChange) {
                TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(primaryTableName);
                boolean repartition = globalIndexPreparedData.isRepartition();
                bringUpGsi = GsiTaskFactory.addGlobalIndexTasks(
                    schemaName,
                    primaryTableName,
                    indexTableName,
                    stayAtDeleteOnly,
                    stayAtWriteOnly,
                    stayAtBackFill,
                    virtualColumnMap,
                    physicalPlanData,
                    tableMeta,
                    repartition,
                    executionContext.getOriginSql()
                );
            } else {
                bringUpGsi = GsiTaskFactory.createGlobalIndexTasks(
                    schemaName,
                    primaryTableName,
                    indexTableName
                );
            }
            CreateTableAddTablesPartitionInfoMetaTask createTableAddTablesPartitionInfoMetaTask =
                new CreateTableAddTablesPartitionInfoMetaTask(schemaName, indexTableName,
                    physicalPlanData.isTemporary(),
                    physicalPlanData.getTableGroupConfig(),  null, indexAlignWithPrimaryTableGroup, primaryTableName,
                    null);
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
                    specialDefaultValueFlags
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
                    needOnlineSchemaChange
                );
            addIndexMetaTask = (GsiInsertIndexMetaTask) addIndexMetaTask.onExceptionTryRecoveryThenRollback();

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
            taskList.addAll(bringUpGsi);

            DdlTask tableSyncTask = new TableSyncTask(schemaName, indexTableName);
            taskList.add(tableSyncTask);

            Map<String, TableMeta> tableMetaMap = executionContext.getSchemaManager().getCache();
            if (unique && !clusteredIndex && tableMetaMap != null
                && tableMetaMap.containsKey(primaryTableName.toLowerCase())) {
                // only create ugsi (not contains clustered ugsi)
                // create table with ugsi will not arrive here
                taskList.add(generateDropLocalIndexJob());
            }

            final ExecutableDdlJob4CreatePartitionGsi result = new ExecutableDdlJob4CreatePartitionGsi();
            result.addSequentialTasks(taskList);
            //todo delete me
            result.labelAsHead(validateTask);
            result.labelAsTail(tableSyncTask);

            result.setCreateGsiPreCheckTask(preValidateTask);
            result.setCreateGsiValidateTask(validateTask);
            result.setCreateTableAddTablesPartitionInfoMetaTask(createTableAddTablesPartitionInfoMetaTask);
            result.setCreateTableAddTablesMetaTask(addTablesMetaTask);
            result.setCreateTableShowTableMetaTask(showTableMetaTask);
            result.setGsiInsertIndexMetaTask(addIndexMetaTask);
            result.setCreateGsiPhyDdlTask(createGsiPhyDdlTask);
            result.setLastUpdateGsiStatusTask(bringUpGsi.get(bringUpGsi.size() - 1));

            result.setLastTask(tableSyncTask);

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
        List<String> params = Lists.newArrayList(
            ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK.getName() + "=false"
        );
        String hint = String.format("/*+TDDL:CMD_EXTRA(%s)*/", StringUtils.join(params, ","));
        return String.format(globalIndexPreparedData.getIndexTablePreparedData().getSourceSql());
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
        String ddl = "alter table " + indexTableName + " drop index " + TddlConstants.UGSI_PK_UNIQUE_INDEX_NAME;
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
            CreateGlobalIndexBuilder.create(ddl, globalIndexPreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();

        return CreateGsiJobFactory.create(globalIndexPreparedData, physicalPlanData, executionContext).create();
    }

    public static ExecutableDdlJob create4CreateTableWithGsi(@Deprecated DDL ddl,
                                                             CreateGlobalIndexPreparedData globalIndexPreparedData,
                                                             ExecutionContext ec) {
        CreateGlobalIndexBuilder builder = new CreatePartitionGlobalIndexBuilder(ddl, globalIndexPreparedData, ec);
        builder.build();

        boolean autoPartition = globalIndexPreparedData.getIndexTablePreparedData().isAutoPartition();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData(autoPartition);
        ec.getDdlContext().setIgnoreCdcGsiMark(true);
        CreateGsiJobFactory gsiJobFactory = new CreatePartitionGsiJobFactory(
            globalIndexPreparedData,
            physicalPlanData,
            ec
        );
        gsiJobFactory.needOnlineSchemaChange = false;
        return gsiJobFactory.create();
    }
}
