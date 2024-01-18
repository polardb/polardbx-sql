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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesExtMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InsertIntoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.StoreTableLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateSelect;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateTable;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.List;
import java.sql.Connection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CreateTableJobFactory extends DdlJobFactory {

    public static final String CREATE_TABLE_CDC_MARK_TASK = "CREATE_TABLE_CDC_MARK_TASK";
    public static final String CREATE_TABLE_SHOW_TABLE_META_TASK = "CREATE_TABLE_SHOW_TABLE_META_TASK";
    public static final String CREATE_TABLE_SYNC_TASK = "CREATE_TABLE_SYNC_TASK";

    protected final boolean autoPartition;
    protected final boolean hasTimestampColumnDefault;
    protected final PhysicalPlanData physicalPlanData;
    protected final String schemaName;
    protected final String logicalTableName;
    protected final ExecutionContext executionContext;
    protected final Map<String, String> specialDefaultValues;
    protected final Map<String, Long> specialDefaultValueFlags;
    protected final List<ForeignKeyData> addedForeignKeys;
    protected final boolean fromTruncateTable;
    protected String selectSql;

    public CreateTableJobFactory(boolean autoPartition,
                                 boolean hasTimestampColumnDefault,
                                 Map<String, String> specialDefaultValues,
                                 Map<String, Long> specialDefaultValueFlags,
                                 List<ForeignKeyData> addedForeignKeys,
                                 PhysicalPlanData physicalPlanData,
                                 ExecutionContext executionContext) {
        this(autoPartition,
            hasTimestampColumnDefault,
            specialDefaultValues,
            specialDefaultValueFlags,
            addedForeignKeys,
            physicalPlanData,
            executionContext,
            false);
    }

    public CreateTableJobFactory(boolean autoPartition,
                                 boolean hasTimestampColumnDefault,
                                 Map<String, String> specialDefaultValues,
                                 Map<String, Long> specialDefaultValueFlags,
                                 List<ForeignKeyData> addedForeignKeys,
                                 PhysicalPlanData physicalPlanData,
                                 ExecutionContext executionContext,
                                 boolean fromTruncateTable) {
        this.autoPartition = autoPartition;
        this.hasTimestampColumnDefault = hasTimestampColumnDefault;
        this.physicalPlanData = physicalPlanData;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.executionContext = executionContext;
        this.specialDefaultValues = specialDefaultValues;
        this.specialDefaultValueFlags = specialDefaultValueFlags;
        this.addedForeignKeys = addedForeignKeys;
        this.fromTruncateTable = fromTruncateTable;
    }

    @Override
    protected void validate() {
    }

    public void setSelectSql(String sql) {
        selectSql = sql;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        CreateTableValidateTask validateTask =
            new CreateTableValidateTask(schemaName, logicalTableName, physicalPlanData.getTablesExtRecord());

        CreateTableAddTablesExtMetaTask addExtMetaTask =
            new CreateTableAddTablesExtMetaTask(schemaName, logicalTableName, physicalPlanData.isTemporary(),
                physicalPlanData.getTablesExtRecord(), autoPartition);

        CreateTablePhyDdlTask phyDdlTask = new CreateTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);

        CdcDdlMarkTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, !fromTruncateTable,
            CollectionUtils.isNotEmpty(addedForeignKeys));

        CreateTableAddTablesMetaTask addTableMetaTask =
            new CreateTableAddTablesMetaTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(), physicalPlanData.getSequence(),
                physicalPlanData.getTablesExtRecord(), physicalPlanData.isPartitioned(),
                physicalPlanData.isIfNotExists(), physicalPlanData.getKind(), addedForeignKeys,
                hasTimestampColumnDefault, specialDefaultValues, specialDefaultValueFlags);

        //Renew this one.
        LocalityDesc locality = physicalPlanData.getLocalityDesc();
        if (locality == null) {
            locality = new LocalityDesc();
        }
        StoreTableLocalityTask storeLocalityTask =
            new StoreTableLocalityTask(schemaName, logicalTableName, locality.toString(), false);

        CreateTableShowTableMetaTask showTableMetaTask =
            new CreateTableShowTableMetaTask(schemaName, logicalTableName);

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        ExecutableDdlJob4CreateTable result = new ExecutableDdlJob4CreateTable();
        // TODO(moyi) store locality and show table meta should be put in a transaction

        if (executionContext.getParamManager().getBoolean(ConnectionParams.CREATE_TABLE_SKIP_CDC)) {
            cdcDdlMarkTask = null;
        }
        List<DdlTask> taskList = Lists.newArrayList(
            validateTask,
            addExtMetaTask,
            phyDdlTask,
            addTableMetaTask,
            cdcDdlMarkTask,
            showTableMetaTask,
            storeLocalityTask,
            tableSyncTask);

        if (!GeneralUtil.isEmpty(addedForeignKeys)) {
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
        result.labelTask(CREATE_TABLE_CDC_MARK_TASK, cdcDdlMarkTask);
        result.labelTask(CREATE_TABLE_SHOW_TABLE_META_TASK, showTableMetaTask);
        result.labelTask(CREATE_TABLE_SYNC_TASK, tableSyncTask);

        result.setCreateTableValidateTask(validateTask);
        result.setCreateTableAddTablesExtMetaTask(addExtMetaTask);
        result.setCreateTablePhyDdlTask(phyDdlTask);
        result.setCreateTableAddTablesMetaTask(addTableMetaTask);
        result.setCdcDdlMarkTask(cdcDdlMarkTask);
        result.setCreateTableShowTableMetaTask(showTableMetaTask);
        result.setTableSyncTask(tableSyncTask);

        if (selectSql != null) {
            InsertIntoTask insertIntoTask = new InsertIntoTask(schemaName, logicalTableName, selectSql, null, 0);
            affectRows = insertIntoTask.getAffectRows();
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

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
