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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameGsiUpdateMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableUpdateMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.RenameColumnarTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RenameTableJobFactory extends DdlJobFactory {

    private final PhysicalPlanData physicalPlanData;
    private final String schemaName;
    private final String logicalTableName;
    private final String newLogicalTableName;
    private final boolean needRenamePhyTables;
    private final ExecutionContext executionContext;
    protected final Long versionId;

    public RenameTableJobFactory(PhysicalPlanData physicalPlanData, ExecutionContext executionContext, Long versionId) {
        this.physicalPlanData = physicalPlanData;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.newLogicalTableName = physicalPlanData.getNewLogicalTableName();
        this.needRenamePhyTables = physicalPlanData.isRenamePhyTable();
        this.executionContext = executionContext;
        this.versionId = versionId;
    }

    @Override
    protected void validate() {
        GsiValidator.validateAllowRenameOnTable(schemaName, logicalTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        boolean isGsi = TableValidator.checkTableIsGsi(schemaName, logicalTableName);
        DdlTask validateTask = new RenameTableValidateTask(schemaName, logicalTableName, newLogicalTableName);
        DdlTask addMetaTask = new RenameTableAddMetaTask(schemaName, logicalTableName, newLogicalTableName);
        DdlTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, false, false, versionId);

        DdlTask phyDdlTask =
            new RenameTablePhyDdlTask(schemaName, physicalPlanData).onExceptionTryRecoveryThenRollback();
        DdlTask updateMetaTask;
        DdlTask syncTask;
        if (isGsi) {
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
            String primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;

            updateMetaTask =
                new RenameGsiUpdateMetaTask(schemaName, primaryTableName, logicalTableName, newLogicalTableName,
                    needRenamePhyTables);
            syncTask = new TableSyncTask(schemaName, primaryTableName);
        } else {
            updateMetaTask =
                new RenameTableUpdateMetaTask(schemaName, logicalTableName, newLogicalTableName, needRenamePhyTables);
            syncTask = new RenameTableSyncTask(schemaName, logicalTableName, newLogicalTableName);
        }

        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        TableMeta tableMeta = schemaManager.getTable(logicalTableName);
        boolean withColumnar = tableMeta.withColumnar();

        List<DdlTask> taskList = new ArrayList<>();
        taskList.add(validateTask);
        taskList.add(addMetaTask);
        if (needRenamePhyTables) {
            taskList.add(phyDdlTask);
        }
        Engine engine =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName).getEngine();
        if (!Engine.isFileStore(engine) && !isGsi) {
            if (withColumnar) {
                DdlTask renameColumnarTask =
                    new RenameColumnarTableMetaTask(schemaName, logicalTableName, newLogicalTableName, versionId);
                taskList.add(renameColumnarTask);
            }
            taskList.add(cdcDdlMarkTask);
        }
        taskList.add(updateMetaTask);
        taskList.add(syncTask);

        // sync foreign key table meta
        taskList.addAll(FactoryUtils.getFkTableSyncTasks(schemaName, logicalTableName));

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(validateTask);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
        resources.add(concatWithDot(schemaName, newLogicalTableName));

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPartDb) {
            String tgName = FactoryUtils.getTableGroupNameByTableName(schemaName, logicalTableName);
            if (tgName != null) {
                resources.add(concatWithDot(schemaName, tgName));
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, schemaName, logicalTableName);
            }
        }

        // exclude foreign key tables
        FactoryUtils.getFkTableExcludeResources(schemaName, logicalTableName, resources);
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
