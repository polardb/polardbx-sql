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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableUpdateMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UnBindingArchiveTableMetaDirectTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.archive.TtlSourceInfo;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RecycleOssTableJobFactory extends DdlJobFactory {

    private final PhysicalPlanData physicalPlanData;
    private final String schemaName;
    private final String logicalTableName;
    private final String newLogicalTableName;
    private final boolean needRenamePhyTable;
    private final ExecutionContext executionContext;

    private String sourceSchemaName;

    private String sourceTableName;

    public RecycleOssTableJobFactory(PhysicalPlanData physicalPlanData, ExecutionContext executionContext) {
        this.physicalPlanData = physicalPlanData;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.newLogicalTableName = physicalPlanData.getNewLogicalTableName();
        this.needRenamePhyTable = physicalPlanData.isRenamePhyTable();
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        GsiValidator.validateAllowRenameOnTable(schemaName, logicalTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        DdlTask validateTask = new RenameTableValidateTask(schemaName, logicalTableName, newLogicalTableName);

        List<DdlTask> tasks = Lists.newArrayList();
        tasks.add(validateTask);
//        Optional<Pair<String, String>> source = CheckOSSArchiveUtil.getTTLSource(schemaName, logicalTableName);
        Optional<TtlSourceInfo> source = CheckOSSArchiveUtil.getTtlSourceInfo(schemaName, logicalTableName);
        source.ifPresent(x -> {

            boolean useRowLevelTtl = x.isUseRowLevelTtl();
            if (useRowLevelTtl) {
                sourceSchemaName = x.getTtlInfoRecord().getTableSchema();
                sourceTableName = x.getTtlInfoRecord().getTableName();
            } else {
                sourceSchemaName = x.getTableLocalPartitionRecord().getTableSchema();
                sourceTableName = x.getTableLocalPartitionRecord().getTableName();
            }

            TableMeta tm =
                OptimizerContext.getContext(sourceSchemaName).getLatestSchemaManager().getTable(sourceTableName);

            // validate version
            Map<String, Long> tableVersions = new HashMap<>();
            tableVersions.put(sourceTableName, tm.getVersion());
            ValidateTableVersionTask validateTableVersionTask =
                new ValidateTableVersionTask(sourceSchemaName, tableVersions);

            // unbinding task
            DdlTask unBindingTask = new UnBindingArchiveTableMetaDirectTask(sourceSchemaName, sourceTableName,
                x.isUseRowLevelTtl(),
                schemaName, logicalTableName);

            TableSyncTask tableSyncTask = new TableSyncTask(sourceSchemaName, sourceTableName);

            tasks.add(validateTableVersionTask);
            tasks.add(unBindingTask);
            tasks.add(tableSyncTask);

            if (useRowLevelTtl) {
                String ttlTmpSchema = x.getTtlInfoRecord().getArcTmpTblSchema();
                String ttlTmpTbl = x.getTtlInfoRecord().getArcTmpTblName();
                String dropSql = String.format("DROP TABLE IF EXISTS `%s`.`%s`", ttlTmpSchema, ttlTmpTbl);
                SubJobTask dropTtlTmpTblTask = new SubJobTask(ttlTmpSchema, dropSql, "");
                tasks.add(dropTtlTmpTblTask);
            }

        });
        DdlTask addMetaTask = new RenameTableAddMetaTask(schemaName, logicalTableName, newLogicalTableName);
        DdlTask phyDdlTask = new RenameTablePhyDdlTask(schemaName, physicalPlanData);
        DdlTask updateMetaTask =
            new RenameTableUpdateMetaTask(schemaName, logicalTableName, newLogicalTableName, needRenamePhyTable);
        DdlTask syncTask = new RenameTableSyncTask(schemaName, logicalTableName, newLogicalTableName);
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        tasks.add(addMetaTask);
        if (needRenamePhyTable) {
            tasks.add(phyDdlTask);
        }
        tasks.addAll(Lists.newArrayList(
            updateMetaTask,
            syncTask,
            tableSyncTask
        ));

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(tasks);
        executableDdlJob.labelAsHead(validateTask);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
        resources.add(concatWithDot(schemaName, newLogicalTableName));
        if (sourceSchemaName != null && sourceTableName != null) {
            resources.add(concatWithDot(sourceSchemaName, sourceTableName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
