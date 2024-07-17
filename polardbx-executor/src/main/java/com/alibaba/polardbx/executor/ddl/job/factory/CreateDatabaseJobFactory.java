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

import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.CreateDatabaseEventLogTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableDataMigrationBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ChangeDatabaseReadWriteStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateDatabaseTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.GlobalAcquireMdlLockInDbSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.GlobalReleaseMdlLockInDbSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalConvertSequenceTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalTableStructureMigrationTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateDatabasePreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.alibaba.polardbx.executor.utils.DrdsToAutoTableCreationSqlUtil.buildCreateAutoModeDatabaseSql;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class CreateDatabaseJobFactory extends DdlJobFactory {

    protected final CreateDatabasePreparedData createDatabasePreparedData;

    protected final List<String> needDoCreationTables;
    protected final List<String> allTableNames;

    protected final Map<String, String> allTableCreationSqlInDrdsMode;
    protected final Map<String, String> allTableCreationSqlInAutoMode;

    protected final Map<String, CostEstimableDdlTask.CostInfo> allTableCostInfo;

    protected final int taskParallelism;

    public CreateDatabaseJobFactory(CreateDatabasePreparedData createDatabasePreparedData,
                                    Map<String, String> allTableCreationSqlInAutoMode,
                                    Map<String, String> allTableCreationSqlInDrdsMode,
                                    Map<String, CostEstimableDdlTask.CostInfo> costInfoMap,
                                    int taskParallelism) {
        this.createDatabasePreparedData = createDatabasePreparedData;
        this.needDoCreationTables = new ArrayList<>();
        this.allTableCreationSqlInAutoMode = new TreeMap<>(String::compareToIgnoreCase);
        this.allTableCreationSqlInAutoMode.putAll(allTableCreationSqlInAutoMode);
        this.allTableCostInfo = costInfoMap;
        this.taskParallelism = taskParallelism;
        this.allTableCreationSqlInDrdsMode = allTableCreationSqlInDrdsMode;

        if (!createDatabasePreparedData.getIncludeTables().isEmpty()) {
            needDoCreationTables.addAll(createDatabasePreparedData.getIncludeTables());
        } else if (!createDatabasePreparedData.getExcludeTables().isEmpty()) {
            Set<String> exclude = new TreeSet<>(String::compareToIgnoreCase);
            exclude.addAll(createDatabasePreparedData.getExcludeTables());

            allTableCreationSqlInAutoMode.keySet().forEach(tbName -> {
                if (!exclude.contains(tbName)) {
                    needDoCreationTables.add(tbName);
                }
            });
        } else {
            needDoCreationTables.addAll(
                allTableCreationSqlInAutoMode.keySet()
            );
        }

        this.allTableNames = new ArrayList<>();
        allTableNames.addAll(allTableCreationSqlInAutoMode.keySet());
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        //create database task
        String createDatabaseSql = buildCreateAutoModeDatabaseSql(
            createDatabasePreparedData.getSrcSchemaName(),
            createDatabasePreparedData.getDstSchemaName()
        );
        CreateDatabaseTask createDatabaseTask =
            new CreateDatabaseTask(SystemDbHelper.DEFAULT_DB_NAME, createDatabaseSql);

        //create table tasks
        Map<String, DdlTask> createTableTasks = new TreeMap<>(String::compareToIgnoreCase);
        needDoCreationTables.forEach(
            table -> {
                DdlTask createTableTask =
                    new LogicalTableStructureMigrationTask(
                        createDatabasePreparedData.getSrcSchemaName(),
                        createDatabasePreparedData.getDstSchemaName(),
                        table,
                        allTableCreationSqlInDrdsMode.get(table),
                        allTableCreationSqlInAutoMode.get(table)
                    );
                createTableTasks.put(table, createTableTask);
            }
        );

        //change database readWrite status
        DdlTask globalAcquireMdlLockInDbTask1 = new GlobalAcquireMdlLockInDbSyncTask(
            createDatabasePreparedData.getSrcSchemaName(),
            ImmutableSet.of(
                createDatabasePreparedData.getSrcSchemaName(),
                createDatabasePreparedData.getDstSchemaName()
            )
        );
        DdlTask globalAcquireMdlLockInDbTask2 = new GlobalAcquireMdlLockInDbSyncTask(
            createDatabasePreparedData.getSrcSchemaName(),
            ImmutableSet.of(
                createDatabasePreparedData.getSrcSchemaName(),
                createDatabasePreparedData.getDstSchemaName()
            )
        );
        DdlTask globalReleaseMdlLockInDbTask1 = new GlobalReleaseMdlLockInDbSyncTask(
            createDatabasePreparedData.getSrcSchemaName(),
            ImmutableSet.of(
                createDatabasePreparedData.getSrcSchemaName(),
                createDatabasePreparedData.getDstSchemaName()
            )
        );
        DdlTask globalReleaseMdlLockInDbTask2 =
            new GlobalReleaseMdlLockInDbSyncTask(createDatabasePreparedData.getSrcSchemaName(),
                ImmutableSet.of(
                    createDatabasePreparedData.getSrcSchemaName(),
                    createDatabasePreparedData.getDstSchemaName()
                )
            );

        Map<String, Integer> readOnlyStatus = ImmutableMap.of(
            createDatabasePreparedData.getSrcSchemaName(), DbInfoRecord.DB_READ_ONLY,
            createDatabasePreparedData.getDstSchemaName(), DbInfoRecord.DB_READ_ONLY
        );
        Map<String, Integer> readWriteStatus = ImmutableMap.of(
            createDatabasePreparedData.getSrcSchemaName(), DbInfoRecord.DB_READ_WRITE,
            createDatabasePreparedData.getDstSchemaName(), DbInfoRecord.DB_READ_WRITE
        );
        DdlTask changeDatabaseToReadOnlyTask =
            new ChangeDatabaseReadWriteStatusTask(createDatabasePreparedData.getSrcSchemaName(), readOnlyStatus);
        DdlTask changeDatabaseToReadWriteTask =
            new ChangeDatabaseReadWriteStatusTask(createDatabasePreparedData.getSrcSchemaName(), readWriteStatus);

        //convert sequence task
        DdlTask convertSequenceTask;
        if (!createDatabasePreparedData.isDoCreateTable()) {
            convertSequenceTask = new LogicalConvertSequenceTask(
                createDatabasePreparedData.getSrcSchemaName(),
                createDatabasePreparedData.getDstSchemaName(),
                ImmutableList.of(),
                true,
                true
            );
        } else if (!createDatabasePreparedData.getExcludeTables().isEmpty()
            || !createDatabasePreparedData.getIncludeTables().isEmpty()) {
            convertSequenceTask = new LogicalConvertSequenceTask(
                createDatabasePreparedData.getSrcSchemaName(),
                createDatabasePreparedData.getDstSchemaName(),
                needDoCreationTables,
                true,
                false
            );
        } else {
            convertSequenceTask = new LogicalConvertSequenceTask(
                createDatabasePreparedData.getSrcSchemaName(),
                createDatabasePreparedData.getDstSchemaName(),
                allTableNames,
                false,
                false
            );
        }

        //need data migration
        Map<String, DdlTask> dataMigrationTasks = new TreeMap<>(String::compareToIgnoreCase);
        if (createDatabasePreparedData.isAs()) {
            needDoCreationTables.forEach(
                table -> {
                    LogicalTableDataMigrationBackfillTask dataMigrationTask =
                        new LogicalTableDataMigrationBackfillTask(
                            createDatabasePreparedData.getSrcSchemaName(),
                            createDatabasePreparedData.getDstSchemaName(),
                            table,
                            table
                        );
                    dataMigrationTask.setCostInfo(allTableCostInfo.get(table));
                    dataMigrationTasks.put(table, dataMigrationTask);
                }
            );
        }

        //manufacture create database with lock
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.setMaxParallelism(taskParallelism);
        executableDdlJob.addTask(createDatabaseTask);
        if (needDoCreationTables.isEmpty()) {
            return executableDdlJob;
        }

        if (createDatabasePreparedData.isWithLock()) {
            executableDdlJob.addTask(globalAcquireMdlLockInDbTask1);
            executableDdlJob.addTask(changeDatabaseToReadOnlyTask);
            executableDdlJob.addTask(globalReleaseMdlLockInDbTask1);
            executableDdlJob.addTask(globalAcquireMdlLockInDbTask2);
            executableDdlJob.addTask(changeDatabaseToReadWriteTask);
            executableDdlJob.addTask(globalReleaseMdlLockInDbTask2);
            executableDdlJob.addTask(convertSequenceTask);
            for (String table : this.needDoCreationTables) {
                executableDdlJob.addTask(createTableTasks.get(table));
                executableDdlJob.addTaskRelationship(createDatabaseTask, createTableTasks.get(table));
                executableDdlJob.addTaskRelationship(createTableTasks.get(table), globalAcquireMdlLockInDbTask1);
            }
            executableDdlJob.addTaskRelationship(globalAcquireMdlLockInDbTask1, changeDatabaseToReadOnlyTask);
            executableDdlJob.addTaskRelationship(changeDatabaseToReadOnlyTask, globalReleaseMdlLockInDbTask1);
            executableDdlJob.addTaskRelationship(globalReleaseMdlLockInDbTask1, convertSequenceTask);
            if (createDatabasePreparedData.isAs()) {
                for (String table : this.needDoCreationTables) {
                    executableDdlJob.addTask(dataMigrationTasks.get(table));
                    executableDdlJob.addTaskRelationship(convertSequenceTask, dataMigrationTasks.get(table));
                    executableDdlJob.addTaskRelationship(dataMigrationTasks.get(table), globalAcquireMdlLockInDbTask2);
                }
            } else {
                executableDdlJob.addTaskRelationship(convertSequenceTask, globalAcquireMdlLockInDbTask2);
            }

            executableDdlJob.addTaskRelationship(globalAcquireMdlLockInDbTask2, changeDatabaseToReadWriteTask);
            executableDdlJob.addTaskRelationship(changeDatabaseToReadWriteTask, globalReleaseMdlLockInDbTask2);
        } else {
            executableDdlJob.addTask(convertSequenceTask);
            for (String table : this.needDoCreationTables) {
                executableDdlJob.addTask(createTableTasks.get(table));
                executableDdlJob.addTaskRelationship(createDatabaseTask, createTableTasks.get(table));
                executableDdlJob.addTaskRelationship(createTableTasks.get(table), convertSequenceTask);
            }
            if (createDatabasePreparedData.isAs()) {
                for (String table : this.needDoCreationTables) {
                    executableDdlJob.addTask(dataMigrationTasks.get(table));
                    executableDdlJob.addTaskRelationship(convertSequenceTask, dataMigrationTasks.get(table));
                }
            }
        }

        long rowCountSum = 0;
        for (String tableName : needDoCreationTables) {
            rowCountSum += allTableCostInfo.get(tableName).rows;
        }
        CreateDatabaseEventLogTask logTask = new CreateDatabaseEventLogTask(
            createDatabasePreparedData.getSrcSchemaName(),
            createDatabasePreparedData.getDstSchemaName(),
            createDatabasePreparedData.isLike(),
            (long) needDoCreationTables.size(),
            rowCountSum
        );
        executableDdlJob.appendTask(logTask);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        //forbid all database's ddl
        resources.add(createDatabasePreparedData.getSrcSchemaName());
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
