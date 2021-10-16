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

package com.alibaba.polardbx.executor.ddl.job.task.factory;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableColumnBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiDropColumnCleanUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiInsertColumnMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexColumnStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexStatusTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4BringUpGsiTable;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * an interesting gsi-relevant task generator
 */
public class GsiTaskFactory {

    /**
     * todo guxu refactor me
     * for
     * create table with gsi
     */
    public static List<DdlTask> createGlobalIndexTasks(String schemaName,
                                                       String primaryTableName,
                                                       String indexName) {
        List<DdlTask> taskList = new ArrayList<>();

        DdlTask publicTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.CREATING,
            IndexStatus.PUBLIC
        ).onExceptionTryRecoveryThenRollback();

        taskList.add(publicTask);
        return taskList;
    }

    /**
     * for
     * create global index
     * alter table add global index
     */
    public static List<DdlTask> addGlobalIndexTasks(String schemaName,
                                                    String primaryTableName,
                                                    String indexName,
                                                    boolean stayAtDeleteOnly,
                                                    boolean stayAtWriteOnly,
                                                    boolean stayAtBackFill) {
        List<DdlTask> taskList = new ArrayList<>();

        DdlTask deleteOnlyTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.CREATING,
            IndexStatus.DELETE_ONLY
        ).onExceptionTryRecoveryThenRollback();
        DdlTask writeOnlyTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.DELETE_ONLY,
            IndexStatus.WRITE_ONLY
        ).onExceptionTryRecoveryThenRollback();
        DdlTask writeReOrgTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.WRITE_ONLY,
            IndexStatus.WRITE_REORG
        ).onExceptionTryRecoveryThenRollback();
        DdlTask publicTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.WRITE_REORG,
            IndexStatus.PUBLIC
        ).onExceptionTryRecoveryThenRollback();

        taskList.add(deleteOnlyTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        if (stayAtDeleteOnly) {
            return taskList;
        }
        taskList.add(writeOnlyTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        if (stayAtWriteOnly) {
            return taskList;
        }
        taskList.add(new LogicalTableBackFillTask(schemaName, primaryTableName, indexName));
        if (stayAtBackFill) {
            return taskList;
        }
        taskList.add(writeReOrgTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        taskList.add(publicTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        return taskList;
    }

    /**
     * for
     * create global index
     * alter table add global index
     */
    public static ExecutableDdlJob4BringUpGsiTable addGlobalIndexTasks(String schemaName,
                                                                       String primaryTableName,
                                                                       String indexName) {
        ExecutableDdlJob4BringUpGsiTable executableDdlJob4BringUpGsiTable =
            new ExecutableDdlJob4BringUpGsiTable();

        GsiUpdateIndexStatusTask deleteOnlyTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.CREATING,
            IndexStatus.DELETE_ONLY
        );
        deleteOnlyTask.setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
        GsiUpdateIndexStatusTask writeOnlyTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.DELETE_ONLY,
            IndexStatus.WRITE_ONLY
        );
        writeOnlyTask.setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
        GsiUpdateIndexStatusTask writeReOrgTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.WRITE_ONLY,
            IndexStatus.WRITE_REORG
        );
        writeReOrgTask.setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
        GsiUpdateIndexStatusTask publicTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.WRITE_REORG,
            IndexStatus.PUBLIC
        );
        publicTask.setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);

        executableDdlJob4BringUpGsiTable.setDeleteOnlyTask(deleteOnlyTask);
        executableDdlJob4BringUpGsiTable.setSyncTaskAfterDeleteOnly(new TableSyncTask(schemaName, primaryTableName));

        executableDdlJob4BringUpGsiTable.setWriteOnlyTask(deleteOnlyTask);
        executableDdlJob4BringUpGsiTable.setSyncTaskAfterWriteOnly(new TableSyncTask(schemaName, primaryTableName));

        LogicalTableBackFillTask backFillTask = new LogicalTableBackFillTask(schemaName, primaryTableName, indexName);
        executableDdlJob4BringUpGsiTable.setLogicalTableBackFillTask(backFillTask);

        executableDdlJob4BringUpGsiTable.setWriteReorgTask(deleteOnlyTask);
        executableDdlJob4BringUpGsiTable.setSyncTaskAfterWriteReorg(new TableSyncTask(schemaName, primaryTableName));

        executableDdlJob4BringUpGsiTable.setPublicTask(deleteOnlyTask);
        executableDdlJob4BringUpGsiTable.setSyncTaskAfterPublic(new TableSyncTask(schemaName, primaryTableName));

        return executableDdlJob4BringUpGsiTable;
    }

    /**
     * for
     * drop index
     * alter table drop index
     */
    public static List<DdlTask> dropGlobalIndexTasks(String schemaName,
                                                     String primaryTableName,
                                                     String indexName) {
        List<DdlTask> taskList = new ArrayList<>();

        for (Pair<IndexStatus, IndexStatus> statusChange : IndexStatus.dropGsiStatusChange()) {
            DdlTask changeStatus = new GsiUpdateIndexStatusTask(
                schemaName,
                primaryTableName,
                indexName,
                statusChange.getKey(),
                statusChange.getValue()
            );
            taskList.add(changeStatus);
            taskList.add(new TableSyncTask(schemaName, primaryTableName));
        }

        return taskList;
    }

    /**
     * see changeAddColumnsStatusWithGsi()
     * <p>
     * for
     * cluster index add column
     * add index column
     */
    public static List<DdlTask> alterGlobalIndexAddColumnTasks(String schemaName,
                                                               String primaryTableName,
                                                               String indexName,
                                                               List<String> columns,
                                                               List<String> backfillColumns) {
        List<DdlTask> taskList = new ArrayList<>();

        // Insert meta
        DdlTask insertColumnMetaTask = new GsiInsertColumnMetaTask(schemaName, primaryTableName, indexName, columns);
        taskList.add(insertColumnMetaTask);

        // Add column
        for (Pair<TableStatus, TableStatus> change : TableStatus.schemaChangeForAddColumn()) {
            TableStatus before = change.getKey();
            TableStatus after = change.getValue();
            // change status
            DdlTask task = changeGsiColumnStatus(schemaName, primaryTableName, indexName, columns, before, after);
            taskList.add(task);

            // sync meta
            taskList.add(new TableSyncTask(schemaName, primaryTableName));

            // backfill
            if (after.equals(TableStatus.WRITE_REORG) && CollectionUtils.isNotEmpty(backfillColumns)) {
                DdlTask columnBackFillTask =
                    new LogicalTableColumnBackFillTask(schemaName, primaryTableName, indexName, backfillColumns);
                taskList.add(columnBackFillTask);
            }
        }

        return taskList;
    }

    /**
     * see changeAddColumnsStatusWithGsi()
     * <p>
     * for
     * cluster index drop column
     * drop index column
     */
    public static List<DdlTask> alterGlobalIndexDropColumnTasks(String schemaName,
                                                                String primaryTableName,
                                                                String indexName,
                                                                List<String> columns) {
        List<DdlTask> taskList = new ArrayList<>();

        DdlTask cleanUpTask =
            new GsiDropColumnCleanUpTask(schemaName, primaryTableName, indexName, columns);
        taskList.add(cleanUpTask);

        for (Pair<TableStatus, TableStatus> statusChange : TableStatus.schemaChangeForDropColumn()) {
            DdlTask changeStatus = changeGsiColumnStatus(schemaName, primaryTableName, indexName, columns,
                statusChange.getKey(), statusChange.getValue());
            taskList.add(changeStatus);
            taskList.add(new TableSyncTask(schemaName, primaryTableName));
        }

        return taskList;
    }

    private static GsiUpdateIndexColumnStatusTask changeGsiColumnStatus(String schemaName, String primaryTableName,
                                                                        String indexName, List<String> columns,
                                                                        TableStatus before, TableStatus after) {
        return new GsiUpdateIndexColumnStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            columns,
            before,
            after
        );
    }

}
