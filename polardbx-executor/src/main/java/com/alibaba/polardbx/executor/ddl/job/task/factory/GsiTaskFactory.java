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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableColumnBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiDropColumnCleanUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiInsertColumnMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexColumnStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.StatisticSampleTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4BringUpGsiTable;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * an interesting gsi-relevant task generator
 */
public class GsiTaskFactory {

    /**
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
     * do not change gsi phy table
     */
    public static List<DdlTask> alterGlobalIndexAddColumnsStatusTasks(String schemaName,
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

    private static String genAlterGlobalIndexAddColumnsSql(String primaryTableDefinition,
                                                           String indexName,
                                                           List<String> columns,
                                                           ExecutionContext executionContext) {
        List<String> columnsDef = new ArrayList<>();

        final MySqlCreateTableStatement astCreateIndexTable = (MySqlCreateTableStatement) SQLUtils
            .parseStatements(primaryTableDefinition, JdbcConstants.MYSQL).get(0).clone();

        String onUpdate = null;
        String defaultCurrentTime = null;
        String timestampWithoutDefault = null;

        final Iterator<SQLTableElement> it = astCreateIndexTable.getTableElementList().iterator();
        while (it.hasNext()) {
            final SQLTableElement tableElement = it.next();
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());

                if (columns.stream().anyMatch(columnName::equalsIgnoreCase)) {
                    if (columnDefinition.isAutoIncrement()) {
                        columnDefinition.setAutoIncrement(false);
                    }

                    final SQLExpr defaultExpr = columnDefinition.getDefaultExpr();
                    defaultCurrentTime = extractCurrentTimestamp(defaultCurrentTime, defaultExpr);

                    onUpdate = extractCurrentTimestamp(onUpdate, columnDefinition.getOnUpdate());

                    if ("timestamp".equalsIgnoreCase(columnDefinition.getDataType().getName()) && null == defaultExpr) {
                        timestampWithoutDefault = columnName;
                    }

                    columnsDef.add(SQLUtils.toSQLString(columnDefinition, com.alibaba.polardbx.druid.DbType.mysql));
                }
            }
        }

        final boolean defaultCurrentTimestamp =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_DEFAULT_CURRENT_TIMESTAMP);
        final boolean onUpdateCurrentTimestamp =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_ON_UPDATE_CURRENT_TIMESTAMP);

        if (null != defaultCurrentTime && !defaultCurrentTimestamp) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPARTITION_KEY,
                "cannot use DEFAULT " + defaultCurrentTime + " on partition key when gsi table exists");
        }

        if (null != onUpdate && !onUpdateCurrentTimestamp) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPARTITION_KEY,
                "cannot use ON UPDATE " + onUpdate + " on partition key when has gsi table exists");
        }

        if (null != timestampWithoutDefault && (!defaultCurrentTimestamp || !onUpdateCurrentTimestamp)) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPARTITION_KEY,
                "need default value other than CURRENT_TIMESTAMP for column `" + timestampWithoutDefault + "`");
        }

        if (columnsDef.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_COLUMN, columns.toString());
        }

        StringBuilder alterGsiTableSql = new StringBuilder("alter table " + indexName + " add column ");
        alterGsiTableSql.append(StringUtils.join(columnsDef.toArray(), ", add column "));

        return alterGsiTableSql.toString();
    }

    private static String extractCurrentTimestamp(String onUpdate, SQLExpr onUpdateExpr) {
        if (onUpdateExpr instanceof SQLCurrentTimeExpr || onUpdateExpr instanceof SQLMethodInvokeExpr) {
            try {
                if (onUpdateExpr instanceof SQLMethodInvokeExpr) {
                    SQLCurrentTimeExpr.Type.valueOf(((SQLMethodInvokeExpr) onUpdateExpr).getMethodName().toUpperCase());
                    onUpdate = SQLUtils.toMySqlString(onUpdateExpr);
                } else {
                    onUpdate = ((SQLCurrentTimeExpr) onUpdateExpr).getType().name;
                }
            } catch (Exception e) {
                // ignore error for ON UPDATE CURRENT_TIMESTAMP(3);
            }
        }
        return onUpdate;
    }

    /**
     * add global index column and change gsi phy table
     */
    public static AlterTableJobFactory alterGlobalIndexAddColumnFactory(String schemaName,
                                                                        String primaryTableName,
                                                                        String primaryTableDefinition,
                                                                        String indexName,
                                                                        List<String> columns,
                                                                        ExecutionContext executionContext) {
        String sql = genAlterGlobalIndexAddColumnsSql(primaryTableDefinition, indexName, columns, executionContext);
        AlterTableBuilder alterTableBuilder =
            AlterTableBuilder.createGsiAddColumnsBuilder(schemaName, indexName, sql, columns, executionContext);
        DdlPhyPlanBuilder builder = alterTableBuilder.build();
        PhysicalPlanData clusterIndexPlan = builder.genPhysicalPlanData();
        AlterTableJobFactory jobFactory = new AlterTableJobFactory(
            clusterIndexPlan,
            alterTableBuilder.getPreparedData(),
            alterTableBuilder.getLogicalAlterTable(),
            executionContext);
        jobFactory.validateExistence(false);
        jobFactory.withAlterGsi4Repartition(true, true, primaryTableName);
        return jobFactory;
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
