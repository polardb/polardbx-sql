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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.OnlineModifyColumnBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcOnlineModifyDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CheckColumnTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnDropMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnInitMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnStopMultiWriteTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnSwapMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.Attribute.ALTER_TABLE_ALGORITHM_OMC_INDEX;

public class AlterTableOnlineModifyColumnJobFactory extends DdlJobFactory {
    private final PhysicalPlanData physicalPlahData;
    private final String schemaName;
    private final String logicalTableName;
    private final AlterTablePreparedData prepareData;
    private final AlterTableWithGsiPreparedData gsiPreparedData;
    private final LogicalAlterTable logicalAlterTable;
    private final TableGroupConfig tableGroupConfig;

    private final String oldColumnType;
    private final String newColumnType;
    private final boolean withUniqueConstraint;

    private final String oldColumnName; // source column in multi-write
    private final String newColumnName; // target column in multi-write

    private final String dropColumnName; // column to be dropped
    private final String keepColumnName; // column to be kept

    private final String afterColumnName; // target column position, null if first

    private final String dbIndex;
    private final String phyTableName;

    private final List<PhysicalPlanData> gsiPhysicalPlanData;

    private final Map<String, List<String>> localIndexes;

    private final boolean oldColumnNullable;
    private final boolean newColumnNullable;

    private final boolean isChange;

    private final boolean useChecker;
    private final boolean useSimpleChecker;
    private final SQLColumnDefinition newColumnTypeForChecker;
    private final String checkerColumnName;

    private final SQLAlterTableItem alterTableItem;

    // DEBUG ONLY
    private final boolean skipBackfill;

    private final ExecutionContext executionContext;

    public AlterTableOnlineModifyColumnJobFactory(PhysicalPlanData physicalPlanData,
                                                  List<PhysicalPlanData> gsiPhysicalPlanData,
                                                  AlterTablePreparedData preparedData,
                                                  AlterTableWithGsiPreparedData gsiPreparedData,
                                                  LogicalAlterTable logicalAlterTable,
                                                  ExecutionContext executionContext) {
        this.physicalPlahData = physicalPlanData;
        this.schemaName = logicalAlterTable.getSchemaName();
        this.logicalTableName = logicalAlterTable.getTableName();
        this.gsiPhysicalPlanData = gsiPhysicalPlanData;
        this.prepareData = preparedData;
        this.gsiPreparedData = gsiPreparedData;
        this.logicalAlterTable = logicalAlterTable;
        this.executionContext = executionContext;

        this.dbIndex = physicalPlanData.getDefaultDbIndex();
        this.phyTableName = physicalPlanData.getDefaultPhyTableName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        this.tableGroupConfig = isNewPart ? physicalPlanData.getTableGroupConfig() : null;

        this.oldColumnType = preparedData.getModifyColumnType();

        String origSql = executionContext.getDdlContext().getDdlStmt();
        SQLAlterTableStatement alterTableStmt = (SQLAlterTableStatement) FastsqlUtils.parseSql(origSql).get(0);
        SQLAlterTableItem alterItem = alterTableStmt.getItems().get(0);

        if (alterItem instanceof MySqlAlterTableChangeColumn) {
            MySqlAlterTableChangeColumn changeColumn = (MySqlAlterTableChangeColumn) alterItem;
            String oldColumnName = SQLUtils.normalizeNoTrim(changeColumn.getColumnName().getSimpleName());
            String newColumnName = SQLUtils.normalizeNoTrim(changeColumn.getNewColumnDefinition().getColumnName());
            if (oldColumnName.equalsIgnoreCase(newColumnName)) {
                // Rewrite to modify
                MySqlAlterTableModifyColumn modifyColumn = new MySqlAlterTableModifyColumn();
                modifyColumn.setNewColumnDefinition(changeColumn.getNewColumnDefinition().clone());
                modifyColumn.getNewColumnDefinition().setName(changeColumn.getColumnName());
                modifyColumn.setFirst(changeColumn.isFirst());
                modifyColumn.setFirstColumn(changeColumn.getFirstColumn());
                modifyColumn.setAfterColumn(changeColumn.getAfterColumn());
                alterTableStmt.getItems().set(0, modifyColumn);
                alterItem = modifyColumn;
            }
        }
        this.alterTableItem = alterItem;

        boolean modifyPosition;
        if (alterItem instanceof MySqlAlterTableChangeColumn) {
            MySqlAlterTableChangeColumn changeColumn = (MySqlAlterTableChangeColumn) alterItem;
            this.oldColumnName = SQLUtils.normalizeNoTrim(changeColumn.getColumnName().getSimpleName());
            this.newColumnName = SQLUtils.normalizeNoTrim(changeColumn.getNewColumnDefinition().getColumnName());
            this.dropColumnName = this.oldColumnName;
            this.keepColumnName = this.newColumnName;
            this.isChange = true;
            modifyPosition = changeColumn.isFirst() || changeColumn.getAfterColumn() != null;
            if (!modifyPosition) {
                this.afterColumnName = oldColumnName;
            } else if (changeColumn.getAfterColumn() != null) {
                this.afterColumnName = SQLUtils.normalizeNoTrim(changeColumn.getAfterColumn().getSimpleName());
            } else {
                this.afterColumnName = "";
            }
            this.newColumnTypeForChecker = changeColumn.getNewColumnDefinition().clone();
            this.newColumnType = TableColumnUtils.getDataDefFromColumnDefWithoutUnique(this.newColumnName,
                changeColumn.getNewColumnDefinition().toString());
        } else {
            MySqlAlterTableModifyColumn modifyColumn = (MySqlAlterTableModifyColumn) alterItem;
            this.oldColumnName = SQLUtils.normalizeNoTrim(modifyColumn.getNewColumnDefinition().getColumnName());
            this.newColumnName = SQLUtils.normalizeNoTrim(preparedData.getTmpColumnName());
            this.dropColumnName = this.newColumnName;
            this.keepColumnName = this.oldColumnName;
            this.isChange = false;
            modifyPosition = modifyColumn.isFirst() || modifyColumn.getAfterColumn() != null;
            if (!modifyPosition) {
                this.afterColumnName = oldColumnName;
            } else if (modifyColumn.getAfterColumn() != null) {
                this.afterColumnName = SQLUtils.normalizeNoTrim(modifyColumn.getAfterColumn().getSimpleName());
            } else {
                this.afterColumnName = "";
            }
            this.newColumnTypeForChecker = modifyColumn.getNewColumnDefinition().clone();
            // newColumnType only for swap, so drop unique constraint since we have created unique key
            this.newColumnType = TableColumnUtils.getDataDefFromColumnDefWithoutUnique(this.oldColumnName,
                modifyColumn.getNewColumnDefinition().toString());
        }
        this.withUniqueConstraint = this.newColumnTypeForChecker.getConstraints().stream()
            .anyMatch(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLColumnUniqueKey);

        // remove not nullable, unique, default value, comment, on update
        this.newColumnTypeForChecker.getConstraints()
            .removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLNotNullConstraint);
        this.newColumnTypeForChecker.getConstraints()
            .removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLColumnUniqueKey);
        this.newColumnTypeForChecker.setDefaultExpr(null);
        this.newColumnTypeForChecker.setComment((SQLExpr) null);
        this.newColumnTypeForChecker.setOnUpdate(null);

        this.localIndexes = new HashMap<>();
        for (String tableName : preparedData.getLocalIndexMeta().keySet()) {
            List<String> localIndex = new ArrayList<>(preparedData.getLocalIndexMeta().get(tableName).keySet());
            // We add new local index before swap meta, so we also need to add new index here
            localIndex.addAll(preparedData.getLocalIndexNewNameMap().get(tableName).values());
            localIndexes.put(tableName, localIndex);
        }

        this.oldColumnNullable = preparedData.isOldColumnNullable();
        this.newColumnNullable = preparedData.isNewColumnNullable();

        this.useChecker = preparedData.isUseChecker();
        this.useSimpleChecker = prepareData.isUseSimpleChecker();
        this.checkerColumnName = preparedData.getCheckerColumnName();

        this.skipBackfill = preparedData.isSkipBackfill();
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        List<String> coveringGsi =
            gsiPhysicalPlanData.stream().map(PhysicalPlanData::getLogicalTableName).collect(Collectors.toList());
        List<String> gsiDbIndex =
            gsiPhysicalPlanData.stream().map(PhysicalPlanData::getDefaultDbIndex).collect(Collectors.toList());
        List<String> gsiPhyTableName =
            gsiPhysicalPlanData.stream().map(PhysicalPlanData::getDefaultPhyTableName).collect(Collectors.toList());

        DdlTask validateTask = new AlterTableValidateTask(schemaName, logicalTableName,
            logicalAlterTable.getSqlAlterTable().getSourceSql(), prepareData.getTableVersion(), tableGroupConfig);

        DdlTask initTableUpdateStatusTask =
            new OnlineModifyColumnInitMetaTask(schemaName, logicalTableName, oldColumnName);
        DdlTask initTableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        List<List<DdlTask>> addColumnPhyTasks = new ArrayList<>();
        addColumnPhyTasks.add(ImmutableList.of(genAddColumnPhyTask(logicalTableName, false)));
        for (String gsiName : coveringGsi) {
            addColumnPhyTasks.add(ImmutableList.of(genAddColumnPhyTask(gsiName, true)));
        }

        DdlTask addColumnLogicalTask =
            new OnlineModifyColumnAddMetaTask(schemaName, logicalTableName, isChange, dbIndex, phyTableName,
                newColumnName, oldColumnName, afterColumnName, coveringGsi, gsiDbIndex, gsiPhyTableName);
        DdlTask addColumnTableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        List<List<DdlTask>> columnBackFillTasks = new ArrayList<>();
        if (!skipBackfill) {
            columnBackFillTasks.add(ImmutableList.of(
                new OnlineModifyColumnBackFillTask(schemaName, logicalTableName, oldColumnName, newColumnName)));
            for (String gsiName : coveringGsi) {
                columnBackFillTasks.add(ImmutableList.of(
                    new OnlineModifyColumnBackFillTask(schemaName, gsiName, oldColumnName, newColumnName)));
            }
        }

        List<List<DdlTask>> swapColumnPhyTasks = new ArrayList<>();
        swapColumnPhyTasks.add(ImmutableList.of(genSwapColumnPhyTask(logicalTableName)));
        for (String gsiName : coveringGsi) {
            swapColumnPhyTasks.add(ImmutableList.of(genSwapColumnPhyTask(gsiName)));
        }

        DdlTask swapColumnLogicalTask =
            new OnlineModifyColumnSwapMetaTask(schemaName, logicalTableName, isChange, dbIndex, phyTableName,
                newColumnName, oldColumnName, afterColumnName, localIndexes, prepareData.getNewUniqueIndexNameMap(),
                coveringGsi, gsiDbIndex, gsiPhyTableName, newColumnNullable);
        DdlTask swapColumnTableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        DdlTask cdcDdlMarkTask = new CdcOnlineModifyDdlMarkTask(schemaName, physicalPlahData);

        DdlTask stopMultiWriteTask =
            new OnlineModifyColumnStopMultiWriteTask(schemaName, logicalTableName, isChange, newColumnName,
                oldColumnName);
        DdlTask stopMultiWriteTableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        List<List<DdlTask>> dropColumnPhyTasks = new ArrayList<>();
        dropColumnPhyTasks.add(ImmutableList.of(genDropColumnPhyTask(logicalTableName)));
        for (String gsiName : coveringGsi) {
            dropColumnPhyTasks.add(ImmutableList.of(genDropColumnPhyTask(gsiName)));
        }

        DdlTask dropColumnLogicalTask =
            new OnlineModifyColumnDropMetaTask(schemaName, logicalTableName, isChange, dbIndex, phyTableName,
                newColumnName, oldColumnName, coveringGsi, gsiDbIndex, gsiPhyTableName, withUniqueConstraint);
        DdlTask dropTableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        List<List<DdlTask>> columnNotNullableTasks = new ArrayList<>();
        if (!newColumnNullable) {
            columnNotNullableTasks.add(
                ImmutableList.of(genColumnNotNullableTasks(newColumnName, newColumnType, logicalTableName)));
            for (String gsiName : coveringGsi) {
                columnNotNullableTasks.add(
                    ImmutableList.of(genColumnNotNullableTasks(newColumnName, newColumnType, gsiName)));
            }
        }

        List<List<DdlTask>> columnNullableTasks = new ArrayList<>();
        if (!oldColumnNullable) {
            columnNullableTasks.add(
                ImmutableList.of(genColumnNullableTasks(dropColumnName, oldColumnType, logicalTableName)));
            for (String gsiName : coveringGsi) {
                columnNullableTasks.add(
                    ImmutableList.of(genColumnNullableTasks(dropColumnName, oldColumnType, gsiName)));
            }
        }

        List<List<DdlTask>> checkColumnTasks = new ArrayList<>();
        if (useChecker) {
            checkColumnTasks.add(genCheckerTasks(logicalTableName));
            for (String gsiName : coveringGsi) {
                checkColumnTasks.add(genCheckerTasks(gsiName));
            }
        }

        DdlTask barrierTask1 = new EmptyTask(schemaName);
        DdlTask barrierTask2 = new EmptyTask(schemaName);
        DdlTask barrierTask3 = new EmptyTask(schemaName);
        DdlTask barrierTask4 = new EmptyTask(schemaName);
        DdlTask barrierTask5 = new EmptyTask(schemaName);

        List<List<DdlTask>> addLocalIndexTasks = genAddLocalIndexTasks();

        List<List<DdlTask>> swapAndDropLocalIndexTasks = genSwapAndDropLocalIndexTasks();
        List<DdlTask> allTasks = Lists.newArrayList(
            validateTask,
            initTableUpdateStatusTask,
            initTableSyncTask,
            addColumnLogicalTask,
            addColumnTableSyncTask,
            barrierTask1,
            barrierTask2,
            barrierTask3,
            barrierTask4,
            swapColumnLogicalTask,
            cdcDdlMarkTask,
            swapColumnTableSyncTask,
            barrierTask5,
            stopMultiWriteTask,
            stopMultiWriteTableSyncTask,
            dropColumnLogicalTask,
            dropTableSyncTask
        );

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(allTasks);

        addConcurrentTasks(executableDdlJob, initTableSyncTask, addColumnLogicalTask, addColumnPhyTasks);
        addConcurrentTasks(executableDdlJob, addColumnTableSyncTask, barrierTask1, columnBackFillTasks);
        addConcurrentTasks(executableDdlJob, barrierTask1, barrierTask2, columnNotNullableTasks);
        addConcurrentTasks(executableDdlJob, barrierTask2, barrierTask3, addLocalIndexTasks);
        addConcurrentTasks(executableDdlJob, barrierTask3, barrierTask4, checkColumnTasks);
        addConcurrentTasks(executableDdlJob, barrierTask4, swapColumnLogicalTask, swapColumnPhyTasks);
        addConcurrentTasks(executableDdlJob, swapColumnTableSyncTask, barrierTask5, swapAndDropLocalIndexTasks);
        addConcurrentTasks(executableDdlJob, barrierTask5, stopMultiWriteTask, columnNullableTasks);
        addConcurrentTasks(executableDdlJob, stopMultiWriteTableSyncTask, dropColumnLogicalTask, dropColumnPhyTasks);

        executableDdlJob.setExceptionActionForAllSuccessor(validateTask, DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
        // TODO(qianjing): rollback swapColumnLogicalTask
        executableDdlJob.setExceptionActionForAllSuccessor(swapColumnLogicalTask, DdlExceptionAction.PAUSE);
        // Do not try to recover SubJobTask since it already tries to recover during subjob execution
        for (List<DdlTask> addLocalIndexTask : addLocalIndexTasks) {
            for (DdlTask ddlTask : addLocalIndexTask) {
                ddlTask.setExceptionAction(DdlExceptionAction.ROLLBACK);
            }
        }

        executableDdlJob.labelAsHead(validateTask);
        return executableDdlJob;
    }

    private List<DdlTask> genCheckerTasks(String tableName) {
        List<DdlTask> result = new ArrayList<>();
        // Simple checker only checks if both columns are null or not null
        if (useSimpleChecker) {
            result.add(new CheckColumnTask(schemaName, tableName, oldColumnName, oldColumnName, newColumnName, true));
        } else {
            String checkerColumnType = TableColumnUtils.getDataDefFromColumnDef(keepColumnName,
                newColumnTypeForChecker.toString());
            String addSql =
                String.format("ALTER TABLE `%s` ADD COLUMN `%s` %s GENERATED ALWAYS AS (ALTER_TYPE(`%s`)) VIRTUAL",
                    tableName,
                    checkerColumnName, checkerColumnType, oldColumnName);
            String dropSql = String.format("ALTER TABLE `%s` DROP COLUMN `%s`", tableName, checkerColumnName);
            result.add(genAlterTablePhyTask(addSql, dropSql, tableName, "INPLACE"));
            result.add(
                new CheckColumnTask(schemaName, tableName, checkerColumnName, oldColumnName, newColumnName, false));
            result.add(genAlterTablePhyTask(dropSql, addSql, tableName, "INPLACE"));
        }
        return result;
    }

    private DdlTask genAddColumnPhyTask(String tableName, boolean isGsi) {
        String origSql = executionContext.getDdlContext().getDdlStmt();
        SQLAlterTableStatement alterTableStmt = (SQLAlterTableStatement) FastsqlUtils.parseSql(origSql).get(0);
        SQLColumnDefinition colDef;
        boolean modifyPosition;
        SQLName afterColumn;

        if (this.alterTableItem instanceof MySqlAlterTableChangeColumn) {
            MySqlAlterTableChangeColumn changeColumn = (MySqlAlterTableChangeColumn) this.alterTableItem;
            colDef = changeColumn.getNewColumnDefinition().clone();
            modifyPosition = changeColumn.isFirst() || changeColumn.getAfterColumn() != null;
            afterColumn = changeColumn.getAfterColumn();
        } else {
            MySqlAlterTableModifyColumn modifyColumn = (MySqlAlterTableModifyColumn) this.alterTableItem;
            colDef = modifyColumn.getNewColumnDefinition().clone();
            modifyPosition = modifyColumn.isFirst() || modifyColumn.getAfterColumn() != null;
            afterColumn = modifyColumn.getAfterColumn();
            colDef.setName(SqlIdentifier.surroundWithBacktick(newColumnName));
        }

        if (!newColumnNullable) {
            colDef.getConstraints()
                .removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLNotNullConstraint);
        }

        if (withUniqueConstraint) {
            colDef.getConstraints()
                .removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLColumnUniqueKey);
        }

        SQLAlterTableAddColumn addColumn = new SQLAlterTableAddColumn();
        addColumn.addColumn(colDef);

        // Do not set column position for gsi, since it may not contain after column
        if (!isGsi) {
            if (!modifyPosition) {
                addColumn.setAfterColumn(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(oldColumnName)));
            } else if (afterColumn != null) {
                addColumn.setAfterColumn(afterColumn);
            } else {
                addColumn.setFirst(true);
            }
        }

        // TableOptions will not be null since we have to specify ALGORITHM=OMC
        alterTableStmt.getTableOptions().clear();
        alterTableStmt.getItems().clear();
        alterTableStmt.getItems().add(addColumn);
        String addColumnSql = alterTableStmt.toString();

        return genAlterTablePhyTask(addColumnSql, "", tableName, "DEFAULT");
    }

    private DdlTask genSwapColumnPhyTask(String tableName) {
        if (isChange) {
            return new EmptyTask(schemaName);
        }
        // TODO(qianjing): Find a better way to do this: If we use wrong type, alter table will fail and ddl is stuck
        return genAlterTablePhyTask(genSwapColumnSql(tableName, false), genSwapColumnSql(tableName, true),
            tableName, "INPLACE");
    }

    private String genSwapColumnSql(String tableName, boolean reverse) {
        if (!reverse) {
            return String.format("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s, CHANGE COLUMN `%s` `%s` %s",
                tableName, oldColumnName, newColumnName, oldColumnType, newColumnName, oldColumnName, newColumnType);
        } else {
            return String.format("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s, CHANGE COLUMN `%s` `%s` %s",
                tableName, oldColumnName, newColumnName, newColumnType, newColumnName, oldColumnName, oldColumnType);
        }
    }

    private DdlTask genDropColumnPhyTask(String tableName) {
        String dropColumnSql = String.format("ALTER TABLE `%s` DROP COLUMN `%s`", tableName, dropColumnName);
        return genAlterTablePhyTask(dropColumnSql, "", tableName, "INPLACE");
    }

    private DdlTask genColumnNotNullableTasks(String columnName, String colDef, String tableName) {
        // Nullable to not nullable, colDef is not nullable
        String sql = String.format("ALTER TABLE `%s` MODIFY COLUMN `%s` %s", tableName, columnName, colDef);
        String reverseSql = String.format("ALTER TABLE `%s` MODIFY COLUMN `%s` %s", tableName, columnName,
            colDef.replace("NOT NULL", ""));
        return genAlterTablePhyTask(sql, reverseSql, tableName, "INPLACE");
    }

    private DdlTask genColumnNullableTasks(String columnName, String colDef, String tableName) {
        // Not nullable to nullable, coldef is not nullable
        colDef = colDef.replace("NOT NULL", "");
        String sql = String.format("ALTER TABLE `%s` MODIFY COLUMN `%s` %s", tableName, columnName, colDef);
        String reverseSql = sql + " NOT NULL";
        return genAlterTablePhyTask(sql, reverseSql, tableName, "INPLACE");
    }

    private List<List<DdlTask>> genAddLocalIndexTasks() {
        if (localIndexes.isEmpty() && prepareData.getNewUniqueIndexNameMap().isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, String> newUniqueIndexNameMap = prepareData.getNewUniqueIndexNameMap();
        Set<String> tableNames =
            newUniqueIndexNameMap.isEmpty() ? localIndexes.keySet() : newUniqueIndexNameMap.keySet();

        List<List<DdlTask>> allIndexTasks = new ArrayList<>();

        for (String tableName : tableNames) {
            List<DdlTask> createIndexTasks = new ArrayList<>();

            // Create unique index first if needed
            if (!prepareData.getNewUniqueIndexNameMap().isEmpty()) {
                String newUniqueIndexName = newUniqueIndexNameMap.get(tableName);
                String sql = genAddLocalIndexSql(schemaName, tableName, newUniqueIndexName,
                    genKeyColumns(ImmutableList.of(newColumnName), ImmutableList.of(0L)), "UNIQUE");
                String reverseSql = genDropLocalIndexSql(schemaName, tableName, newUniqueIndexName);

                SubJobTask createUniqueIndexSubJobTask = new SubJobTask(schemaName, sql, reverseSql);
                createUniqueIndexSubJobTask.setParentAcquireResource(true);
                createIndexTasks.add(createUniqueIndexSubJobTask);
            }

            Map<String, String> localIndexNewNameMap = prepareData.getLocalIndexNewNameMap().get(tableName);
            Map<String, String> localIndexTmpNameMap = prepareData.getLocalIndexTmpNameMap().get(tableName);
            Map<String, IndexMeta> localIndexMeta = prepareData.getLocalIndexMeta().get(tableName);

            if (GeneralUtil.isEmpty(localIndexNewNameMap)) {
                allIndexTasks.add(createIndexTasks);
                continue;
            }

            for (Map.Entry<String, IndexMeta> entry : localIndexMeta.entrySet()) {
                IndexMeta indexMeta = entry.getValue();
                String unique = indexMeta.isUniqueIndex() ? "UNIQUE" : "";
                List<String> columnNames =
                    indexMeta.getKeyColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
                List<Long> subParts =
                    indexMeta.getKeyColumnsExt().stream().map(IndexColumnMeta::getSubPart).collect(Collectors.toList());

                ListIterator<String> iterator = columnNames.listIterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    if (next.equalsIgnoreCase(oldColumnName)) {
                        iterator.set(newColumnName);
                        break;
                    }
                }

                // Add temporary local index
                String indexName = entry.getKey();
                String newIndexName = localIndexNewNameMap.get(indexName);
                String tmpIndexName = localIndexTmpNameMap.get(indexName);

                String sql =
                    genAddLocalIndexSql(schemaName, tableName, newIndexName, genKeyColumns(columnNames, subParts),
                        unique);
                String reverseSql = genDropLocalIndexSql(schemaName, tableName, newIndexName);

                SubJobTask createLocalIndexSubJobTask = new SubJobTask(schemaName, sql, reverseSql);
                createLocalIndexSubJobTask.setParentAcquireResource(true);
                createIndexTasks.add(createLocalIndexSubJobTask);
            }

            allIndexTasks.add(createIndexTasks);
        }
        return allIndexTasks;
    }

    private List<List<DdlTask>> genSwapAndDropLocalIndexTasks() {
        if (localIndexes.isEmpty()) {
            return Collections.emptyList();
        }

        List<List<DdlTask>> allIndexTasks = new ArrayList<>();

        for (String tableName : localIndexes.keySet()) {
            List<DdlTask> createIndexTasks = new ArrayList<>();
            List<DdlTask> dropIndexTasks = new ArrayList<>();

            List<String> indexNameList = new ArrayList<>();
            List<String> newIndexNameList = new ArrayList<>();
            List<String> tmpIndexNameList = new ArrayList<>();

            Map<String, String> localIndexNewNameMap = prepareData.getLocalIndexNewNameMap().get(tableName);
            Map<String, String> localIndexTmpNameMap = prepareData.getLocalIndexTmpNameMap().get(tableName);
            Map<String, IndexMeta> localIndexMeta = prepareData.getLocalIndexMeta().get(tableName);

            for (Map.Entry<String, IndexMeta> entry : localIndexMeta.entrySet()) {
                IndexMeta indexMeta = entry.getValue();
                String unique = indexMeta.isUniqueIndex() ? "UNIQUE" : "";
                List<String> columnNames =
                    indexMeta.getKeyColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
                List<Long> subParts =
                    indexMeta.getKeyColumnsExt().stream().map(IndexColumnMeta::getSubPart).collect(Collectors.toList());
                // Drop temporary local index
                ListIterator<String> iterator = columnNames.listIterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    if (next.equalsIgnoreCase(oldColumnName)) {
                        iterator.set(dropColumnName);
                        break;
                    }
                }
                String indexName = entry.getKey();
                String newIndexName = localIndexNewNameMap.get(indexName);
                String tmpIndexName = localIndexTmpNameMap.get(indexName);

                indexNameList.add(indexName);
                newIndexNameList.add(newIndexName);
                tmpIndexNameList.add(tmpIndexName);

                String sql = genDropLocalIndexSql(schemaName, tableName, tmpIndexName);
                String reverseSql =
                    genAddLocalIndexSql(schemaName, tableName, tmpIndexName, genKeyColumns(columnNames, subParts),
                        unique);

                SubJobTask dropLocalIndexSubJobTask = new SubJobTask(schemaName, sql, reverseSql);
                dropLocalIndexSubJobTask.setParentAcquireResource(true);
                dropIndexTasks.add(dropLocalIndexSubJobTask);
            }

            // Swap local index name
            // old -> tmp, new - > old
            String sql =
                genRenameIndexSql(schemaName, tableName, indexNameList, newIndexNameList, tmpIndexNameList);
            // old -> new, tmp - > old
            String reverseSql =
                genRenameIndexSql(schemaName, tableName, indexNameList, tmpIndexNameList, newIndexNameList);
            SubJobTask swapLocalIndexSubJobTask = new SubJobTask(schemaName, sql, reverseSql);
            swapLocalIndexSubJobTask.setParentAcquireResource(true);
            createIndexTasks.add(swapLocalIndexSubJobTask);
            createIndexTasks.addAll(dropIndexTasks);

            allIndexTasks.add(createIndexTasks);
        }
        return allIndexTasks;
    }

    private String genKeyColumns(List<String> columnNames, List<Long> subParts) {
        List<String> keyColumns = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (subParts.get(i).equals(0L)) {
                keyColumns.add("`" + columnNames.get(i) + "`");
            } else {
                keyColumns.add("`" + columnNames.get(i) + "`(" + subParts.get(i) + ")");
            }
        }
        return StringUtils.join(keyColumns, ",");
    }

    private String genAddLocalIndexSql(String schemaName, String tableName, String indexName, String columnNames,
                                       String unique) {
        // InnoDB only supports BTREE, even if it's HASH in SHOW CREATE TABLE
        return String.format(
            "/*+TDDL:cmd_extra(DDL_ON_GSI=TRUE)*/ ALTER TABLE `%s`.`%s` ADD LOCAL %s INDEX `%s` USING BTREE (%s), ALGORITHM=%s",
            schemaName, tableName, unique, indexName, columnNames, ALTER_TABLE_ALGORITHM_OMC_INDEX);
    }

    private String genDropLocalIndexSql(String schemaName, String tableName, String indexName) {
        return String.format("/*+TDDL:cmd_extra(DDL_ON_GSI=TRUE)*/ ALTER TABLE `%s`.`%s` DROP INDEX `%s`, ALGORITHM=%s",
            schemaName, tableName, indexName, ALTER_TABLE_ALGORITHM_OMC_INDEX);
    }

    private String genRenameIndexSql(String schemaName, String tableName, List<String> oldIndexName,
                                     List<String> newIndexName, List<String> tmpIndexName) {
        List<String> renameItems = new ArrayList<>();
        // RENAME INDEX old_index TO tmp_index, new_index TO old_index
        for (int i = 0; i < oldIndexName.size(); i++) {
            renameItems.add(String.format(" RENAME INDEX `%s` TO `%s`", oldIndexName.get(i), tmpIndexName.get(i)));
            renameItems.add(String.format(" RENAME INDEX `%s` TO `%s`", newIndexName.get(i), oldIndexName.get(i)));
        }
        return String.format("/*+TDDL:cmd_extra(DDL_ON_GSI=TRUE)*/ ALTER TABLE `%s`.`%s` %s, ALGORITHM=%s", schemaName,
            tableName, StringUtils.join(renameItems, ","), ALTER_TABLE_ALGORITHM_OMC_INDEX);
    }

    private DdlTask genAlterTablePhyTask(String sql, String reverseSql, String tableName, String algorithm) {
        sql = sql + " ,ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSql)) {
            reverseSql = reverseSql + " ,ALGORITHM=" + algorithm;
        }

        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        SqlAlterTable sqlAlterTable =
            (SqlAlterTable) new FastsqlParser().parse(sql, executionContext).get(0);
        sqlAlterTable = (SqlAlterTable) sqlAlterTable.accept(visitor);

        SqlIdentifier tableNameNode =
            new SqlIdentifier(Lists.newArrayList(schemaName, tableName), SqlParserPos.ZERO);

        final RelOptCluster cluster =
            SqlConverter.getInstance(executionContext).createRelOptCluster(new PlannerContext(executionContext));
        AlterTable alterTable = AlterTable.create(cluster, sqlAlterTable, tableNameNode, null);

        LogicalAlterTable logicalAlterTable = LogicalAlterTable.create(alterTable);
        logicalAlterTable.prepareData();

        DdlPhyPlanBuilder alterTableBuilder =
            AlterTableBuilder.create(alterTable, logicalAlterTable.getAlterTablePreparedData(), executionContext)
                .build();

        PhysicalPlanData physicalPlanData = alterTableBuilder.genPhysicalPlanData();
        AlterTablePhyDdlTask task;
        task = new AlterTablePhyDdlTask(schemaName, tableName, physicalPlanData);
        task.setSourceSql(sql);
        if (!StringUtils.isEmpty(reverseSql)) {
            task.setRollbackSql(reverseSql);
        }
        return task;
    }

    private void addConcurrentTasks(ExecutableDdlJob ddlJob, DdlTask beforeTask, DdlTask afterTask,
                                    List<List<DdlTask>> addedTasks) {
        if (addedTasks.isEmpty()) {
            return;
        }
        ddlJob.removeTaskRelationship(beforeTask, afterTask);
        for (List<DdlTask> tasks : addedTasks) {
            ddlJob.addSequentialTasksAfter(beforeTask, tasks);
            ddlJob.addTaskRelationship(tasks.get(tasks.size() - 1), afterTask);
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }
}
