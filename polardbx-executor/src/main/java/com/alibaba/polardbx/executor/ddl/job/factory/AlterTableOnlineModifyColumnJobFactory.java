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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNullConstraint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.ColumnBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableColumnDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CheckColumnTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnDropMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnInitMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnStopMultiWriteTask;
import com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn.OnlineModifyColumnSwapMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.Attribute.ALTER_TABLE_ALGORITHM_OMC_INDEX;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

public class AlterTableOnlineModifyColumnJobFactory extends DdlJobFactory {
    private final PhysicalPlanData physicalPlanData;
    private final String schemaName;
    private final String logicalTableName;
    private final AlterTablePreparedData prepareData;
    private final AlterTableWithGsiPreparedData gsiPreparedData;
    private final LogicalAlterTable logicalAlterTable;
    private final TableGroupConfig tableGroupConfig;

    private final String oldColumnType;
    private final String newColumnType;
    private final String newColumnTypeNullable;
    private final String oldColumnTypeNullable;
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

    private final Map<String, PhysicalPlanData> physicalPlanDataMap;

    private final boolean useInstantAddColumn;

    // DEBUG ONLY
    private final boolean skipBackfill;

    private final ExecutionContext executionContext;

    public AlterTableOnlineModifyColumnJobFactory(PhysicalPlanData physicalPlanData,
                                                  List<PhysicalPlanData> gsiPhysicalPlanData,
                                                  AlterTablePreparedData preparedData,
                                                  AlterTableWithGsiPreparedData gsiPreparedData,
                                                  LogicalAlterTable logicalAlterTable,
                                                  ExecutionContext executionContext) {
        this.physicalPlanData = physicalPlanData;
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
        this.oldColumnTypeNullable = prepareData.getModifyColumnTypeNullable();

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
            this.newColumnType =
                TableColumnUtils.getDataDefFromColumnDefWithoutUnique(changeColumn.getNewColumnDefinition());
            this.newColumnTypeNullable =
                TableColumnUtils.getDataDefFromColumnDefWithoutUniqueNullable(changeColumn.getNewColumnDefinition());

            if (changeColumn.getNewColumnDefinition().getGeneratedAlawsAs() != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Column [%s] can not be modified to a generated column", oldColumnName));
            }

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
            this.newColumnType =
                TableColumnUtils.getDataDefFromColumnDefWithoutUnique(modifyColumn.getNewColumnDefinition());
            this.newColumnTypeNullable =
                TableColumnUtils.getDataDefFromColumnDefWithoutUniqueNullable(modifyColumn.getNewColumnDefinition());

            if (modifyColumn.getNewColumnDefinition().getGeneratedAlawsAs() != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Column [%s] can not be modified to a generated column", oldColumnName));
            }
        }
        this.withUniqueConstraint = this.newColumnTypeForChecker.getConstraints().stream()
            .anyMatch(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLColumnUniqueKey);

        // remove not nullable, unique, default value, comment, on update
        this.newColumnTypeForChecker.getConstraints()
            .removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLNotNullConstraint);
        this.newColumnTypeForChecker.getConstraints()
            .removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLNullConstraint);
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
        this.checkerColumnName = preparedData.getCheckerColumnNames().get(0);

        this.skipBackfill = preparedData.isSkipBackfill();

        this.physicalPlanDataMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        physicalPlanDataMap.put(logicalTableName, physicalPlanData);
        for (PhysicalPlanData gsiPhysicalPlanDatum : gsiPhysicalPlanData) {
            physicalPlanDataMap.put(gsiPhysicalPlanDatum.getLogicalTableName(), gsiPhysicalPlanDatum);
        }

        this.useInstantAddColumn = canUseInstantAddColumn();
    }

    public static DdlTask genColumnNotNullableTasks(String columnName, String colDef, String colDefNullable,
                                                    String schemaName, String tableName,
                                                    PhysicalPlanData physicalPlanData) {
        String tableNameWithBacktick = surroundWithBacktick(tableName);
        // Nullable to not nullable, colDef is not nullable
        String sqlFormatter =
            String.format("ALTER TABLE %%s MODIFY COLUMN %s %s", surroundWithBacktick(columnName), colDef);
        String reverseSqlFormatter =
            String.format("ALTER TABLE %%s MODIFY COLUMN %s %s", surroundWithBacktick(columnName), colDefNullable);
        String sql = String.format(sqlFormatter, tableNameWithBacktick);
        String reverseSql = String.format(reverseSqlFormatter, tableNameWithBacktick);
        String sqlTemplate = String.format(sqlFormatter, "?");
        String reverseSqlTemplate = String.format(reverseSqlFormatter, "?");
        return genAlterTablePhyTask(sql, reverseSql, sqlTemplate, reverseSqlTemplate, schemaName, tableName, "INPLACE",
            "INPLACE", physicalPlanData);
    }

    public static DdlTask genColumnNullableTasks(String columnName, String colDef, String colDefNullable,
                                                 String schemaName, String tableName,
                                                 PhysicalPlanData physicalPlanData) {
        String tableNameWithBacktick = surroundWithBacktick(tableName);
        // Not nullable to nullable, coldef is not nullable
        String sqlFormatter =
            String.format("ALTER TABLE %%s MODIFY COLUMN %s %s", surroundWithBacktick(columnName), colDefNullable);
        String reverseSqlFormatter =
            String.format("ALTER TABLE %%s MODIFY COLUMN %s %s", surroundWithBacktick(columnName), colDef);
        String sql = String.format(sqlFormatter, tableNameWithBacktick);
        String reverseSql = String.format(reverseSqlFormatter, tableNameWithBacktick);
        String sqlTemplate = String.format(sqlFormatter, "?");
        String reverseSqlTemplate = String.format(reverseSqlFormatter, "?");
        return genAlterTablePhyTask(sql, reverseSql, sqlTemplate, reverseSqlTemplate, schemaName, tableName, "INPLACE",
            "INPLACE", physicalPlanData);
    }

    public static DdlTask genAlterTablePhyTask(String sql, String reverseSql, String sqlTemplate,
                                               String reverseSqlTemplate, String schemaName, String tableName,
                                               String algorithm, String reverseAlgorithm,
                                               PhysicalPlanData physicalPlanData) {
        sql = sql + " ,ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSql)) {
            reverseSql = reverseSql + ", ALGORITHM=" + reverseAlgorithm;
        }

        sqlTemplate = sqlTemplate + ", ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSqlTemplate)) {
            reverseSqlTemplate = reverseSqlTemplate + ", ALGORITHM=" + reverseAlgorithm;
        }

        PhysicalPlanData newPhysicalPlanData = physicalPlanData.clone();
        newPhysicalPlanData.setSqlTemplate(sqlTemplate);
        AlterTablePhyDdlTask task;
        task = new AlterTablePhyDdlTask(schemaName, tableName, newPhysicalPlanData);
        task.setSourceSql(sql);
        if (!StringUtils.isEmpty(reverseSql)) {
            task.setRollbackSql(reverseSql);
            task.setRollbackSqlTemplate(reverseSqlTemplate);
        }
        return task;
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
                new ColumnBackFillTask(schemaName, logicalTableName, ImmutableList.of(oldColumnName),
                    ImmutableList.of(newColumnName), true, false)));
            for (String gsiName : coveringGsi) {
                columnBackFillTasks.add(ImmutableList.of(
                    new ColumnBackFillTask(schemaName, gsiName, ImmutableList.of(oldColumnName),
                        ImmutableList.of(newColumnName), true, false)));
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

        DdlTask cdcDdlMarkTask = new CdcAlterTableColumnDdlMarkTask(schemaName, physicalPlanData, true);

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
            columnNotNullableTasks.add(ImmutableList.of(
                genColumnNotNullableTasks(newColumnName, newColumnType, newColumnTypeNullable, schemaName,
                    logicalTableName, physicalPlanDataMap.get(logicalTableName))));
            for (String gsiName : coveringGsi) {
                columnNotNullableTasks.add(ImmutableList.of(
                    genColumnNotNullableTasks(newColumnName, newColumnType, newColumnTypeNullable, schemaName, gsiName,
                        physicalPlanDataMap.get(gsiName))));
            }
        }

        List<List<DdlTask>> columnNullableTasks = new ArrayList<>();
        if (!oldColumnNullable) {
            columnNullableTasks.add(ImmutableList.of(
                genColumnNullableTasks(dropColumnName, oldColumnType, oldColumnTypeNullable, schemaName,
                    logicalTableName, physicalPlanDataMap.get(logicalTableName))));
            for (String gsiName : coveringGsi) {
                columnNullableTasks.add(ImmutableList.of(
                    genColumnNullableTasks(dropColumnName, oldColumnType, oldColumnTypeNullable, schemaName, gsiName,
                        physicalPlanDataMap.get(gsiName))));
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
        List<DdlTask> allTasks =
            Lists.newArrayList(validateTask, initTableUpdateStatusTask, initTableSyncTask, addColumnLogicalTask,
                addColumnTableSyncTask, barrierTask1, barrierTask2, barrierTask3, barrierTask4, swapColumnLogicalTask,
                cdcDdlMarkTask, swapColumnTableSyncTask, barrierTask5, stopMultiWriteTask, stopMultiWriteTableSyncTask,
                dropColumnLogicalTask, dropTableSyncTask);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(allTasks);

        executableDdlJob.addConcurrentTasksBetween(initTableSyncTask, addColumnLogicalTask, addColumnPhyTasks);
        executableDdlJob.addConcurrentTasksBetween(addColumnTableSyncTask, barrierTask1, columnBackFillTasks);
        executableDdlJob.addConcurrentTasksBetween(barrierTask1, barrierTask2, columnNotNullableTasks);
        executableDdlJob.addConcurrentTasksBetween(barrierTask2, barrierTask3, addLocalIndexTasks);
        executableDdlJob.addConcurrentTasksBetween(barrierTask3, barrierTask4, checkColumnTasks);
        executableDdlJob.addConcurrentTasksBetween(barrierTask4, swapColumnLogicalTask, swapColumnPhyTasks);
        executableDdlJob.addConcurrentTasksBetween(swapColumnTableSyncTask, barrierTask5, swapAndDropLocalIndexTasks);
        executableDdlJob.addConcurrentTasksBetween(barrierTask5, stopMultiWriteTask, columnNullableTasks);
        executableDdlJob.addConcurrentTasksBetween(stopMultiWriteTableSyncTask, dropColumnLogicalTask,
            dropColumnPhyTasks);

        executableDdlJob.setExceptionActionForAllSuccessor(validateTask, DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
        // TODO(qianjing): rollback swapColumnLogicalTask
        executableDdlJob.setExceptionActionForAllSuccessor(swapColumnLogicalTask, DdlExceptionAction.PAUSE);
        // Do not try to recover SubJobTask since it already tries to recover during subjob execution
        for (List<DdlTask> addLocalIndexTask : addLocalIndexTasks) {
            for (DdlTask ddlTask : addLocalIndexTask) {
                ddlTask.setExceptionAction(DdlExceptionAction.PAUSE);
            }
        }

        executableDdlJob.labelAsHead(validateTask);
        return executableDdlJob;
    }

    private List<DdlTask> genCheckerTasks(String tableName) {
        List<DdlTask> result = new ArrayList<>();
        String tableNameWithBacktick = surroundWithBacktick(tableName);
        // Simple checker only checks if both columns are null or not null
        if (useSimpleChecker) {
            result.add(new CheckColumnTask(schemaName, tableName, oldColumnName, oldColumnName, newColumnName, true));
        } else {
            String checkerColumnType = TableColumnUtils.getDataDefFromColumnDef(newColumnTypeForChecker);

            String addSqlFormatter =
                String.format("ALTER TABLE %%s ADD COLUMN %s %s GENERATED ALWAYS AS (ALTER_TYPE(%s)) VIRTUAL",
                    surroundWithBacktick(checkerColumnName), checkerColumnType, surroundWithBacktick(oldColumnName));
            String dropSqlFormatter =
                String.format("ALTER TABLE %%s DROP COLUMN %s", surroundWithBacktick(checkerColumnName));
            String addSql = String.format(addSqlFormatter, tableNameWithBacktick);
            String dropSql = String.format(dropSqlFormatter, tableNameWithBacktick);
            String addSqlTemplate = String.format(addSqlFormatter, "?");
            String dropSqlTemplate = String.format(dropSqlFormatter, "?");

            result.add(
                genAlterTablePhyTask(addSql, dropSql, addSqlTemplate, dropSqlTemplate, schemaName, tableName, "INPLACE",
                    "INPLACE", physicalPlanDataMap.get(tableName)));
            result.add(
                new CheckColumnTask(schemaName, tableName, checkerColumnName, oldColumnName, newColumnName, false));
            result.add(
                genAlterTablePhyTask(dropSql, addSql, dropSqlTemplate, addSqlTemplate, schemaName, tableName, "INPLACE",
                    "INPLACE", physicalPlanDataMap.get(tableName)));
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
            colDef.getConstraints().removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLColumnUniqueKey);
        }

        SQLAlterTableAddColumn addColumn = new SQLAlterTableAddColumn();
        addColumn.addColumn(colDef);

        if (useInstantAddColumn) {
            addColumn.setFirst(false);
            addColumn.setFirstColumn(null);
            addColumn.setAfterColumn(null);
        } else if (!isGsi) {
            // Do not set column position for gsi, since it may not contain after column
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
        alterTableStmt.setAfterSemi(false);
        String addColumnSql = alterTableStmt.toString();
        alterTableStmt.getTableSource().setExpr("?");
        String addColumnSqlTemplate = alterTableStmt.toString();
        return genAlterTablePhyTask(addColumnSql, "", addColumnSqlTemplate, "", schemaName, tableName,
            useInstantAddColumn ? "INSTANT" : "INPLACE", "INPLACE", physicalPlanDataMap.get(tableName));
    }

    private DdlTask genSwapColumnPhyTask(String tableName) {
        if (isChange) {
            return new EmptyTask(schemaName);
        }
        // TODO(qianjing): Find a better way to do this: If we use wrong type, alter table will fail and ddl is stuck
        return genAlterTablePhyTask(genSwapColumnSql(tableName, false, false), genSwapColumnSql(tableName, true, false),
            genSwapColumnSql(tableName, false, true), genSwapColumnSql(tableName, true, true), schemaName, tableName,
            "INPLACE", "INPLACE", physicalPlanDataMap.get(tableName));
    }

    private String genSwapColumnSql(String tableName, boolean reverse, boolean template) {
        tableName = template ? "?" : surroundWithBacktick(tableName);
        if (!reverse) {
            return String.format("ALTER TABLE %s CHANGE COLUMN %s %s %s, CHANGE COLUMN %s %s %s", tableName,
                surroundWithBacktick(oldColumnName), surroundWithBacktick(newColumnName), oldColumnType,
                surroundWithBacktick(newColumnName), surroundWithBacktick(oldColumnName), newColumnType);
        } else {
            return String.format("ALTER TABLE %s CHANGE COLUMN %s %s %s, CHANGE COLUMN %s %s %s", tableName,
                surroundWithBacktick(oldColumnName), surroundWithBacktick(newColumnName), newColumnType,
                surroundWithBacktick(newColumnName), surroundWithBacktick(oldColumnName), oldColumnType);
        }
    }

    private DdlTask genDropColumnPhyTask(String tableName) {
        String tableNameWithBacktick = surroundWithBacktick(tableName);
        String dropColumnSqlFormatter =
            String.format("ALTER TABLE %%s DROP COLUMN %s", surroundWithBacktick(dropColumnName));
        return genAlterTablePhyTask(String.format(dropColumnSqlFormatter, tableNameWithBacktick), "",
            String.format(dropColumnSqlFormatter, "?"), "", schemaName, tableName, "INPLACE",
            "INPLACE", physicalPlanDataMap.get(tableName));
    }

    public List<List<DdlTask>> genAddLocalIndexTasks() {
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
            String sql = genRenameIndexSql(schemaName, tableName, indexNameList, newIndexNameList, tmpIndexNameList);
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
                keyColumns.add(surroundWithBacktick(columnNames.get(i)));
            } else {
                keyColumns.add(surroundWithBacktick(columnNames.get(i)) + "(" + subParts.get(i) + ")");
            }
        }
        return StringUtils.join(keyColumns, ",");
    }

    private String genAddLocalIndexSql(String schemaName, String tableName, String indexName, String columnNames,
                                       String unique) {
        // InnoDB only supports BTREE, even if it's HASH in SHOW CREATE TABLE
        return String.format(
            "/*+TDDL:cmd_extra(DDL_ON_GSI=TRUE)*/ ALTER TABLE %s.%s ADD LOCAL %s INDEX %s USING BTREE (%s), ALGORITHM=%s",
            surroundWithBacktick(schemaName), surroundWithBacktick(tableName), unique, surroundWithBacktick(indexName),
            columnNames, ALTER_TABLE_ALGORITHM_OMC_INDEX);
    }

    private String genDropLocalIndexSql(String schemaName, String tableName, String indexName) {
        return String.format("/*+TDDL:cmd_extra(DDL_ON_GSI=TRUE)*/ ALTER TABLE %s.%s DROP INDEX %s, ALGORITHM=%s",
            surroundWithBacktick(schemaName), surroundWithBacktick(tableName), surroundWithBacktick(indexName),
            ALTER_TABLE_ALGORITHM_OMC_INDEX);
    }

    private String genRenameIndexSql(String schemaName, String tableName, List<String> oldIndexName,
                                     List<String> newIndexName, List<String> tmpIndexName) {
        List<String> renameItems = new ArrayList<>();
        // RENAME INDEX old_index TO tmp_index, new_index TO old_index
        for (int i = 0; i < oldIndexName.size(); i++) {
            renameItems.add(String.format(" RENAME INDEX %s TO %s", surroundWithBacktick(oldIndexName.get(i)),
                surroundWithBacktick(tmpIndexName.get(i))));
            renameItems.add(String.format(" RENAME INDEX %s TO %s", surroundWithBacktick(newIndexName.get(i)),
                surroundWithBacktick(oldIndexName.get(i))));
        }
        return String.format("/*+TDDL:cmd_extra(DDL_ON_GSI=TRUE)*/ ALTER TABLE %s.%s %s, ALGORITHM=%s",
            surroundWithBacktick(schemaName), surroundWithBacktick(tableName), StringUtils.join(renameItems, ","),
            ALTER_TABLE_ALGORITHM_OMC_INDEX);
    }

    private DdlTask genAlterTablePhyTask(String sql, String reverseSql, String sqlTemplate, String reverseSqlTemplate,
                                         String tableName, String algorithm) {
        sql = sql + ", ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSql)) {
            reverseSql = reverseSql + ", ALGORITHM=" + algorithm;
        }

        sqlTemplate = sqlTemplate + " ,ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSqlTemplate)) {
            reverseSqlTemplate = reverseSqlTemplate + " ,ALGORITHM=" + algorithm;
        }

        PhysicalPlanData newPhysicalPlanData = physicalPlanDataMap.get(tableName).clone();
        newPhysicalPlanData.setSqlTemplate(sqlTemplate);
        AlterTablePhyDdlTask task;
        task = new AlterTablePhyDdlTask(schemaName, tableName, newPhysicalPlanData);
        task.setSourceSql(sql);
        if (!StringUtils.isEmpty(reverseSql)) {
            task.setRollbackSql(reverseSql);
            task.setRollbackSqlTemplate(reverseSqlTemplate);
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

    private boolean canUseInstantAddColumn() {
        DataSource dataSource = DdlHelper.getPhyDataSource(schemaName, dbIndex);
        return executionContext.getParamManager().getBoolean(ConnectionParams.SUPPORT_INSTANT_ADD_COLUMN)
            && TableInfoManager.isInstantAddColumnSupportedByPhyDb(dataSource, dbIndex);
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }
}
