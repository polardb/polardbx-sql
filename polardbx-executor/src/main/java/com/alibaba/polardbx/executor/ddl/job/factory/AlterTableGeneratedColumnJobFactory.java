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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.ColumnBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AddGeneratedColumnMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ChangeColumnStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableColumnDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlDropColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AlterTableGeneratedColumnJobFactory extends DdlJobFactory {
    private final PhysicalPlanData physicalPlanData;
    private final String schemaName;
    private final String logicalTableName;
    private final AlterTablePreparedData prepareData;
    private final LogicalAlterTable logicalAlterTable;
    private final TableGroupConfig tableGroupConfig;
    private final ExecutionContext executionContext;

    private final List<String> targetColumns;
    private final List<String> sourceExprs;

    // columnName -> (nullable def, not nullable def)
    private final Map<String, Pair<String, String>> notNullableColumns;

    // FOR DEBUG ONLY
    private final boolean skipBackfill;
    private final boolean forceCnEval;

    public AlterTableGeneratedColumnJobFactory(PhysicalPlanData physicalPlanData, AlterTablePreparedData preparedData,
                                               LogicalAlterTable logicalAlterTable, ExecutionContext executionContext) {
        this.physicalPlanData = physicalPlanData;
        this.schemaName = logicalAlterTable.getSchemaName();
        this.logicalTableName = logicalAlterTable.getTableName();
        this.prepareData = preparedData;
        this.logicalAlterTable = logicalAlterTable;
        this.executionContext = executionContext;

        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        this.tableGroupConfig = isNewPart ? physicalPlanData.getTableGroupConfig() : null;

        this.targetColumns = new ArrayList<>();
        this.sourceExprs = new ArrayList<>();

        if (logicalAlterTable.isAddGeneratedColumn()) {
            this.notNullableColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            String origSql = executionContext.getDdlContext().getDdlStmt();
            SQLAlterTableStatement alterTableStmt = (SQLAlterTableStatement) FastsqlUtils.parseSql(origSql).get(0);
            for (SQLAlterTableItem item : alterTableStmt.getItems()) {
                if (item instanceof SQLAlterTableAddColumn) {
                    SQLColumnDefinition colDef = ((SQLAlterTableAddColumn) item).getColumns().get(0);
                    colDef.setGeneratedAlawsAs(null);
                    colDef.setLogical(false);
                    if (colDef.getConstraints().stream().anyMatch(c -> c instanceof SQLNotNullConstraint)) {
                        String columnDefNullable = TableColumnUtils.getDataDefFromColumnDefWithoutNullable(colDef);
                        String columnDefNotNullable = TableColumnUtils.getDataDefFromColumnDef(colDef);
                        notNullableColumns.put(SQLUtils.normalizeNoTrim(colDef.getColumnName()),
                            new Pair<>(columnDefNullable, columnDefNotNullable));
                    }
                    colDef.getConstraints().removeIf(c -> c instanceof SQLNotNullConstraint);
                }
            }
            if (!notNullableColumns.isEmpty()) {
                alterTableStmt.setName(new SQLIdentifierExpr("?"));
                physicalPlanData.setSqlTemplate(alterTableStmt.toString());
            }

            for (Map.Entry<String, Long> entry : this.prepareData.getSpecialDefaultValueFlags().entrySet()) {
                if (entry.getValue().equals(ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN)) {
                    targetColumns.add(entry.getKey());
                    sourceExprs.add(prepareData.getSpecialDefaultValues().get(entry.getKey()));
                }
            }
        } else {
            this.notNullableColumns = preparedData.getNotNullableGeneratedColumnDefs();
            for (SqlAlterSpecification alter : logicalAlterTable.getSqlAlterTable().getAlters()) {
                this.targetColumns.add(((SqlDropColumn) alter).getColName().getLastName());
            }
        }

        this.skipBackfill = preparedData.isSkipBackfill();
        this.forceCnEval = preparedData.isForceCnEval();
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        return logicalAlterTable.isAddGeneratedColumn() ? doCreateAddGeneratedColumn() : doCreateDropGeneratedColumn();
    }

    protected ExecutableDdlJob doCreateDropGeneratedColumn() {
        // validte -> change status (WRITE_ONLY) -> sync -> (change to nullable) -> cdc -> change status (ABSENT)
        // -> sync -> drop phy -> drop meta -> sync
        DdlTask validateTask = new AlterTableValidateTask(schemaName, logicalTableName,
            logicalAlterTable.getSqlAlterTable().getSourceSql(), prepareData.getTableVersion(), tableGroupConfig);
        DdlTask writeOnlyTask =
            new ChangeColumnStatusTask(schemaName, logicalTableName, targetColumns, ColumnStatus.PUBLIC,
                ColumnStatus.WRITE_ONLY);
        DdlTask writeOnlySyncTask = new TableSyncTask(schemaName, logicalTableName);

        List<DdlTask> nullableTasks = genNullableTask();

        DdlTask cdcTask = new CdcAlterTableColumnDdlMarkTask(schemaName, physicalPlanData, false);

        DdlTask hideColumnsTask =
            new ChangeColumnStatusTask(schemaName, logicalTableName, targetColumns, ColumnStatus.WRITE_ONLY,
                ColumnStatus.ABSENT);
        DdlTask hideColumnsSyncTask = new TableSyncTask(schemaName, logicalTableName);

        DdlTask phyDdlTask = new AlterTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);
        DdlTask updateMetaTask =
            new AlterTableChangeMetaTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(), physicalPlanData.getKind(), physicalPlanData.isPartitioned(),
                prepareData.getDroppedColumns(), prepareData.getAddedColumns(), prepareData.getUpdatedColumns(),
                prepareData.getChangedColumns(), prepareData.isTimestampColumnDefault(),
                prepareData.getSpecialDefaultValues(), prepareData.getSpecialDefaultValueFlags(),
                prepareData.getDroppedIndexes(), prepareData.getAddedIndexes(),
                prepareData.getAddedIndexesWithoutNames(),
                prepareData.getRenamedIndexes(),
                prepareData.isPrimaryKeyDropped(), prepareData.getAddedPrimaryKeyColumns(),
                prepareData.getColumnAfterAnother(), prepareData.isLogicalColumnOrder(), prepareData.getTableComment(),
                prepareData.getTableRowFormat(), physicalPlanData.getSequence(),
                prepareData.isOnlineModifyColumnIndexTask());
        DdlTask updateMetaSyncTask = new TableSyncTask(schemaName, logicalTableName);

        List<DdlTask> allTasks =
            Lists.newArrayList(validateTask, writeOnlyTask, writeOnlySyncTask, cdcTask, hideColumnsTask,
                hideColumnsSyncTask, phyDdlTask, updateMetaTask, updateMetaSyncTask);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(allTasks);
        executableDdlJob.addConcurrentTasksBetween(cdcTask, hideColumnsTask, ImmutableList.of(nullableTasks));

        executableDdlJob.setExceptionActionForAllSuccessor(validateTask, DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
        executableDdlJob.setExceptionActionForAllSuccessor(cdcTask, DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(updateMetaSyncTask);
        return executableDdlJob;
    }

    protected ExecutableDdlJob doCreateAddGeneratedColumn() {
        // validte -> add phy column -> add meta (WRITE_ONLY) -> sync -> backfill -> (change to not nullable) ->
        // cdc -> change status(PUBLIC) -> sync
        DdlTask validateTask = new AlterTableValidateTask(schemaName, logicalTableName,
            logicalAlterTable.getSqlAlterTable().getSourceSql(), prepareData.getTableVersion(), tableGroupConfig);

        DdlTask addColumnPhyTask = new AlterTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);
        DdlTask addColumnMetaTask =
            new AddGeneratedColumnMetaTask(schemaName, logicalTableName, sourceExprs, targetColumns,
                physicalPlanData.getDefaultDbIndex(), physicalPlanData.getDefaultPhyTableName(),
                prepareData.getColumnAfterAnother(), prepareData.isLogicalColumnOrder(),
                prepareData.getAddedIndexesWithoutNames());
        DdlTask addColumnMetaSync = new TableSyncTask(schemaName, logicalTableName);
        DdlTask columnBackFillTask = skipBackfill ? new EmptyTask(schemaName) :
            new ColumnBackFillTask(schemaName, logicalTableName, sourceExprs, targetColumns, false, forceCnEval);

        List<List<DdlTask>> checkColumnTasks = new ArrayList<>();
        for (int i = 0; i < targetColumns.size(); i++) {
            checkColumnTasks.add(genCheckerTasks());
        }
        List<DdlTask> notNullableTasks = genNotNullableTask();

        DdlTask cdcTask = new CdcAlterTableColumnDdlMarkTask(schemaName, physicalPlanData, false);

        DdlTask showColumnsTask =
            new ChangeColumnStatusTask(schemaName, logicalTableName, targetColumns, ColumnStatus.WRITE_ONLY,
                ColumnStatus.PUBLIC);
        DdlTask showColumnsSync = new TableSyncTask(schemaName, logicalTableName);

        List<DdlTask> allTasks =
            Lists.newArrayList(validateTask, addColumnPhyTask, addColumnMetaTask, addColumnMetaSync, columnBackFillTask,
                cdcTask, showColumnsTask, showColumnsSync);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(allTasks);
        executableDdlJob.addConcurrentTasksBetween(columnBackFillTask, cdcTask, ImmutableList.of(notNullableTasks));
        executableDdlJob.addConcurrentTasksBetween(cdcTask, showColumnsTask, checkColumnTasks);

        executableDdlJob.setExceptionActionForAllSuccessor(validateTask, DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
        executableDdlJob.setExceptionActionForAllSuccessor(cdcTask, DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(showColumnsSync);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    private List<DdlTask> genCheckerTasks() {
        // TODO: check non-pushable functions
        List<DdlTask> result = new ArrayList<>();
        return result;
    }

    private List<DdlTask> genNullableTask() {
        List<DdlTask> result = new ArrayList<>();
        for (Map.Entry<String, Pair<String, String>> entry : notNullableColumns.entrySet()) {
            String columnName = entry.getKey();
            result.add(
                AlterTableOnlineModifyColumnJobFactory.genColumnNullableTasks(columnName, entry.getValue().getValue(),
                    entry.getValue().getKey(), schemaName, logicalTableName, physicalPlanData));
        }
        return result;
    }

    private List<DdlTask> genNotNullableTask() {
        List<DdlTask> result = new ArrayList<>();
        for (Map.Entry<String, Pair<String, String>> entry : notNullableColumns.entrySet()) {
            String columnName = entry.getKey();
            result.add(AlterTableOnlineModifyColumnJobFactory.genColumnNotNullableTasks(columnName,
                entry.getValue().getValue(), entry.getValue().getKey(), schemaName, logicalTableName,
                physicalPlanData));
        }
        return result;
    }
}
