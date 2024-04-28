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
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreateGsiCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableColumnBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcGsiDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetCatchUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiDropColumnCleanUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiInsertColumnMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexColumnStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexStatusTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genChangeSetCatchUpTasks;

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
            IndexStatus.PUBLIC,
            false
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
                                                    boolean stayAtBackFill,
                                                    Map<String, String> virtualColumns,
                                                    Map<String, String> backfillColumnMap,
                                                    List<String> modifyStringColumns,
                                                    PhysicalPlanData physicalPlanData,
                                                    TableMeta tableMeta,
                                                    boolean repartition,
                                                    boolean modifyColumn,
                                                    boolean mirrorCopy,
                                                    String originalDdl) {
        List<DdlTask> taskList = new ArrayList<>();

        DdlTask deleteOnlyTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.CREATING,
            IndexStatus.DELETE_ONLY,
            true
        ).onExceptionTryRecoveryThenRollback();
        DdlTask writeOnlyTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.DELETE_ONLY,
            IndexStatus.WRITE_ONLY,
            true
        ).onExceptionTryRecoveryThenRollback();
        DdlTask writeReOrgTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.WRITE_ONLY,
            IndexStatus.WRITE_REORG,
            true
        ).onExceptionTryRecoveryThenRollback();
        DdlTask publicTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.WRITE_REORG,
            IndexStatus.PUBLIC,
            true
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
        taskList.add(
            new LogicalTableBackFillTask(schemaName, primaryTableName, indexName, virtualColumns, backfillColumnMap,
                modifyStringColumns, false, mirrorCopy, modifyColumn));
        if (stayAtBackFill) {
            return taskList;
        }
        taskList.add(writeReOrgTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        if (!tableMeta.isAutoPartition() && !repartition) {
            CdcGsiDdlMarkTask cdcDdlMarkTask =
                new CdcGsiDdlMarkTask(schemaName, physicalPlanData, primaryTableName, originalDdl);
            taskList.add(cdcDdlMarkTask);
        }
        taskList.add(publicTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        return taskList;
    }

    public static List<DdlTask> addGlobalIndexTasksChangeSet(String schemaName,
                                                             String primaryTableName,
                                                             String oldIndexName,
                                                             String indexName,
                                                             boolean stayAtDeleteOnly,
                                                             boolean stayAtWriteOnly,
                                                             boolean stayAtBackFill,
                                                             Map<String, String> virtualColumns,
                                                             Map<String, String> backfillColumnMap,
                                                             List<String> modifyStringColumns,
                                                             boolean modifyColumn,
                                                             boolean mirrorCopy,
                                                             PhysicalPlanData physicalPlanData,
                                                             PartitionInfo indexPartitionInfo) {
        List<DdlTask> taskList = new ArrayList<>();
        // start
        Long changeSetId = ChangeSetManager.getChangeSetId();
        Map<String, Set<String>> sourcePhyTableNames = GsiUtils.getPhyTables(schemaName, oldIndexName);
        Map<String, String> targetTableLocations =
            GsiUtils.getPhysicalTableMapping(schemaName, oldIndexName, null, physicalPlanData, indexPartitionInfo);

        ChangeSetStartTask changeSetStartTask = new ChangeSetStartTask(
            schemaName, oldIndexName, sourcePhyTableNames,
            ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN,
            changeSetId
        );

        Map<String, ChangeSetCatchUpTask> catchUpTasks = genChangeSetCatchUpTasks(
            schemaName,
            oldIndexName,
            indexName,
            sourcePhyTableNames,
            targetTableLocations,
            ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN,
            changeSetId
        );

        CreateGsiCheckTask createGsiCheckTask =
            new CreateGsiCheckTask(schemaName, primaryTableName, indexName, virtualColumns, backfillColumnMap);

        DdlTask absentTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.CREATING,
            IndexStatus.CHANGE_SET_START,
            true
        ).onExceptionTryRecoveryThenRollback();

        DdlTask deleteOnlyTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.CHANGE_SET_START,
            IndexStatus.DELETE_ONLY,
            true
        ).onExceptionTryRecoveryThenRollback();

        DdlTask writeOnlyTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.DELETE_ONLY,
            IndexStatus.WRITE_ONLY,
            true
        ).onExceptionTryRecoveryThenRollback();

        DdlTask writeReOrgTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.WRITE_ONLY,
            IndexStatus.WRITE_REORG,
            true
        ).onExceptionTryRecoveryThenRollback();

        DdlTask publicTask = new GsiUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            indexName,
            IndexStatus.WRITE_REORG,
            IndexStatus.PUBLIC,
            true
        ).onExceptionTryRecoveryThenRollback();

        taskList.add(changeSetStartTask);
        taskList.add(absentTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        // backfill
        taskList.add(
            new LogicalTableBackFillTask(schemaName, oldIndexName, indexName, virtualColumns, backfillColumnMap,
                modifyStringColumns, true, mirrorCopy, modifyColumn));
        taskList.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.ABSENT.toString()));
        taskList.add(deleteOnlyTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        if (stayAtDeleteOnly) {
            taskList.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL.toString()));
            return taskList;
        }
        taskList.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.DELETE_ONLY.toString()));
        taskList.add(writeOnlyTask);
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        taskList.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL.toString()));
        if (stayAtWriteOnly) {
            return taskList;
        }
        taskList.add(createGsiCheckTask);
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
     * drop index
     * alter table drop index
     */
    public static List<DdlTask> dropIndexTasks(String schemaName,
                                               String primaryTableName,
                                               String indexName,
                                               List<Pair<IndexStatus, IndexStatus>> statusChangeList) {
        List<DdlTask> taskList = new ArrayList<>();

        for (Pair<IndexStatus, IndexStatus> statusChange : statusChangeList) {
            DdlTask changeStatus = new GsiUpdateIndexStatusTask(
                schemaName,
                primaryTableName,
                indexName,
                statusChange.getKey(),
                statusChange.getValue(),
                true
            );
            taskList.add(changeStatus);
            taskList.add(new TableSyncTask(schemaName, primaryTableName));
        }

        return taskList;
    }

    public static List<DdlTask> dropGlobalIndexTasks(String schemaName,
                                                     String primaryTableName,
                                                     String indexName) {
        return dropIndexTasks(schemaName, primaryTableName, indexName, IndexStatus.dropGsiStatusChange());
    }

    public static List<DdlTask> dropColumnarIndexTasks(String schemaName,
                                                       String primaryTableName,
                                                       String indexName) {
        return dropIndexTasks(schemaName, primaryTableName, indexName, IndexStatus.dropColumnarIndexStatusChange());
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
        for (Pair<ColumnStatus, ColumnStatus> change : ColumnStatus.schemaChangeForAddColumn()) {
            ColumnStatus before = change.getKey();
            ColumnStatus after = change.getValue();
            // change status
            DdlTask task = changeGsiColumnStatus(schemaName, primaryTableName, indexName, columns, before, after);
            taskList.add(task);

            // sync meta
            taskList.add(new TableSyncTask(schemaName, primaryTableName));

            // backfill
            if (after.equals(ColumnStatus.WRITE_REORG) && CollectionUtils.isNotEmpty(backfillColumns)) {
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
            .parseStatementsWithDefaultFeatures(primaryTableDefinition, JdbcConstants.MYSQL).get(0).clone();

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

    public static String genAlterGlobalIndexDropColumnsSql(String indexName, List<String> columns) {
        if (indexName == null || columns == null || columns.isEmpty()) {
            return null;
        }
        String hint = "/*+TDDL:CMD_EXTRA(DDL_ON_GSI=true)*/";
        return hint + "alter table " + indexName + " drop column " + StringUtils.join(columns, ", drop column ");
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
     * add column on global index and change gsi phy table
     */
    public static AlterTableJobFactory alterGlobalIndexAddColumnFactory(String schemaName,
                                                                        String primaryTableName,
                                                                        String primaryTableDefinition,
                                                                        String indexName,
                                                                        List<String> columns,
                                                                        ExecutionContext executionContext,
                                                                        RelOptCluster cluster) {
        String sql = genAlterGlobalIndexAddColumnsSql(primaryTableDefinition, indexName, columns, executionContext);
        AlterTableBuilder alterTableBuilder =
            AlterTableBuilder.createGsiAddColumnsBuilder(schemaName, indexName, sql, columns, executionContext,
                cluster);
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

        for (Pair<ColumnStatus, ColumnStatus> statusChange : ColumnStatus.schemaChangeForDropColumn()) {
            DdlTask changeStatus = changeGsiColumnStatus(schemaName, primaryTableName, indexName, columns,
                statusChange.getKey(), statusChange.getValue());
            taskList.add(changeStatus);
            taskList.add(new TableSyncTask(schemaName, primaryTableName));
        }

        return taskList;
    }

    private static GsiUpdateIndexColumnStatusTask changeGsiColumnStatus(String schemaName, String primaryTableName,
                                                                        String indexName, List<String> columns,
                                                                        ColumnStatus before, ColumnStatus after) {
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
