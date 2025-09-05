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
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterColumnDefaultTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterForeignKeyTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableHideMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableInsertColumnsMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropEntitySecurityAttrTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.spec.AlterTableRollbacker;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableRewrittenDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.CommitTwoPhaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.CompensationPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.EmitPhysicalDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.FinishTwoPhaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.InitTwoPhaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.LogTwoPhaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.PrepareTwoPhaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.WaitTwoPhaseDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4AlterTable;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlManager;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils;
import com.alibaba.polardbx.executor.shadowtable.ShadowTableUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CHECK_TABLE_BEFORE_PHY_DDL;

public class AlterTableJobFactory extends DdlJobFactory {

    protected final PhysicalPlanData physicalPlanData;
    protected final String schemaName;
    protected final String logicalTableName;
    protected final AlterTablePreparedData prepareData;
    protected final LogicalAlterTable logicalAlterTable;

    /**
     * Whether altering a gsi table.
     */
    private boolean alterGsiTable = false;
    private String primaryTableName;

    /**
     * Whether altering a gsi table for repartition
     */
    private boolean repartition = false;

    /**
     * Whether generate validate table task
     */
    protected boolean validateExistence = true;

    private Boolean supportTwoPhaseDdl = false;

    private String finalStatus = "FINISH";

    static final String INPLACE_ALGORITHM = "INPLACE";

    static final String COPY_ALGORITHM = "COPY";

    static final String DEFAULT_ALGORITHM = "DEFAULT";

    static final String INSTANT_ALGORITHM = "INSTANT";

    static final String ALGORITHM = "ALGORITHM";

    private enum DdlAlgorithmType {
        INPLACE_ADD_DROP_COLUMN_INDEX,
        COPY_ADD_DROP_COLUMN_INDEX,
        INPLACE_MODIFY_CHANGE_COLUMN,
        COPY_MODIFY_CHANGE_COLUMN,

        OMC_MODIFY_CHANGE_COLUMN,
        IDEMPOTENT_MODIFY_CHANGE_COLUMN,

        UNKNOWN_MODIFY_CHANGE_COLUMN_ALGORITHM,

        UNKNOWN_ALGORITHM

    }

    protected ExecutionContext executionContext;

    public AlterTableJobFactory(PhysicalPlanData physicalPlanData,
                                AlterTablePreparedData preparedData,
                                LogicalAlterTable logicalAlterTable,
                                ExecutionContext executionContext) {
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.physicalPlanData = physicalPlanData;
        this.prepareData = preparedData;
        this.logicalAlterTable = logicalAlterTable;
        this.executionContext = executionContext;
    }

    public void setSupportTwoPhaseDdl(boolean supportTwoPhaseDdl) {
        this.supportTwoPhaseDdl = supportTwoPhaseDdl;
    }

    public void setFinalStatus(String finalStatus) {
        this.finalStatus = finalStatus;
    }

    public void withAlterGsi(boolean alterGsi, String primaryTableName) {
        this.alterGsiTable = alterGsi;
        this.primaryTableName = primaryTableName;
    }

    public void withAlterGsi4Repartition(boolean alterGsi, boolean repartition, String primaryTableName) {
        withAlterGsi(alterGsi, primaryTableName);
        this.repartition = repartition;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        TableGroupConfig tableGroupConfig = isNewPart ? physicalPlanData.getTableGroupConfig() : null;
        Boolean isPushDownMultipleStatement = ((AlterTablePreparedData) prepareData).isPushDownMultipleStatement();
        DdlTask validateTask = this.validateExistence ? new AlterTableValidateTask(schemaName, logicalTableName,
            logicalAlterTable.getSqlAlterTable().getSourceSql(), prepareData.getTableVersion(),
            isPushDownMultipleStatement, tableGroupConfig) : new EmptyTask(schemaName);

        final boolean isDropColumnOrDropIndex =
            CollectionUtils.isNotEmpty(prepareData.getDroppedColumns())
                || CollectionUtils.isNotEmpty(prepareData.getDroppedIndexes());
        final boolean isAddColumnOrAddIndex =
            CollectionUtils.isNotEmpty(prepareData.getAddedColumns())
                || CollectionUtils.isNotEmpty(prepareData.getAddedColumns());
        final boolean isModifyColumn =
            CollectionUtils.isNotEmpty(prepareData.getUpdatedColumns());

        List<DdlTask> alterGsiMetaTasks = new ArrayList<>();
        if (withRenameColumn(logicalAlterTable.getNativeSql())) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we don't support rename column in this version, you can use change column instead or upgrade to later version!");
        }
        if (this.alterGsiTable) {
            // TODO(moyi) simplify these tasks, which could be executed batched
            if (CollectionUtils.isNotEmpty(prepareData.getDroppedColumns())) {
                alterGsiMetaTasks.addAll(GsiTaskFactory.alterGlobalIndexDropColumnTasks(
                    schemaName,
                    primaryTableName,
                    logicalTableName,
                    prepareData.getDroppedColumns()));
            }

            if (CollectionUtils.isNotEmpty(prepareData.getAddedColumns())) {
                alterGsiMetaTasks.addAll(GsiTaskFactory.alterGlobalIndexAddColumnsStatusTasks(
                    schemaName,
                    primaryTableName,
                    logicalTableName,
                    prepareData.getAddedColumns(),
                    prepareData.getBackfillColumns(),
                    prepareData.getIsNullableMap()));
            }
        }

        // End alter column default after gsi physical ddl is finished
        DdlTask beginAlterColumnDefault = null;
        DdlTask beginAlterColumnDefaultSyncTask = null;
        if (!this.alterGsiTable && CollectionUtils.isNotEmpty(prepareData.getAlterDefaultColumns())) {
            beginAlterColumnDefault =
                new AlterColumnDefaultTask(schemaName, logicalTableName, prepareData.getAlterDefaultColumns(), true);
            beginAlterColumnDefaultSyncTask = new TableSyncTask(schemaName, logicalTableName);
            beginAlterColumnDefault.setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
            beginAlterColumnDefaultSyncTask.setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
        }

        boolean isForeignKeysDdl =
            !prepareData.getAddedForeignKeys().isEmpty() || !prepareData.getDroppedForeignKeys().isEmpty();
        boolean isForeignKeyCdcMark = isForeignKeysDdl && !executionContext.getDdlContext().isFkRepartition();

        boolean isRenameCci = false;
        String cciTableName = null;
        if (prepareData.isColumnar() && !prepareData.getRenamedIndexes().isEmpty()) {
            TableMeta cciTableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
                .getTableWithNull(prepareData.getRenamedIndexes().get(0).getValue());
            isRenameCci = cciTableMeta != null && cciTableMeta.isColumnar();
            if (isRenameCci) {
                cciTableName = prepareData.getRenamedIndexes().get(0).getValue();
            }
        }

        Boolean generateTwoPhaseDdlTask = supportTwoPhaseDdl;
        DdlTask phyDdlTask = new AlterTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);
        if (this.repartition) {
            ((AlterTablePhyDdlTask) phyDdlTask).setSourceSql(logicalAlterTable.getNativeSql());
            generateTwoPhaseDdlTask = false;
        }
        if (generateTwoPhaseDdlTask) {
            generateTwoPhaseDdlTask = checkIfGenerateTwoPhaseDdl(prepareData);
        }
        // TODO: exclude Physical Partition Table on Two phase ddl.
        // TODO: exclude foreign key table.

        Boolean withForeignKey = false;

        physicalPlanData.setAlterTablePreparedData(prepareData);
        DdlTask cdcDdlMarkTask;
        if (this.prepareData.isOnlineModifyColumnIndexTask() || CBOUtil.isOss(schemaName,
            logicalTableName)) {
            cdcDdlMarkTask = null;
        } else if (this.logicalAlterTable.isRewrittenAlterSql()) {
            // Use rewritten sql to mark cdc instead of sql in ddl context
            cdcDdlMarkTask = new CdcAlterTableRewrittenDdlMarkTask(schemaName, physicalPlanData,
                logicalAlterTable.getBytesSql().toString(), isForeignKeyCdcMark);
        } else {
            if (ignoreMarkCdcDDL()) {
                cdcDdlMarkTask = null;
            } else if (isRenameCci) {
                cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, false, isForeignKeyCdcMark,
                    prepareData.getDdlVersionId());
                ((CdcDdlMarkTask) cdcDdlMarkTask).setCci(true);
            } else {
                cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, false, isForeignKeyCdcMark,
                    prepareData.getDdlVersionId());
            }
        }

        DdlTask beforeUpdateMetaTask = phyDdlTask;
        DdlTask updateMetaTask = null;
        if (!this.repartition) {
            updateMetaTask = new AlterTableChangeMetaTask(
                schemaName,
                logicalTableName,
                physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(),
                physicalPlanData.getKind(),
                physicalPlanData.isPartitioned(),
                prepareData.getDroppedColumns(),
                prepareData.getAddedColumns(),
                prepareData.getUpdatedColumns(),
                prepareData.getChangedColumns(),
                prepareData.isTimestampColumnDefault(),
                prepareData.getSpecialDefaultValues(),
                prepareData.getSpecialDefaultValueFlags(),
                prepareData.getDroppedIndexes(),
                prepareData.getAddedIndexes(),
                prepareData.getAddedIndexesWithoutNames(),
                prepareData.getRenamedIndexes(),
                prepareData.isPrimaryKeyDropped(),
                prepareData.getAddedPrimaryKeyColumns(),
                prepareData.getColumnAfterAnother(),
                prepareData.isLogicalColumnOrder(),
                prepareData.getTableComment(),
                prepareData.getTableRowFormat(),
                physicalPlanData.getSequence(),
                prepareData.isOnlineModifyColumnIndexTask(),
                prepareData.getDdlVersionId());
        } else {
            // only add columns
            updateMetaTask = new AlterTableInsertColumnsMetaTask(
                schemaName,
                logicalTableName,
                physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(),
                prepareData.getAddedColumns()
            );
        }

        DdlTask tableSyncTaskAfterShowing = new TableSyncTask(schemaName, logicalTableName);

        ExecutableDdlJob4AlterTable executableDdlJob = new ExecutableDdlJob4AlterTable();

        List<DdlTask> taskList = null;
        if (isDropColumnOrDropIndex) {
            DdlTask hideMetaTask =
                new AlterTableHideMetaTask(schemaName, logicalTableName,
                    prepareData.getDroppedColumns(),
                    prepareData.getDroppedIndexes());
            DropEntitySecurityAttrTask dropESATask =
                createDropESATask(schemaName, logicalTableName, prepareData.getDroppedColumns());
            DdlTask tableSyncTaskAfterHiding = new TableSyncTask(schemaName, logicalTableName);
            Pair<DdlAlgorithmType, Long> alterCheckResult =
                alterTableViaDefaultAlgorithm(generateTwoPhaseDdlTask, isModifyColumn);
            DdlAlgorithmType ddlAlgorithmType = alterCheckResult.getKey();
            Long twoPhaseDdlId = alterCheckResult.getValue();
            if (!generateTwoPhaseDdlTask || ddlAlgorithmType == DdlAlgorithmType.COPY_ADD_DROP_COLUMN_INDEX
                || ddlAlgorithmType == DdlAlgorithmType.UNKNOWN_ALGORITHM) {
                taskList = Lists.newArrayList(
                    validateTask,
                    hideMetaTask,
                    tableSyncTaskAfterHiding,
                    beginAlterColumnDefault,
                    beginAlterColumnDefaultSyncTask,
                    phyDdlTask,
                    updateMetaTask,
                    cdcDdlMarkTask,
                    dropESATask
                ).stream().filter(Objects::nonNull).collect(Collectors.toList());
            } else {
                taskList = Lists.newArrayList(
                    validateTask,
                    hideMetaTask,
                    dropESATask,
                    tableSyncTaskAfterHiding
                );
                List<DdlTask> twoPhaseDdlTasks = generateTwoPhaseDdlTask(ddlAlgorithmType, twoPhaseDdlId);
                taskList.addAll(twoPhaseDdlTasks);
                taskList.add(updateMetaTask);
                taskList.add(cdcDdlMarkTask);
                taskList = taskList.stream().filter(Objects::nonNull).collect(Collectors.toList());
                beforeUpdateMetaTask = twoPhaseDdlTasks.get(twoPhaseDdlTasks.size() - 1);
            }
        } else {
            // 1. physical DDL
            // 2. alter GSI meta if necessary
            // 3. update meta
            // 4. sync table
            String originDdl = executionContext.getDdlContext().getDdlStmt();
            if (AlterTableRollbacker.checkIfRollbackable(originDdl)) {
                phyDdlTask = phyDdlTask.onExceptionTryRecoveryThenRollback();
            }
            Pair<DdlAlgorithmType, Long> alterCheckResult =
                alterTableViaDefaultAlgorithm(generateTwoPhaseDdlTask, isModifyColumn);
            DdlAlgorithmType algorithmType = alterCheckResult.getKey();
            Long twoPhaseDdlId = alterCheckResult.getValue();
            if (isRenameCci) {
                taskList = Lists.newArrayList(
                    validateTask,
                    updateMetaTask,
                    cdcDdlMarkTask,
                    // must sync cci table to reload table group cache
                    new TableSyncTask(schemaName, cciTableName)
                ).stream().filter(Objects::nonNull).collect(Collectors.toList());
            } else if (!generateTwoPhaseDdlTask || algorithmType == DdlAlgorithmType.COPY_ADD_DROP_COLUMN_INDEX
                || algorithmType == DdlAlgorithmType.UNKNOWN_MODIFY_CHANGE_COLUMN_ALGORITHM
                || algorithmType == DdlAlgorithmType.UNKNOWN_ALGORITHM) {
                taskList = Lists.newArrayList(
                    validateTask,
                    beginAlterColumnDefault,
                    beginAlterColumnDefaultSyncTask,
                    phyDdlTask,
                    updateMetaTask,
                    cdcDdlMarkTask
                ).stream().filter(Objects::nonNull).collect(Collectors.toList());
            } else {
                taskList = Lists.newArrayList(
                    validateTask,
                    beginAlterColumnDefault,
                    beginAlterColumnDefaultSyncTask
                );

                List<DdlTask> twoPhaseDdlTasks = generateTwoPhaseDdlTask(algorithmType, twoPhaseDdlId);
                taskList.addAll(twoPhaseDdlTasks);
                taskList.add(updateMetaTask);
                taskList.add(cdcDdlMarkTask);
                taskList = taskList.stream().filter(Objects::nonNull).collect(Collectors.toList());
                beforeUpdateMetaTask = twoPhaseDdlTasks.get(twoPhaseDdlTasks.size() - 1);
            }
        }
        taskList.addAll(alterGsiMetaTasks);

        if (isForeignKeysDdl) {
            DdlTask updateForeignKeysTask =
                new AlterForeignKeyTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                    physicalPlanData.getDefaultPhyTableName(), prepareData.getAddedForeignKeys(),
                    prepareData.getDroppedForeignKeys(), false);
            taskList.add(updateForeignKeysTask);
        }

        // sync foreign key table meta
        syncFkTables(taskList);

        taskList.add(tableSyncTaskAfterShowing);

//        if (StringUtils.equalsIgnoreCase(finalStatus, "ONLY_FINISH")) {
//            taskList = generateTwoPhaseDdlTask(isPhysicalOnline);
//            executableDdlJob.addSequentialTasks(taskList);
//            return executableDdlJob;
//        }
        executableDdlJob.addSequentialTasks(taskList);

        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(tableSyncTaskAfterShowing);

        executableDdlJob.setTableValidateTask((BaseValidateTask) validateTask);
        executableDdlJob.setBeforeChangeMetaTask(beforeUpdateMetaTask);
        executableDdlJob.setChangeMetaTask(updateMetaTask);
        executableDdlJob.setTableSyncTask((TableSyncTask) tableSyncTaskAfterShowing);

        return executableDdlJob;
    }

    private DropEntitySecurityAttrTask createDropESATask(
        String schemaName, String tableName, List<String> droppedColumns) {
        if (droppedColumns == null || droppedColumns.size() == 0) {
            return null;
        }
        List<LBACSecurityEntity> esaList = new ArrayList<>();
        for (String col : droppedColumns) {
            LBACSecurityLabel label = LBACSecurityManager.getInstance().getColumnLabel(schemaName, tableName, col);
            if (label != null) {
                esaList.add(new LBACSecurityEntity(
                    LBACSecurityEntity.EntityKey.createColumnKey(schemaName, tableName, col),
                    LBACSecurityEntity.EntityType.COLUMN,
                    label.getLabelName()
                ));
            }
        }
        return esaList.isEmpty() ? null : new DropEntitySecurityAttrTask(schemaName, tableName, esaList);
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));

        // exclude foreign key tables
        excludeFkTables(resources);
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        String tgName = FactoryUtils.getTableGroupNameByTableName(schemaName, logicalTableName);
        if (tgName != null) {
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    protected Boolean checkAlgorithmSpecificationCopy(SQLAlterTableStatement alterTable) {
        List<MySqlAlterTableOption> alterTableItems = alterTable.getItems().stream()
            .filter(o -> o instanceof MySqlAlterTableOption)
            .map(o -> (MySqlAlterTableOption) o)
            .filter(o -> o.getName().equalsIgnoreCase(ALGORITHM))
            .collect(Collectors.toList());
        return !(alterTableItems.isEmpty()) && alterTableItems.stream()
            .allMatch(o -> o.getValue().toString().equalsIgnoreCase(COPY_ALGORITHM));
    }

    protected Boolean checkAlgorithmSpecificationOthers(SQLAlterTableStatement alterTable) {
        List<String> supportedAlgorithms =
            Arrays.asList(INPLACE_ALGORITHM, COPY_ALGORITHM, INSTANT_ALGORITHM, DEFAULT_ALGORITHM);
        List<MySqlAlterTableOption> alterTableItems = alterTable.getItems().stream()
            .filter(o -> o instanceof MySqlAlterTableOption)
            .map(o -> (MySqlAlterTableOption) o)
            .filter(o -> o.getName().equalsIgnoreCase(ALGORITHM))
            .collect(Collectors.toList());
        return !(alterTableItems.isEmpty()) && alterTableItems.stream()
            .anyMatch(o -> !supportedAlgorithms.contains(o.getValue().toString().toUpperCase()));
    }

    protected DdlAlgorithmType determineModifyColumnAlgorithmByReachedPoints(Pair<Boolean, Boolean> reachedPoints) {
        if (reachedPoints.getKey() && reachedPoints.getValue()) {
            return DdlAlgorithmType.INPLACE_MODIFY_CHANGE_COLUMN;
        } else if (reachedPoints.getKey() && !reachedPoints.getValue()) {
            return DdlAlgorithmType.IDEMPOTENT_MODIFY_CHANGE_COLUMN;
        } else if (!reachedPoints.getKey() && reachedPoints.getValue()) {
            return DdlAlgorithmType.COPY_MODIFY_CHANGE_COLUMN;
        } else {
            return DdlAlgorithmType.UNKNOWN_MODIFY_CHANGE_COLUMN_ALGORITHM;
        }
    }

    protected DdlAlgorithmType determineModifyColumnAlgorithm(String origSqlTemplate, Long id) {
        DdlAlgorithmType algorithmType;
        String shadowTableName = ShadowTableUtils.generateShadowTableName(logicalTableName, id);
        String groupName = physicalPlanData.getDefaultDbIndex();
        String originalTableName = physicalPlanData.getDefaultPhyTableName();
        ShadowTableUtils.createShadowTable(executionContext, schemaName, logicalTableName, groupName, originalTableName,
            shadowTableName);
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) FastsqlUtils.parseSql(origSqlTemplate).get(0);
        alterTable.setTargetImplicitTableGroup(null);
        alterTable.getIndexTableGroupPair().clear();
        // If specify copy, then algorithm is copy.
        if (checkAlgorithmSpecificationCopy(alterTable)) {
            algorithmType = DdlAlgorithmType.COPY_MODIFY_CHANGE_COLUMN;
            return algorithmType;
        } else if (checkAlgorithmSpecificationOthers(alterTable)) {
            algorithmType = DdlAlgorithmType.UNKNOWN_ALGORITHM;
            return algorithmType;
        }
        alterTable.setTableSource(
            new SQLExprTableSource(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(shadowTableName))));
        alterTable.addItem(new MySqlAlterTableOption(ALGORITHM, INPLACE_ALGORITHM));
        String alterTableStmt = alterTable.toString();
        try {
            SQLRecorderLogger.ddlLogger.info(
                String.format("<MultiPhaseDdl>trace physical table %s with ddl %s", shadowTableName, alterTableStmt));
            ShadowTableUtils.initTraceShadowTable(executionContext, schemaName, logicalTableName, groupName,
                shadowTableName, id);
            ShadowTableUtils.alterShadowTable(executionContext, schemaName, logicalTableName, groupName,
                shadowTableName,
                alterTableStmt);
            Pair<Boolean, Boolean> reachedPoints =
                ShadowTableUtils.fetchTraceTableDdl(executionContext, schemaName, logicalTableName, groupName,
                    shadowTableName, id);
            algorithmType = determineModifyColumnAlgorithmByReachedPoints(reachedPoints);
        } catch (Exception exception) {
            // If inplace not ok, then algorithm is copy.
            if (exception.getMessage() != null && exception.getMessage()
                .contains("ALGORITHM=INPLACE is not supported")) {
                algorithmType = DdlAlgorithmType.COPY_MODIFY_CHANGE_COLUMN;
            } else {
                // there are unknown error
                throw exception;
            }
        } finally {
            ShadowTableUtils.clearShadowTable(executionContext, schemaName, logicalTableName, groupName,
                shadowTableName);
            ShadowTableUtils.finishTraceShadowTable(executionContext, schemaName, logicalTableName, groupName,
                shadowTableName, id);
        }
        return algorithmType;
    }

    protected Boolean compareTheSame(String originalCreateTableSql, String afterCreateTableSql) {
        SQLCreateTableStatement originalCreateTableStmt = (SQLCreateTableStatement)
            FastsqlUtils.parseSql(originalCreateTableSql).get(0);
        SQLCreateTableStatement afterCreateTableStmt = (SQLCreateTableStatement)
            FastsqlUtils.parseSql(afterCreateTableSql).get(0);
        originalCreateTableSql = originalCreateTableStmt.toString();
        afterCreateTableSql = afterCreateTableStmt.toString();
        return originalCreateTableSql.equals(afterCreateTableSql);
    }

    protected Boolean withRenameColumn(String origSql) {
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) FastsqlUtils.parseSql(origSql).get(0);
        List<SQLAlterTableItem> alterTableItems = alterTable.getItems();
        for (SQLAlterTableItem alterTableItem : alterTableItems) {
            if(alterTableItem instanceof SQLAlterTableRenameColumn){
                return true;
            }
        }
        return false;
    }

    protected DdlAlgorithmType determineOnlineDdlAlgorithm(String origSql, Long id) {
        DdlAlgorithmType algorithmType;
        String shadowTableName = ShadowTableUtils.generateShadowTableName(logicalTableName, id);
        String groupName = physicalPlanData.getDefaultDbIndex();
        String originalTableName = physicalPlanData.getDefaultPhyTableName();
        ShadowTableUtils.createShadowTable(executionContext, schemaName, logicalTableName, groupName, originalTableName,
            shadowTableName);
        // If specify copy, then algorithm is copy.
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) FastsqlUtils.parseSql(origSql).get(0);
        alterTable.setTableSource(
            new SQLExprTableSource(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(shadowTableName))));
        if (checkAlgorithmSpecificationCopy(alterTable)) {
            algorithmType = DdlAlgorithmType.COPY_ADD_DROP_COLUMN_INDEX;
            return algorithmType;
        } else if (checkAlgorithmSpecificationOthers(alterTable)) {
            algorithmType = DdlAlgorithmType.UNKNOWN_ALGORITHM;
            return algorithmType;
        }
        alterTable.addItem(new MySqlAlterTableOption(ALGORITHM, INPLACE_ALGORITHM));
        alterTable.setTargetImplicitTableGroup(null);
        alterTable.getIndexTableGroupPair().clear();
        String alterTableStmt = alterTable.toString();
        try {
            SQLRecorderLogger.ddlLogger.info(
                String.format("<MultiPhaseDdl>trace physical table %s with ddl %s", shadowTableName, alterTableStmt));
            ShadowTableUtils.alterShadowTable(executionContext, schemaName, logicalTableName, groupName,
                shadowTableName,
                alterTableStmt);
            algorithmType = DdlAlgorithmType.INPLACE_ADD_DROP_COLUMN_INDEX;
        } catch (Exception exception) {
            // If inplace not ok, then algorithm is copy.
            if (exception.getMessage().contains("ALGORITHM=INPLACE is not supported")) {
                algorithmType = DdlAlgorithmType.COPY_ADD_DROP_COLUMN_INDEX;
            } else {
                // there are unknown error
                throw exception;
            }
        } finally {
            ShadowTableUtils.clearShadowTable(executionContext, schemaName, logicalTableName, groupName,
                shadowTableName);
        }
        return algorithmType;
    }

    protected Pair<DdlAlgorithmType, Long> alterTableViaDefaultAlgorithm(Boolean generateTwoPhaseDdlTask,
                                                                         Boolean isModifyColumn) {
        Boolean supportTwoPhaseDdlOnDn =
            TwoPhaseDdlManager.checkEnableTwoPhaseDdlOnDn(schemaName, logicalTableName, executionContext);
        Long twoPhaseDdlId = TwoPhaseDdlManager.generateTwoPhaseDdlManagerId(schemaName, logicalTableName);
        String origSqlTemplate = logicalAlterTable.getNativeSql();
        DdlAlgorithmType algorithmType;
        if (!supportTwoPhaseDdlOnDn || !generateTwoPhaseDdlTask) {
            algorithmType = DdlAlgorithmType.UNKNOWN_ALGORITHM;
        } else if (isModifyColumn) {
            algorithmType = determineModifyColumnAlgorithm(origSqlTemplate, twoPhaseDdlId);
        } else {
            algorithmType = determineOnlineDdlAlgorithm(origSqlTemplate, twoPhaseDdlId);
        }
        return Pair.of(algorithmType, twoPhaseDdlId);
    }

    protected List<DdlTask> generateTwoPhaseDdlTask(DdlAlgorithmType algorithmType, Long twoPhaseDdlId) {
        String origSqlTemplate = logicalAlterTable.getSqlAlterTable().getSourceSql();
        int waitPreparedDelay =
            executionContext.getParamManager().getInt(ConnectionParams.MULTI_PHASE_WAIT_PREPARED_DELAY);
        int waitCommitDelay = executionContext.getParamManager().getInt(ConnectionParams.MULTI_PHASE_WAIT_COMMIT_DELAY);
        int prepareDelay = executionContext.getParamManager().getInt(ConnectionParams.MULTI_PHASE_PREPARE_DELAY);
        int commitDelay = executionContext.getParamManager().getInt(ConnectionParams.MULTI_PHASE_COMMIT_DELAY);
//        if (nondefaultAlgriothm) {
//            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
//                "We don't support set specified algorithm under two phase ddl,"
//                    + " you can use /*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=false)*/ to avoid this error.");
//        }
        List<DdlTask> taskList = new ArrayList<>();
        final boolean preClear =
            StringUtils.equalsIgnoreCase(finalStatus, "PRE_CLEAR");
        final boolean stayAtInit =
            StringUtils.equalsIgnoreCase(finalStatus, "INIT");
        final boolean stayAtEmit =
            StringUtils.equalsIgnoreCase(finalStatus, "EMIT");
        final boolean stayAtWaitPrepared =
            StringUtils.equalsIgnoreCase(finalStatus, "WAIT_PREPARE");
        final boolean stayAtPrepare =
            StringUtils.equalsIgnoreCase(finalStatus, "PREPARE");
        final boolean stayAtWaitCommit =
            StringUtils.equalsIgnoreCase(finalStatus, "WAIT_COMMIT");
        final boolean skipFinish =
            StringUtils.equalsIgnoreCase(finalStatus, "SKIP_FINISH");
        final boolean stayAtCommit =
            StringUtils.equalsIgnoreCase(finalStatus, "COMMIT");
        Map<String, Set<String>> tableTopology = new HashMap<>();
        for (String phyDbName : physicalPlanData.getTableTopology().keySet()) {
            String groupName = phyDbName;
            tableTopology.put(groupName, physicalPlanData.getTableTopology().get(phyDbName).stream().
                flatMap(List::stream).collect(Collectors.toSet()));
        }
        DdlTask clearTwoPhaseDdlTask =
            new FinishTwoPhaseDdlTask(schemaName, logicalTableName, tableTopology, origSqlTemplate
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId);
        DdlTask initTwoPhaseDdlTask =
            new InitTwoPhaseDdlTask(schemaName, logicalTableName, tableTopology, origSqlTemplate
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId, new HashMap<>());
        DdlTask emitPhysicalDdlTask =
            new EmitPhysicalDdlTask(schemaName, logicalTableName, tableTopology, origSqlTemplate
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId, null);
        DdlTask waitPreparedDdlTask =
            new WaitTwoPhaseDdlTask(schemaName, logicalTableName, tableTopology,
                TwoPhaseDdlUtils.TWO_PHASE_DDL_WAIT_PREPARE_TASK_NAME
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId, waitPreparedDelay);
        DdlTask prepareTwoPhaseDdlTask =
            new PrepareTwoPhaseDdlTask(schemaName, logicalTableName, tableTopology
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId, prepareDelay);
        DdlTask waitCommitDdlTask =
            new WaitTwoPhaseDdlTask(schemaName, logicalTableName, tableTopology,
                TwoPhaseDdlUtils.TWO_PHASE_DDL_WAIT_COMMIT_TASK_NAME
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId, waitCommitDelay);
        DdlTask commitTwoPhaseDdlTask =
            new CommitTwoPhaseDdlTask(schemaName, logicalTableName, tableTopology
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId,
                origSqlTemplate, commitDelay);
        DdlTask finishTwoPhaseDdlTask =
            new FinishTwoPhaseDdlTask(schemaName, logicalTableName, tableTopology, origSqlTemplate
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId);
        DdlTask logTwoPhaseDdlTask =
            new LogTwoPhaseDdlTask(schemaName, logicalTableName, tableTopology, origSqlTemplate
                , ComplexTaskMetaManager.ComplexTaskType.TWO_PHASE_ALTER_TABLE, twoPhaseDdlId);
        DdlTask compensationPhysicalDdlTask =
            new CompensationPhyDdlTask(schemaName, logicalTableName, physicalPlanData);
        if (preClear) {
            taskList.add(clearTwoPhaseDdlTask);
        }
        taskList.add(initTwoPhaseDdlTask);
        if (stayAtInit) {
            return taskList;
        }
        taskList.add(emitPhysicalDdlTask);
        if (stayAtEmit) {
            return taskList;
        }
        if (algorithmType == DdlAlgorithmType.INPLACE_ADD_DROP_COLUMN_INDEX
            || algorithmType == DdlAlgorithmType.INPLACE_MODIFY_CHANGE_COLUMN
            || algorithmType == DdlAlgorithmType.IDEMPOTENT_MODIFY_CHANGE_COLUMN) {
            taskList.add(waitPreparedDdlTask);
            if (stayAtWaitPrepared) {
                return taskList;
            }
            taskList.add(prepareTwoPhaseDdlTask);
            if (stayAtPrepare) {
                return taskList;
            }
        }
        if (algorithmType == DdlAlgorithmType.INPLACE_MODIFY_CHANGE_COLUMN
            || algorithmType == DdlAlgorithmType.INPLACE_ADD_DROP_COLUMN_INDEX
            || algorithmType == DdlAlgorithmType.COPY_ADD_DROP_COLUMN_INDEX
            || algorithmType == DdlAlgorithmType.COPY_MODIFY_CHANGE_COLUMN) {
            taskList.add(waitCommitDdlTask);
            if (stayAtWaitCommit) {
                return taskList;
            }
            taskList.add(commitTwoPhaseDdlTask);
            if (stayAtCommit || skipFinish) {
                return taskList;
            }
        }
        taskList.add(logTwoPhaseDdlTask);
        taskList.add(finishTwoPhaseDdlTask);
        taskList.add(compensationPhysicalDdlTask);
        return taskList;
    }

    public void validateExistence(boolean validateExistence) {
        this.validateExistence = validateExistence;
    }

    private boolean ignoreMarkCdcDDL() {

        TableMeta tableMeta = null;
        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        if (schemaManager != null) {
            tableMeta = schemaManager.getTable(logicalTableName);
        }

        boolean isAutoPartition = tableMeta != null && tableMeta.isAutoPartition();
        boolean isGSI = tableMeta != null && tableMeta.isGsi();
        if (!isAutoPartition && !isGSI) {
            return false;
        }

        return this.alterGsiTable;
    }

    public void syncFkTables(List<DdlTask> taskList) {
        List<Pair<String, String>> relatedTables = fkRelatedTables();
        for (Pair<String, String> relatedTable : relatedTables) {
            taskList.add(new TableSyncTask(relatedTable.getKey(), relatedTable.getValue()));
        }
    }

    public void excludeFkTables(Set<String> resources) {
        List<Pair<String, String>> relatedTables = fkRelatedTables();
        for (Pair<String, String> relatedTable : relatedTables) {
            resources.add(concatWithDot(relatedTable.getKey(), relatedTable.getValue()));
        }
    }

    public List<Pair<String, String>> fkRelatedTables() {
        List<Pair<String, String>> relatedTables = new ArrayList<>();
        if (!prepareData.getAddedForeignKeys().isEmpty()) {
            ForeignKeyData data = prepareData.getAddedForeignKeys().get(0);
            relatedTables.add(new Pair<>(data.refSchema, data.refTableName));
        }
        if (!prepareData.getDroppedForeignKeys().isEmpty()) {
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
            for (ForeignKeyData data : tableMeta.getForeignKeys().values()) {
                if (data.constraint.equals(prepareData.getDroppedForeignKeys().get(0))) {
                    relatedTables.add(new Pair<>(data.refSchema, data.refTableName));
                }
            }
        }
        if (!prepareData.getChangedColumns().isEmpty()) {
            Map<String, String> columnNameMap = new HashMap<>();
            for (Pair<String, String> pair : prepareData.getChangedColumns()) {
                columnNameMap.put(pair.getValue(), pair.getKey());
            }
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
            Map<String, ForeignKeyData> referencedForeignKeys = tableMeta.getReferencedForeignKeys();
            for (Map.Entry<String, ForeignKeyData> e : referencedForeignKeys.entrySet()) {
                boolean sync = false;
                for (int i = 0; i < e.getValue().columns.size(); ++i) {
                    String oldColumn = e.getValue().refColumns.get(i);
                    if (columnNameMap.containsKey(oldColumn)) {
                        sync = true;
                        break;
                    }
                }
                if (sync) {
                    String referencedSchemaName = e.getValue().schema;
                    String referredTableName = e.getValue().tableName;
                    relatedTables.add(new Pair<>(referencedSchemaName, referredTableName));
                }
            }

            Map<String, ForeignKeyData> foreignKeys = tableMeta.getForeignKeys();
            if (!foreignKeys.isEmpty()) {
                for (Map.Entry<String, ForeignKeyData> e : foreignKeys.entrySet()) {
                    boolean sync = false;
                    for (int i = 0; i < e.getValue().columns.size(); ++i) {
                        String oldColumn = e.getValue().columns.get(i);
                        if (columnNameMap.containsKey(oldColumn)) {
                            sync = true;
                            break;
                        }
                    }
                    if (sync) {
                        relatedTables.add(new Pair<>(e.getValue().refSchema, e.getValue().refTableName));
                    }
                }
            }
        }
        return relatedTables;
    }

    private int fetchSize(List<String> lst) {
        return (lst == null) ? 0 : lst.size();
    }

    public Boolean checkTableOk(String schemaName, String tableName) {
        String sql = String.format("check table %s", SqlIdentifier.surroundWithBacktick(tableName));
        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(
            sql,
            schemaName,
            null
        );
        if (result.stream().allMatch(o -> o.get("MSG_TEXT").toString().equalsIgnoreCase("OK"))) {
            return true;
        } else {
            SQLRecorderLogger.ddlLogger.info(
                String.format("check table for %s.%s failed, the result is %s, we will execute ddl in legacy method.",
                    schemaName, tableName, result));
            return false;
        }
    }

    public Boolean checkIfGenerateTwoPhaseDdl(AlterTablePreparedData prepareData) {
        final boolean isDropColumnOrDropIndex =
            CollectionUtils.isNotEmpty(prepareData.getDroppedColumns())
                || CollectionUtils.isNotEmpty(prepareData.getDroppedIndexes());
        final boolean isAddColumnOrAddIndex =
            CollectionUtils.isNotEmpty(prepareData.getAddedColumns())
                || CollectionUtils.isNotEmpty(prepareData.getAddedIndexes())
                || CollectionUtils.isNotEmpty(prepareData.getAddedIndexes());
        final boolean isModifyColumn =
            CollectionUtils.isNotEmpty(prepareData.getUpdatedColumns());
        final int alterNums =
            fetchSize(prepareData.getDroppedColumns()) + fetchSize(prepareData.getDroppedIndexes())
                + fetchSize(prepareData.getAddedColumns()) + fetchSize(prepareData.getAddedIndexes())
                + fetchSize(prepareData.getAddedIndexesWithoutNames()) + fetchSize(prepareData.getUpdatedColumns());
        Engine engine =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName)
                .getEngine();
        LocalPartitionDefinitionInfo localPartitionDefinitionInfo =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName)
                .getLocalPartitionDefinitionInfo();
        Boolean withPhysicalPartition = Engine.isFileStore(engine) || (localPartitionDefinitionInfo != null);
        Boolean withForeignKey =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName).hasForeignKey();
        Boolean withCci =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName).withCci();
        Boolean supportTwoPhaseDdl = false;
        Boolean checkTableOk = true;
        Boolean checkTable = executionContext.getParamManager().getBoolean(CHECK_TABLE_BEFORE_PHY_DDL);
        if (isAddColumnOrAddIndex || isModifyColumn || isDropColumnOrDropIndex) {
            if (alterNums <= 1 && !withPhysicalPartition && !withForeignKey && !withCci) {
                if (checkTable) {
                    checkTableOk = checkTableOk(schemaName, logicalTableName);
                }
                if (checkTableOk) {
                    supportTwoPhaseDdl = true;
                }
            }
        }
        return supportTwoPhaseDdl;
    }
}
