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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameGsiUpdateMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcGsiDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableRenameIndex;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

public class RenameGsiJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final String gsiName;
    private final String newGsiName;
    private final ExecutionContext executionContext;

    public RenameGsiJobFactory(RenameTablePreparedData preparedData, ExecutionContext executionContext) {
        this.schemaName = preparedData.getSchemaName();
        this.gsiName = preparedData.getTableName();
        this.newGsiName = preparedData.getNewTableName();
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        GsiValidator.validateGsi(schemaName, gsiName);
        GsiValidator.validateCreateOnGsi(schemaName, newGsiName, executionContext);
        GsiValidator.validateAllowRenameOnTable(schemaName, gsiName, executionContext);

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(gsiName);
        String primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;

        DdlTask validateTask = new RenameTableValidateTask(schemaName, gsiName, newGsiName);
        DdlTask addMetaTask = new RenameTableAddMetaTask(schemaName, gsiName, newGsiName);

        DdlTask updateMetaTask = new RenameGsiUpdateMetaTask(schemaName, primaryTableName, gsiName, newGsiName, false);
        DdlTask syncTask = new TableSyncTask(schemaName, primaryTableName);

        List<DdlTask> taskList = new ArrayList<>();
        taskList.add(validateTask);
        taskList.add(addMetaTask);
        taskList.add(updateMetaTask);

        TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);
        if (!primaryTableMeta.isAutoPartition()) {
            // mark gsi task
            PhysicalPlanData physicalPlanData = new PhysicalPlanData();
            physicalPlanData.setKind(SqlKind.RENAME_TABLE);
            physicalPlanData.setLogicalTableName(primaryTableName);
            physicalPlanData.setSchemaName(schemaName);
            CdcGsiDdlMarkTask cdcDdlMarkTask =
                new CdcGsiDdlMarkTask(schemaName, physicalPlanData, primaryTableName, executionContext.getOriginSql());
            taskList.add(cdcDdlMarkTask);
        } else {
            // mark local index task
            PhysicalPlanData physicalPlanData = new PhysicalPlanData();
            physicalPlanData.setKind(SqlKind.ALTER_TABLE);
            physicalPlanData.setLogicalTableName(primaryTableName);
            physicalPlanData.setSchemaName(schemaName);
            CdcDdlMarkTask cdcDdlMarkTask =
                new CdcDdlMarkTask(schemaName, physicalPlanData, false, false, DEFAULT_DDL_VERSION_ID);
            taskList.add(cdcDdlMarkTask);
        }
        taskList.add(syncTask);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(validateTask);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, gsiName));
        resources.add(concatWithDot(schemaName, newGsiName));

        if (TableValidator.checkTableIsGsi(schemaName, gsiName)) {
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(gsiName);
            String primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            resources.add(concatWithDot(schemaName, primaryTableName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    /**
     * rename gsi table
     */
    public static ExecutableDdlJob create(RenameTablePreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new RenameGsiJobFactory(preparedData, executionContext).create();
    }

    /**
     * rename local index by rewrite sql (subjob)
     */
    public static DdlTask createRenameLocalIndex(String schemaName, SqlAlterTable sqlNode,
                                                 RenameLocalIndexPreparedData preparedData) {
        if (sqlNode == null || preparedData == null || sqlNode.getAlters().isEmpty()) {
            return null;
        }

        SqlAlterTableRenameIndex renameIndex = (SqlAlterTableRenameIndex) sqlNode.getAlters().get(0);
        String sourceSql = renameIndex.getSourceSql();
        List<SQLStatement> statementList = SQLUtils.parseStatementsWithDefaultFeatures(sourceSql, JdbcConstants.MYSQL);
        SQLAlterTableStatement stmt = (SQLAlterTableStatement) statementList.get(0);

        for (SQLAlterTableItem item : stmt.getItems()) {
            ((SQLAlterTableRenameIndex) item).setName(
                new SQLIdentifierExpr(surroundWithBacktick(preparedData.getOrgIndexName())));
            ((SQLAlterTableRenameIndex) item).setTo(
                new SQLIdentifierExpr(surroundWithBacktick(preparedData.getNewIndexName())));
        }
        String newRenameIndex = stmt.toString();

        for (SQLAlterTableItem item : stmt.getItems()) {
            ((SQLAlterTableRenameIndex) item).setName(
                new SQLIdentifierExpr(surroundWithBacktick(preparedData.getNewIndexName())));
            ((SQLAlterTableRenameIndex) item).setTo(
                new SQLIdentifierExpr(surroundWithBacktick(preparedData.getOrgIndexName())));
        }
        String rollbackRenameIndex = stmt.toString();
        SubJobTask ddlTask = new SubJobTask(schemaName, newRenameIndex, rollbackRenameIndex);
        ddlTask.setParentAcquireResource(true);
        ddlTask.setSkipCdcMarkTask(true);
        return ddlTask;
    }
}
