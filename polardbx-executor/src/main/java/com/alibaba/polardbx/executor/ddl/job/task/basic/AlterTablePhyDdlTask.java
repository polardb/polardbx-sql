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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.PhysicalDdlException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.spec.AlterTableRollbacker;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.properties.ConnectionParams.PHYSICAL_DDL_TASK_RETRY;

@Getter
@TaskName(name = "AlterTablePhyDdlTask")
public class AlterTablePhyDdlTask extends BasePhyDdlTask {

    private String logicalTableName;

    private String sourceSql;

    private String rollbackSql;

    private String rollbackSqlTemplate;

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public void setRollbackSql(String rollbackSql) {
        this.rollbackSql = rollbackSql;
    }

    public void setRollbackSqlTemplate(String rollbackSqlTemplate) {
        this.rollbackSqlTemplate = rollbackSqlTemplate;
    }

    @JSONCreator
    public AlterTablePhyDdlTask(String schemaName, String logicalTableName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
        this.logicalTableName = logicalTableName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        if (!executionContext.getParamManager().getBoolean(PHYSICAL_DDL_TASK_RETRY)) {
            onExceptionTryRollback();
        }

        try {
            super.executeImpl(executionContext);
        } catch (PhysicalDdlException e) {
            int successCount = e.getSuccessCount();
            if (successCount == 0) {
                enableRollback(this);
            } else {
                // Some physical DDLs failed && they do not support rollback,
                // so we forbid CANCEL DDL command here.
                if (!AlterTableRollbacker.checkIfRollbackable(executionContext.getDdlContext().getDdlStmt())) {
                    updateSupportedCommands(true, false, null);
                }
            }
            throw new PhysicalDdlException(e.getTotalCount(), e.getSuccessCount(), e.getFailCount(),
                e.getErrMsg(), e.getSimpleErrMsg());
        }
    }

    @Override
    protected List<RelNode> genRollbackPhysicalPlans(ExecutionContext executionContext) {
        if (StringUtils.isNotEmpty(rollbackSqlTemplate)) {
            return genReversedPhysicalPlansFromTemplate(rollbackSqlTemplate, executionContext);
        }

        if (StringUtils.isNotEmpty(rollbackSql)) {
            return genReversedPhysicalPlans(rollbackSql, executionContext);
        }

        String origSql = StringUtils.isNotEmpty(sourceSql) ? sourceSql : executionContext.getDdlContext().getDdlStmt();
        SQLAlterTableStatement alterTableStmt = (SQLAlterTableStatement) FastsqlUtils.parseSql(origSql).get(0);
        if (AlterTableRollbacker.checkIfRollbackable(alterTableStmt)) {
            String reversedSql = genReversedAlterTableStmt(alterTableStmt);
            return genReversedPhysicalPlans(reversedSql, executionContext);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The DDL job is not rollbackable because the DDL includes some operations that doesn't support rollback");
        }
    }

    protected List<RelNode> genReversedPhysicalPlansFromTemplate(String reversedSqlTemplate,
                                                                 ExecutionContext executionContext) {
        PhysicalPlanData newPhysicalPlanData = physicalPlanData.clone();
        newPhysicalPlanData.setSqlTemplate(reversedSqlTemplate);
        return DdlJobDataConverter.convertToPhysicalPlans(newPhysicalPlanData, executionContext);
    }

    protected List<RelNode> genReversedPhysicalPlans(String reversedSql, ExecutionContext executionContext) {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        SqlAlterTable reversedAlterTable =
            (SqlAlterTable) new FastsqlParser().parse(reversedSql, executionContext).get(0);
        reversedAlterTable = (SqlAlterTable) reversedAlterTable.accept(visitor);

        SqlIdentifier tableNameNode =
            new SqlIdentifier(Lists.newArrayList(schemaName, logicalTableName), SqlParserPos.ZERO);

        final RelOptCluster cluster =
            SqlConverter.getInstance(executionContext).createRelOptCluster(new PlannerContext(executionContext));
        AlterTable alterTable = AlterTable.create(cluster, reversedAlterTable, tableNameNode, null);

        LogicalAlterTable logicalAlterTable = LogicalAlterTable.create(alterTable);
        logicalAlterTable.prepareData();

        DdlPhyPlanBuilder alterTableBuilder =
            AlterTableBuilder.create(alterTable, logicalAlterTable.getAlterTablePreparedData(), executionContext)
                .build();

        return convertToRelNodes(alterTableBuilder.getPhysicalPlans());
    }

    protected String genReversedAlterTableStmt(SQLAlterTableStatement alterTableStmt) {
        List<SQLAlterTableItem> reversedAlterItems = new ArrayList<>();

        for (SQLAlterTableItem alterItem : alterTableStmt.getItems()) {
            List<SQLAlterTableItem> reversedItems = convertToReversedItem(alterItem);
            reversedAlterItems.addAll(reversedItems);
        }

        alterTableStmt.getItems().clear();
        alterTableStmt.getItems().addAll(reversedAlterItems);

        return alterTableStmt.toString();
    }

    private List<SQLAlterTableItem> convertToReversedItem(SQLAlterTableItem origAlterItem) {
        // One original alter item may be reversed to multiple items. For example,
        // ALTER TABLE XXX ADD COLUMN (ca INT, cb INT, cc INT)
        return AlterTableRollbacker.reverse(schemaName, logicalTableName, origAlterItem);
    }
}
