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
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.PhysicalDdlException;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
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
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

@Getter
@TaskName(name = "AlterTablePhyDdlTask")
public class AlterTablePhyDdlTask extends BasePhyDdlTask {

    private String logicalTableName;

    @JSONCreator
    public AlterTablePhyDdlTask(String schemaName, String logicalTableName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
        this.logicalTableName = logicalTableName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        try {
            super.executeImpl(executionContext);
        }catch (PhysicalDdlException e){
            int successCount = e.getSuccessCount();
            if (successCount == 0) {
                final DdlTask currentTask = this;
                DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
                    @Override
                    protected Integer invoke() {
                        DdlEngineRecord engineRecord = engineAccessor.query(jobId);
                        //if successCount==0, check supported_commands to decide exception policy
                        if (engineRecord.isSupportCancel()) {
                            //no physical DDL done
                            //so we can mark DdlTaskState from DIRTY to READY
                            currentTask.setState(DdlTaskState.READY);
                            onExceptionTryRollback();
                            DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(currentTask);
                            return engineTaskAccessor.updateTask(taskRecord);
                        }
                        return 0;
                    }
                };
                delegate.execute();
            } else {
                //some physical DDL failed && they do not support rollback
                //so we forbid CANCEL DDL command here
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
        String origSql = executionContext.getDdlContext().getDdlStmt();
        SQLAlterTableStatement alterTableStmt = (SQLAlterTableStatement) FastsqlUtils.parseSql(origSql).get(0);
        if (AlterTableRollbacker.checkIfRollbackable(alterTableStmt)) {
            return genReversedPhysicalPlans(alterTableStmt, executionContext);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The DDL job is not rollbackable because the DDL includes some operations that doesn't support rollback");
        }
    }

    private List<RelNode> genReversedPhysicalPlans(SQLAlterTableStatement alterTableStmt,
                                                   ExecutionContext executionContext) {
        String reversedSql = genReversedAlterTableStmt(alterTableStmt);

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

    private String genReversedAlterTableStmt(SQLAlterTableStatement alterTableStmt) {
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
