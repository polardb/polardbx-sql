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
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropLocalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndex;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

@TaskName(name = "CreateIndexPhyDdlTask")
public class CreateIndexPhyDdlTask extends BasePhyDdlTask {

    @JSONCreator
    public CreateIndexPhyDdlTask(String schemaName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        super.executeImpl(executionContext);
    }

    @Override
    protected List<RelNode> genRollbackPhysicalPlans(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        ddlContext.setDdlType(DdlType.DROP_INDEX);

        final String indexName = physicalPlanData.getIndexName();
        final String tableName = physicalPlanData.getLogicalTableName();
        final String dropIndexSql = "DROP INDEX " + SqlIdentifier.surroundWithBacktick(indexName) + " ON " +
            SqlIdentifier.surroundWithBacktick(schemaName) + "." + SqlIdentifier.surroundWithBacktick(tableName);

        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        SqlIdentifier indexNameNode = new SqlIdentifier(indexName, SqlParserPos.ZERO);
        SqlIdentifier tableNameNode = new SqlIdentifier(Lists.newArrayList(schemaName, tableName), SqlParserPos.ZERO);

        SqlDropIndex sqlDropIndex =
            SqlDdlNodes.dropIndex(indexNameNode, tableNameNode, dropIndexSql, SqlParserPos.ZERO);
        sqlDropIndex = (SqlDropIndex) sqlDropIndex.accept(visitor);

        final RelOptCluster cluster =
            SqlConverter.getInstance(executionContext).createRelOptCluster(new PlannerContext(executionContext));
        DropIndex dropIndex = DropIndex.create(cluster, sqlDropIndex, tableNameNode);

        LogicalDropIndex logicalDropIndex = LogicalDropIndex.create(dropIndex);
        logicalDropIndex.prepareData();

        DdlPhyPlanBuilder dropIndexBuilder = DropLocalIndexBuilder
            .create(dropIndex, logicalDropIndex.getDropLocalIndexPreparedDataList().get(0), executionContext).build();

        return convertToRelNodes(dropIndexBuilder.getPhysicalPlans());
    }

}
