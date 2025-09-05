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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * drop local index on part of the physical tables
 */
public class DropPartLocalIndexBuilder extends DropLocalIndexBuilder {

    public static DropPartLocalIndexBuilder createBuilder(String schemaName,
                                                          String logicalTableName,
                                                          String indexName,
                                                          String sql,
                                                          TreeMap<String, List<List<String>>> tableTopology,
                                                          PartitionInfo partitionInfo,
                                                          ExecutionContext executionContext) {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        SqlIdentifier logicalTableNameNode = new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);
        SqlIdentifier indexNameNode = new SqlIdentifier(indexName, SqlParserPos.ZERO);

        SqlDropIndex sqlDropIndex = SqlDdlNodes.dropIndex(indexNameNode, logicalTableNameNode, sql, SqlParserPos.ZERO);
        sqlDropIndex = (SqlDropIndex) sqlDropIndex.accept(visitor);

        final RelOptCluster cluster = SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        DropIndex dropIndex = DropIndex.create(cluster, sqlDropIndex, logicalTableNameNode);

        LogicalDropIndex logicalDropIndex = LogicalDropIndex.create(dropIndex);
        logicalDropIndex.setSchemaName(schemaName);
        logicalDropIndex.prepareData();

        DropPartLocalIndexBuilder builder = new DropPartLocalIndexBuilder(dropIndex,
            logicalDropIndex.getDropLocalIndexPreparedDataList().get(0), executionContext);
        builder.setTableTopology(tableTopology);
        builder.setPartitionInfo(partitionInfo);
        return builder;
    }

    public DropPartLocalIndexBuilder(DDL ddl, DropLocalIndexPreparedData preparedData,
                                     ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public void buildTableRuleAndTopology() {

    }

}
