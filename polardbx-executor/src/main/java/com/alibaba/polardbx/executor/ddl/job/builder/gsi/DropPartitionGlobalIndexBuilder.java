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

package com.alibaba.polardbx.executor.ddl.job.builder.gsi;

import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

public class DropPartitionGlobalIndexBuilder extends DropGlobalIndexBuilder {

    public DropPartitionGlobalIndexBuilder(DDL ddl,
                                           DropGlobalIndexPreparedData gsiPreparedData,
                                           ExecutionContext executionContext) {
        super(ddl, gsiPreparedData, executionContext);
    }

    //todo refactor me
    public static DropGlobalIndexBuilder createBuilder(final String schemaName,
                                                       final String primaryTableName,
                                                       final String indexName,
                                                       ExecutionContext executionContext) {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        final String dropIndexSql =
            "DROP INDEX " + SqlIdentifier.surroundWithBacktick(indexName)
                + " ON " + SqlIdentifier.surroundWithBacktick(schemaName) + "." + SqlIdentifier
                .surroundWithBacktick(primaryTableName);
        SqlDropIndex sqlDropIndex =
            SqlDdlNodes.dropIndex(
                new SqlIdentifier(indexName, SqlParserPos.ZERO),
                new SqlIdentifier(primaryTableName, SqlParserPos.ZERO),
                dropIndexSql,
                SqlParserPos.ZERO
            );
        sqlDropIndex = (SqlDropIndex) sqlDropIndex.accept(visitor);

        final RelOptCluster cluster = SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        DropIndex dropIndex =
            DropIndex.create(cluster, sqlDropIndex, new SqlIdentifier(primaryTableName, SqlParserPos.ZERO));

        DropGlobalIndexPreparedData preparedData =
            new DropGlobalIndexPreparedData(schemaName, primaryTableName, indexName, false);
        return new DropPartitionGlobalIndexBuilder(dropIndex, preparedData, executionContext);
    }

    @Override
    protected void buildTableRuleAndTopology() {
        dropTableBuilder =
            new DropPartitionTableBuilder(relDdl, gsiPreparedData.getIndexTablePreparedData(), executionContext);
        dropTableBuilder.build();

        this.partitionInfo = dropTableBuilder.getPartitionInfo();
        this.tableTopology = dropTableBuilder.getTableTopology();
    }

}
