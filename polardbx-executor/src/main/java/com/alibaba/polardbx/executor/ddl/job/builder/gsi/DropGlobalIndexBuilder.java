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

import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

public class DropGlobalIndexBuilder extends DdlPhyPlanBuilder {

    protected final DropGlobalIndexPreparedData gsiPreparedData;

    protected DropTableBuilder dropTableBuilder;

    public DropGlobalIndexBuilder(DDL ddl,
                                  DropGlobalIndexPreparedData gsiPreparedData,
                                  ExecutionContext executionContext) {
        super(ddl, gsiPreparedData.getIndexTablePreparedData(), executionContext);
        this.gsiPreparedData = gsiPreparedData;
    }

    public static DropGlobalIndexBuilder create(DDL ddl,
                                                DropGlobalIndexPreparedData preparedData,
                                                ExecutionContext ec) {
        final String schema = preparedData.getSchemaName();
        return DbInfoManager.getInstance().isNewPartitionDb(schema) ?
            new DropPartitionGlobalIndexBuilder(ddl, preparedData, ec) :
            new DropGlobalIndexBuilder(ddl, preparedData, ec);
    }

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
        return new DropGlobalIndexBuilder(dropIndex, preparedData, executionContext);
    }

    @Override
    protected void buildTableRuleAndTopology() {
        dropTableBuilder = new DropTableBuilder(relDdl, gsiPreparedData.getIndexTablePreparedData(), executionContext);
        dropTableBuilder.buildTableRuleAndTopology();

        this.tableRule = dropTableBuilder.getTableRule();
        this.tableTopology = dropTableBuilder.getTableTopology();
    }

    @Override
    protected void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(gsiPreparedData.getIndexTableName());
    }

    @Override
    protected void buildSqlTemplate() {
        final SqlIdentifier indexName = new SqlIdentifier(gsiPreparedData.getIndexTableName(), SqlParserPos.ZERO);
        final ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(gsiPreparedData.getSchemaName(), executionContext);

        SqlDropTable dropTable = SqlDdlNodes.dropTable(SqlParserPos.ZERO, true, indexName, true);
        dropTable = (SqlDropTable) dropTable.accept(visitor);

        this.sqlTemplate = dropTable;
        this.originSqlTemplate = dropTable;
    }

}
