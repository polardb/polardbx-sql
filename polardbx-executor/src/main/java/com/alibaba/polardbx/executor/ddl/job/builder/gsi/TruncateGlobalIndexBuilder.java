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
import com.alibaba.polardbx.executor.ddl.job.builder.TruncateTableBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateGlobalIndexPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class TruncateGlobalIndexBuilder extends DdlPhyPlanBuilder {

    private final TruncateGlobalIndexPreparedData gsiPreparedData;

    private TruncateTableBuilder truncateTableBuilder;

    public TruncateGlobalIndexBuilder(DDL ddl, TruncateGlobalIndexPreparedData gsiPreparedData,
                                      ExecutionContext executionContext) {
        super(ddl, gsiPreparedData.getIndexTablePreparedData(), executionContext);
        this.gsiPreparedData = gsiPreparedData;
    }

    @Override
    protected void buildTableRuleAndTopology() {
        truncateTableBuilder =
            new TruncateTableBuilder(relDdl, gsiPreparedData.getIndexTablePreparedData(), executionContext);
        truncateTableBuilder.buildTableRuleAndTopology();

        this.tableRule = truncateTableBuilder.getTableRule();
        this.tableTopology = truncateTableBuilder.getTableTopology();
    }

    @Override
    protected void buildPhysicalPlans() {
        truncateTableBuilder.buildPhysicalPlans();
        this.physicalPlans = truncateTableBuilder.getPhysicalPlans();
    }

    @Override
    protected void buildSqlTemplate() {
        final SqlIdentifier indexName = new SqlIdentifier(gsiPreparedData.getIndexTableName(), SqlParserPos.ZERO);
        final ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(gsiPreparedData.getSchemaName(), executionContext);

        SqlTruncateTable truncateTable = SqlDdlNodes.truncateTable(SqlParserPos.ZERO, false, indexName, false);
        truncateTable = (SqlTruncateTable) truncateTable.accept(visitor);

        this.sqlTemplate = truncateTable;
        this.originSqlTemplate = truncateTable;
    }

}
