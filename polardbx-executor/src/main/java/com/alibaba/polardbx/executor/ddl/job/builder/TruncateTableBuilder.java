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
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalTruncateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.TruncateTablePreparedData;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.DropTable;
import org.apache.calcite.rel.ddl.TruncateTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class TruncateTableBuilder extends DdlPhyPlanBuilder {

    protected final TruncateTablePreparedData preparedData;

    public TruncateTableBuilder(DDL ddl, TruncateTablePreparedData preparedData, ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    public static TruncateTableBuilder createBuilder(String schemaName,
                                                     String logicalTableName,
                                                     boolean ifExists,
                                                     ExecutionContext executionContext) {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        SqlIdentifier logicalTableNameNode = new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);

        SqlTruncateTable sqlTruncateTable =
            SqlDdlNodes.truncateTable(SqlParserPos.ZERO, ifExists, logicalTableNameNode, true);
        sqlTruncateTable = (SqlTruncateTable) sqlTruncateTable.accept(visitor);

        final RelOptCluster cluster = SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        TruncateTable truncateTable = TruncateTable.create(cluster, sqlTruncateTable, logicalTableNameNode, false);

        LogicalTruncateTable logicalTruncateTable = LogicalTruncateTable.create(truncateTable);
        logicalTruncateTable.setSchemaName(schemaName);
        logicalTruncateTable.prepareData(executionContext);

        return new TruncateTableBuilder(truncateTable, logicalTruncateTable.getTruncateTableWithGsiPreparedData()
            .getPrimaryTablePreparedData(), executionContext);
    }

    @Override
    public void buildTableRuleAndTopology() {
        buildExistingTableRule(preparedData.getTableName());
        buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
    }

    @Override
    public void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

}
