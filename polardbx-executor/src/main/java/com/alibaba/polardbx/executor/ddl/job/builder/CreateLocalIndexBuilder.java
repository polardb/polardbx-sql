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

import com.alibaba.polardbx.executor.ddl.job.builder.gsi.IndexBuilderHelper;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

public class CreateLocalIndexBuilder extends DdlPhyPlanBuilder {

    protected final CreateLocalIndexPreparedData preparedData;

    /**
     * SqlNode for implicit local index in auto-partition table
     */
    protected boolean implicit = false;
    protected String implicitIndexName;
    protected SqlCreateIndex sqlCreateIndex;
    protected SqlAlterTable sqlAlterTable;

    public CreateLocalIndexBuilder(DDL ddl,
                                   CreateLocalIndexPreparedData preparedData,
                                   ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    public static CreateLocalIndexBuilder create(DDL ddl,
                                                 CreateLocalIndexPreparedData preparedData,
                                                 ExecutionContext ec) {
        boolean isPartitioningDatabase = DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName());
        return isPartitioningDatabase ?
            new CreatePartitionTableLocalIndexBuilder(ddl, preparedData, ec) :
            new CreateLocalIndexBuilder(ddl, preparedData, ec);
    }

    public CreateLocalIndexBuilder withImplicitIndex(boolean implicit, SqlNode sqlNode) {
        this.implicit = implicit;
        if (implicit) {
            if (sqlNode instanceof SqlCreateIndex) {
                this.sqlCreateIndex = (SqlCreateIndex) sqlNode;
            } else if (sqlNode instanceof SqlAlterTable) {
                this.sqlAlterTable = (SqlAlterTable) sqlNode;
            } else {
                throw new UnsupportedOperationException("unknown sql node " + sqlNode);
            }
        }
        return this;
    }

    @Override
    public PhysicalPlanData genPhysicalPlanData() {
        PhysicalPlanData result = super.genPhysicalPlanData();
        result.setIndexName(this.implicit ? this.implicitIndexName : this.preparedData.getIndexName());
        return result;
    }

    @Override
    public void buildTableRuleAndTopology() {
        buildExistingTableRule(preparedData.getTableName());
        buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
    }

    @Override
    public void buildPhysicalPlans() {
        buildImplicitIndex();
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

    private void buildImplicitIndex() {
        if (this.implicit && this.sqlCreateIndex != null) {
            this.sqlCreateIndex = IndexBuilderHelper.buildImplicitLocalIndexSql(this.sqlCreateIndex);
            this.implicitIndexName = this.sqlCreateIndex.getIndexName().getLastName();
        }
        if (this.implicit && this.sqlAlterTable != null) {
            this.implicitIndexName = IndexBuilderHelper.genImplicitLocalIndexName(this.sqlAlterTable);
            this.sqlAlterTable = IndexBuilderHelper.buildImplicitLocalIndexSql(this.sqlAlterTable);
        }
    }

    @Override
    protected void buildSqlTemplate() {
        // backup sql node
        SqlNode originalSqlNode = relDdl.sqlNode;

        if (preparedData.isOnClustered() || sqlCreateIndex != null || sqlAlterTable != null) {
            final ReplaceTableNameWithQuestionMarkVisitor visitor =
                new ReplaceTableNameWithQuestionMarkVisitor(preparedData.getSchemaName(), executionContext);

            final SqlIdentifier newTableName = new SqlIdentifier(preparedData.getTableName(), SqlParserPos.ZERO);

            if (this.sqlCreateIndex != null) {
                relDdl.sqlNode = sqlCreateIndex.replaceTableName(newTableName).accept(visitor);
            } else if (this.sqlAlterTable != null) {
                relDdl.sqlNode = sqlAlterTable.replaceTableName(newTableName).accept(visitor);
            }

//            SqlDdl sqlDdl = this.sqlCreateIndex;
//            if (sqlDdl instanceof SqlCreateIndex) {
//                relDdl.sqlNode = ((SqlCreateIndex) sqlDdl).replaceTableName(newTableName).accept(visitor);
//            } else {
//                relDdl.sqlNode = ((SqlAlterTable) sqlDdl).replaceTableName(newTableName).accept(visitor);
//            }
        }

        super.buildSqlTemplate();

        // restore sql node
        relDdl.sqlNode = originalSqlNode;
    }

}
