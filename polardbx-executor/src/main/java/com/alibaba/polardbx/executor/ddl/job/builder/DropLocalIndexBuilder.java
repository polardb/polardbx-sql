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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.IndexBuilderHelper;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.PreparedDataUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

public class DropLocalIndexBuilder extends DdlPhyPlanBuilder {

    protected final DropLocalIndexPreparedData preparedData;

    /**
     * TODO(moyi) move implicit SqlGeneration into PrepareData
     * If this local index is implicit, the physical index name will be prefixed with`_local_`
     */
    private boolean implicitIndex = false;
    private String implicitIndexName;
    private SqlAlterTable sqlAlterTable;
    private SqlDropIndex sqlDropIndex;

    public DropLocalIndexBuilder(DDL ddl, DropLocalIndexPreparedData preparedData,
                                 ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    public static DropLocalIndexBuilder create(DDL ddl,
                                               DropLocalIndexPreparedData preparedData,
                                               ExecutionContext ec) {
        boolean isPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName());
        return isPartitionDb ?
            new DropPartitionLocalIndexBuilder(ddl, preparedData, ec) :
            new DropLocalIndexBuilder(ddl, preparedData, ec);
    }

    public DropLocalIndexBuilder withImplicitIndex(boolean implicit, SqlNode sqlNode) {
        this.implicitIndex = implicit;
        if (implicit) {
            if (sqlNode instanceof SqlDropIndex) {
                this.sqlDropIndex = (SqlDropIndex) sqlNode;
            } else if (sqlNode instanceof SqlAlterTable) {
                this.sqlAlterTable = (SqlAlterTable) sqlNode;
            } else {
                throw new UnsupportedOperationException("unknown sql node " + sqlNode);
            }
        }
        return this;
    }

    @Override
    protected void buildTableRuleAndTopology() {
        buildExistingTableRule(preparedData.getTableName());
        buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
    }

    @Override
    protected void buildPhysicalPlans() {
        buildImplicitIndex();
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

    private void buildImplicitIndex() {
        if (this.implicitIndex && this.sqlDropIndex != null) {
            this.implicitIndexName = IndexBuilderHelper.genImplicitLocalIndexName(this.sqlDropIndex);
            this.sqlDropIndex = IndexBuilderHelper.buildImplicitLocalIndexSql(this.sqlDropIndex);
        }
        if (this.implicitIndex && this.sqlAlterTable != null) {
            this.implicitIndexName = IndexBuilderHelper.genImplicitLocalIndexName(this.sqlAlterTable);
            this.sqlAlterTable = IndexBuilderHelper.buildImplicitLocalIndexSql(this.sqlAlterTable);
        }
    }

    @Override
    public PhysicalPlanData genPhysicalPlanData() {
        PhysicalPlanData result = super.genPhysicalPlanData();
        result.setIndexName(this.implicitIndex ? this.implicitIndexName : this.preparedData.getIndexName());
        return result;
    }

    @Override
    protected void buildSqlTemplate() {
        SqlDdl sqlDdl = (SqlDdl) relDdl.sqlNode;

        // replace SqlNode to build SQL template
        if (this.sqlAlterTable != null) {
            sqlDdl = this.sqlAlterTable;
        } else if (this.sqlDropIndex != null) {
            sqlDdl = this.sqlDropIndex;
        }

        final String indexName;
        if (sqlDdl instanceof SqlDropIndex) {
            indexName = ((SqlDropIndex) sqlDdl).getIndexName().getLastName();
        } else if (sqlDdl instanceof SqlAlterTable) {
            assert sqlDdl.getKind() == SqlKind.ALTER_TABLE;
            final SqlAlterTable alterTable = (SqlAlterTable) sqlDdl;
            if (alterTable.getAlters().size() > 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Do not support multi ALTER statements on table with clustered index");
            }
            assert alterTable.getAlters().get(0).getKind() == SqlKind.DROP_INDEX;
            indexName =
                ((SqlAlterTableDropIndex) alterTable.getAlters().get(0)).getIndexName().getLastName();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Unknown drop index stmt.");
        }

        final String indexTableName = preparedData.getTableName();

        SqlNode originalSqlNode = relDdl.sqlNode;

        if (preparedData.isOnClustered()) {
            if (!PreparedDataUtil.indexExistence(preparedData.getSchemaName(), indexTableName, indexName)) {
                // TODO(moyi) why ignore
//                return; // Ignore if not existence.
            }

            final ReplaceTableNameWithQuestionMarkVisitor visitor =
                new ReplaceTableNameWithQuestionMarkVisitor(preparedData.getSchemaName(), executionContext);

            final SqlIdentifier newTableName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);

            if (sqlDdl instanceof SqlDropIndex) {
                relDdl.sqlNode = ((SqlDropIndex) sqlDdl).replaceTableName(newTableName).accept(visitor);
            } else {
                relDdl.sqlNode = ((SqlAlterTable) sqlDdl).replaceTableName(newTableName).accept(visitor);
            }
        } else if (preparedData.isOnGsi()) {

            final ReplaceTableNameWithQuestionMarkVisitor visitor =
                new ReplaceTableNameWithQuestionMarkVisitor(preparedData.getSchemaName(), executionContext);
            if (sqlDdl instanceof SqlDropIndex) {
                relDdl.sqlNode = sqlDdl.accept(visitor);
            } else if (sqlDdl instanceof SqlAlterTable) {
                relDdl.sqlNode = sqlDdl.accept(visitor);
            } else {
                throw new UnsupportedOperationException("sql node " + sqlDdl.toString());
            }

        }

        super.buildSqlTemplate();

        relDdl.sqlNode = originalSqlNode;
    }

}
