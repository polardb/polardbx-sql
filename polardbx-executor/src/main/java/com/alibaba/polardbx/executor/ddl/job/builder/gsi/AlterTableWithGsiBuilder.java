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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnique;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.job.builder.CreateLocalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * JobBuilder for alter table with gsi
 */
public class AlterTableWithGsiBuilder {

    private final DDL relDdl;
    private final AlterTableWithGsiPreparedData preparedData;
    private final ExecutionContext executionContext;

    private List<PhyDdlTableOperation> alterTablePhysicalPlans;
    private List<PhyDdlTableOperation> globalIndexPhysicalPlans;
    private List<List<PhyDdlTableOperation>> localIndexPhysicalPlansList = new ArrayList<>();

    public AlterTableWithGsiBuilder(DDL ddl, AlterTableWithGsiPreparedData preparedData,
                                    ExecutionContext executionContext) {
        this.relDdl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    public void build() {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) relDdl.sqlNode;
        if (sqlAlterTable.createGsi()) {
            buildCreateIndexWithGsiPhysicalPlans();
        } else {
            if (sqlAlterTable.dropIndex()) {
            } else if (sqlAlterTable.renameIndex()) {
            } else {
            }
        }

    }

    public List<PhyDdlTableOperation> getAlterTablePhysicalPlans() {
        return alterTablePhysicalPlans;
    }

    public List<PhyDdlTableOperation> getGlobalIndexPhysicalPlans() {
        return globalIndexPhysicalPlans;
    }

    public List<List<PhyDdlTableOperation>> getLocalIndexPhysicalPlansList() {
        return localIndexPhysicalPlansList;
    }

    private void buildCreateIndexWithGsiPhysicalPlans() {
        CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData =
            preparedData.getCreateIndexWithGsiPreparedData();

        CreateGlobalIndexPreparedData globalIndexPreparedData =
            createIndexWithGsiPreparedData.getGlobalIndexPreparedData();

        buildCreateGlobalIndexPhysicalPlans(globalIndexPreparedData);

        buildCreateLocalIndexPhysicalPlans(globalIndexPreparedData,
            createIndexWithGsiPreparedData.getLocalIndexPreparedDataList());
    }

    private void buildCreateGlobalIndexPhysicalPlans(CreateGlobalIndexPreparedData globalIndexPreparedData) {
        DdlPhyPlanBuilder globalIndexBuilder =
            new CreateGlobalIndexBuilder(relDdl, globalIndexPreparedData, executionContext).build();
        this.globalIndexPhysicalPlans = globalIndexBuilder.getPhysicalPlans();
    }

    private void buildCreateLocalIndexPhysicalPlans(CreateGlobalIndexPreparedData globalIndexPreparedData,
                                                    List<CreateLocalIndexPreparedData> localIndexPreparedDataList) {
        TableMeta tableMeta = globalIndexPreparedData.getIndexTablePreparedData().getTableMeta();

        SqlNode originalSqlNode = relDdl.sqlNode;

        if (tableMeta.isAutoPartition()) {
            final String indexTableName = globalIndexPreparedData.getIndexTableName();
            final String localIndexNameString =
                TddlConstants.AUTO_LOCAL_INDEX_PREFIX + TddlSqlToRelConverter.unwrapGsiName(indexTableName);
            final SqlIdentifier localIndexName = new SqlIdentifier(localIndexNameString, SqlParserPos.ZERO);

            final SqlAlterTable alterTable = ((SqlAlterTable) originalSqlNode);
            final String orgSql = alterTable.getSourceSql();
            final List<SQLStatement> stmts = SQLUtils.parseStatementsWithDefaultFeatures(orgSql, JdbcConstants.MYSQL);
            final SQLAlterTableStatement stmt = ((SQLAlterTableStatement) stmts.get(0));
            if (stmt.getItems().get(0) instanceof SQLAlterTableAddIndex) {
                ((SQLAlterTableAddIndex) stmt.getItems().get(0)).getIndexDefinition().setGlobal(false);
                ((SQLAlterTableAddIndex) stmt.getItems().get(0)).getIndexDefinition().setClustered(false);
                ((SQLAlterTableAddIndex) stmt.getItems().get(0)).getIndexDefinition()
                    .setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexNameString)));
            } else if (stmt.getItems().get(0) instanceof SQLAlterTableAddConstraint &&
                ((SQLAlterTableAddConstraint) stmt.getItems().get(0)).getConstraint() instanceof SQLUnique) {
                final SQLIndexDefinition indexDefinition =
                    ((SQLUnique) ((SQLAlterTableAddConstraint) stmt.getItems().get(0)).getConstraint())
                        .getIndexDefinition();
                indexDefinition.setGlobal(false);
                indexDefinition.setClustered(false);
                indexDefinition
                    .setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexNameString)));
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Unknown create GSI.");
            }

            final SqlAddIndex addIndex = ((SqlAddIndex) ((SqlAlterTable) originalSqlNode).getAlters().get(0));

            relDdl.sqlNode =
                new SqlAlterTable(null, alterTable.getOriginTableName(), alterTable.getColumnOpts(), stmt.toString(),
                    alterTable.getTableOptions(), ImmutableList.of(new SqlAddIndex(SqlParserPos.ZERO, localIndexName,
                    addIndex.getIndexDef().rebuildToExplicitLocal(localIndexName))), SqlParserPos.ZERO);
        }

        for (CreateLocalIndexPreparedData localIndexPreparedData : localIndexPreparedDataList) {
            buildCreateLocalIndexPhysicalPlans(localIndexPreparedData);
        }

        relDdl.sqlNode = originalSqlNode;
    }

    private void buildCreateLocalIndexPhysicalPlans(CreateLocalIndexPreparedData localIndexPreparedData) {
        DdlPhyPlanBuilder localIndexBuilder =
            new CreateLocalIndexBuilder(relDdl, localIndexPreparedData, executionContext).build();
        this.localIndexPhysicalPlansList.add(localIndexBuilder.getPhysicalPlans());
    }

}
