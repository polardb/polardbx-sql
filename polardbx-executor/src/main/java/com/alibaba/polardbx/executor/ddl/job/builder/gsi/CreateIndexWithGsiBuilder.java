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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.job.builder.CreateLocalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class CreateIndexWithGsiBuilder {

    private final DDL relDdl;
    private final CreateIndexWithGsiPreparedData preparedData;
    private final ExecutionContext executionContext;

    private List<PhyDdlTableOperation> globalIndexPhysicalPlans;
    private List<List<PhyDdlTableOperation>> localIndexPhysicalPlansList = new ArrayList<>();

    public CreateIndexWithGsiBuilder(DDL ddl, CreateIndexWithGsiPreparedData preparedData,
                                     ExecutionContext executionContext) {
        this.relDdl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    public CreateIndexWithGsiBuilder build() {
        buildGlobalIndexPhysicalPlans();
        buildLocalIndexPhysicalPlans();
        return this;
    }

    public List<PhyDdlTableOperation> getGlobalIndexPhysicalPlans() {
        return globalIndexPhysicalPlans;
    }

    public List<List<PhyDdlTableOperation>> getLocalIndexPhysicalPlansList() {
        return localIndexPhysicalPlansList;
    }

    private void buildGlobalIndexPhysicalPlans() {
        CreateGlobalIndexPreparedData globalIndexPreparedData = preparedData.getGlobalIndexPreparedData();
        if (globalIndexPreparedData != null) {
            DdlPhyPlanBuilder globalIndexBuilder =
                new CreateGlobalIndexBuilder(relDdl, globalIndexPreparedData, executionContext).build();
            this.globalIndexPhysicalPlans = globalIndexBuilder.getPhysicalPlans();
        }
    }

    private void buildLocalIndexPhysicalPlans() {
        TableMeta tableMeta = preparedData.getGlobalIndexPreparedData().getIndexTablePreparedData().getTableMeta();

        SqlNode originalSqlNode = relDdl.sqlNode;

        if (tableMeta.isAutoPartition()) {
            final String indexTableName = preparedData.getGlobalIndexPreparedData().getIndexTableName();
            final String localIndexNameString =
                TddlConstants.AUTO_LOCAL_INDEX_PREFIX + TddlSqlToRelConverter.unwrapGsiName(indexTableName);
            final SqlIdentifier localIndexName = new SqlIdentifier(localIndexNameString, SqlParserPos.ZERO);

            final String origSql = ((SqlCreateIndex) originalSqlNode).getSourceSql();
            final List<SQLStatement> stmts = SQLUtils.parseStatements(origSql, JdbcConstants.MYSQL);
            final SQLCreateIndexStatement stmt = ((SQLCreateIndexStatement) stmts.get(0));
            stmt.setGlobal(false);
            stmt.setClustered(false);
            stmt.getIndexDefinition()
                .setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexNameString)));

            relDdl.sqlNode =
                ((SqlCreateIndex) originalSqlNode).rebuildToExplicitLocal(localIndexName, stmt.toString());

            for (CreateLocalIndexPreparedData localIndexPreparedData : preparedData.getLocalIndexPreparedDataList()) {
                buildLocalIndexPhysicalPlans(localIndexPreparedData);
            }

            relDdl.sqlNode = originalSqlNode;
        } else {

        }
    }

    private void buildLocalIndexPhysicalPlans(CreateLocalIndexPreparedData localIndexPreparedData) {
        DdlPhyPlanBuilder localIndexBuilder =
            new CreateLocalIndexBuilder(relDdl, localIndexPreparedData, executionContext).build();
        this.localIndexPhysicalPlansList.add(localIndexBuilder.getPhysicalPlans());
    }

}
