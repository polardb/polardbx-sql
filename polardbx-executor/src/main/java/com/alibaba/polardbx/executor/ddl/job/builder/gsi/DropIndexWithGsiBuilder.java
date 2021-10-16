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
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropLocalIndexBuilder;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.PreparedDataUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class DropIndexWithGsiBuilder {

    private final DDL relDdl;
    private final DropIndexWithGsiPreparedData preparedData;
    private final ExecutionContext executionContext;

    private List<PhyDdlTableOperation> globalIndexPhysicalPlans;
    private List<List<PhyDdlTableOperation>> localIndexPhysicalPlansList = new ArrayList<>();

    public DropIndexWithGsiBuilder(DDL ddl, DropIndexWithGsiPreparedData preparedData,
                                   ExecutionContext executionContext) {
        this.relDdl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    public DropIndexWithGsiBuilder build() {
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
        DropGlobalIndexPreparedData globalIndexPreparedData = preparedData.getGlobalIndexPreparedData();
        if (globalIndexPreparedData != null) {
            DdlPhyPlanBuilder globalIndexBuilder =
                new DropGlobalIndexBuilder(relDdl, globalIndexPreparedData, executionContext).build();
            this.globalIndexPhysicalPlans = globalIndexBuilder.getPhysicalPlans();
        }
    }

    private void buildLocalIndexPhysicalPlans() {
        for (DropLocalIndexPreparedData localIndexPreparedData : preparedData.getLocalIndexPreparedDataList()) {
            buildLocalIndexPhysicalPlans(localIndexPreparedData);
        }

        if (preparedData.getGlobalIndexPreparedData() != null) {
            final String originTableName;
            final String indexName;

            SqlNode sqlNode = relDdl.getSqlNode();
            if (sqlNode instanceof SqlDropIndex) {
                originTableName = ((SqlDropIndex) sqlNode).getOriginTableName().getLastName();
                indexName = ((SqlDropIndex) sqlNode).getIndexName().getLastName();
            } else if (sqlNode instanceof SqlAlterTable) {
                originTableName = ((SqlAlterTable) sqlNode).getOriginTableName().getLastName();
                indexName = ((SqlAddIndex) ((SqlAlterTable) sqlNode).getAlters().get(0)).getIndexName().getLastName();
            } else {
                return;
            }

            final TableMeta primaryTableSchema =
                OptimizerContext.getContext(preparedData.getSchemaName()).getLatestSchemaManager()
                    .getTable(originTableName);

            if (primaryTableSchema.isAutoPartition()) {
                final String generatedLocalIndexNameString =
                    TddlConstants.AUTO_LOCAL_INDEX_PREFIX + TddlSqlToRelConverter.unwrapGsiName(indexName);
                final SqlIdentifier generatedLocalIndexName =
                    new SqlIdentifier(generatedLocalIndexNameString, SqlParserPos.ZERO);

                if (PreparedDataUtil
                    .indexExistence(preparedData.getSchemaName(), originTableName, generatedLocalIndexNameString)) {
                    // Rebuild primary on generated key.
                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
                        new ReplaceTableNameWithQuestionMarkVisitor(preparedData.getSchemaName(), executionContext);

                    final String dropIndexSql =
                        "DROP INDEX " + SqlIdentifier.surroundWithBacktick(generatedLocalIndexNameString) + " ON "
                            + SqlIdentifier.surroundWithBacktick(preparedData.getSchemaName()) + "."
                            + SqlIdentifier.surroundWithBacktick(originTableName);

                    SqlDropIndex sqlDropIndex = SqlDdlNodes.dropIndex(generatedLocalIndexName,
                        new SqlIdentifier(originTableName, SqlParserPos.ZERO), dropIndexSql, SqlParserPos.ZERO);

                    relDdl.sqlNode = sqlDropIndex.accept(visitor);

                    DropLocalIndexPreparedData localIndexPreparedData = new DropLocalIndexPreparedData();
                    localIndexPreparedData.setSchemaName(preparedData.getSchemaName());
                    localIndexPreparedData.setTableName(originTableName);

                    buildLocalIndexPhysicalPlans(localIndexPreparedData);

                    relDdl.sqlNode = sqlNode;
                }
            }
        }
    }

    private void buildLocalIndexPhysicalPlans(DropLocalIndexPreparedData localIndexPreparedData) {
        DdlPhyPlanBuilder localIndexBuilder =
            new DropLocalIndexBuilder(relDdl, localIndexPreparedData, executionContext).build();
        this.localIndexPhysicalPlansList.add(localIndexBuilder.getPhysicalPlans());
    }

}
