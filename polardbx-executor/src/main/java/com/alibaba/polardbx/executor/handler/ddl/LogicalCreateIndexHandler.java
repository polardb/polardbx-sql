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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreatePartitionGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;

public class LogicalCreateIndexHandler extends LogicalCommonDdlHandler {

    public LogicalCreateIndexHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalCreateIndex logicalCreateIndex = (LogicalCreateIndex) logicalDdlPlan;

        if (logicalCreateIndex.needRewriteToGsi(false)) {
            logicalCreateIndex.needRewriteToGsi(true);
        }

        if (logicalCreateIndex.isClustered() || logicalCreateIndex.isGsi()) {
            return buildCreateGsiJob(logicalCreateIndex, executionContext);
        } else {
            return buildCreateLocalIndexJob(logicalCreateIndex, executionContext);
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlCreateIndex sqlCreateIndex = (SqlCreateIndex) logicalDdlPlan.getNativeSqlNode();

        final String tableName = sqlCreateIndex.getOriginTableName().getLastName();
        TableValidator.validateTableName(tableName);
        TableValidator.validateTableExistence(logicalDdlPlan.getSchemaName(), tableName, executionContext);

        final String indexName = sqlCreateIndex.getIndexName().getLastName();
        IndexValidator.validateIndexNameLength(indexName);
        IndexValidator.validateIndexNonExistence(logicalDdlPlan.getSchemaName(), tableName, indexName);

        return false;
    }

    private DdlJob buildCreateLocalIndexJob(LogicalCreateIndex logicalCreateIndex, ExecutionContext executionContext) {
        logicalCreateIndex.prepareData();

        return CreateIndexJobFactory.createLocalIndex(
            logicalCreateIndex.relDdl, logicalCreateIndex.getNativeSqlNode(),
            logicalCreateIndex.getCreateLocalIndexPreparedDataList(),
            executionContext);
    }

    private DdlJob buildCreateGsiJob(LogicalCreateIndex logicalCreateIndex, ExecutionContext executionContext) {
        initPrimaryTableDefinition(logicalCreateIndex, executionContext);

        // Should prepare data after initializing the primary table definition.
        logicalCreateIndex.prepareData();

        CreateIndexWithGsiPreparedData preparedData = logicalCreateIndex.getCreateIndexWithGsiPreparedData();
        CreateGlobalIndexPreparedData globalIndexPreparedData = preparedData.getGlobalIndexPreparedData();

        ExecutableDdlJob gsiJob = CreatePartitionGsiJobFactory.create(
            logicalCreateIndex.relDdl, globalIndexPreparedData, executionContext);

        ExecutableDdlJob localIndexJob = CreateIndexJobFactory.createLocalIndex(
            logicalCreateIndex.relDdl, logicalCreateIndex.getNativeSqlNode(),
            logicalCreateIndex.getCreateLocalIndexPreparedDataList(),
            executionContext);
        if (localIndexJob != null) {
            gsiJob.appendJob(localIndexJob);
        }
        return gsiJob;
    }

    /**
     * Get table definition from primary table and generate index table definition with it
     */
    private void initPrimaryTableDefinition(LogicalCreateIndex logicalDdlPlan, ExecutionContext executionContext) {
        SqlCreateIndex sqlCreateIndex = (SqlCreateIndex) logicalDdlPlan.getNativeSqlNode();
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);
        sqlCreateIndex.setPrimaryTableDefinition(primaryTableInfo.getKey());
        sqlCreateIndex.setPrimaryTableNode(primaryTableInfo.getValue());

        // TODO(moyi) these two AST is duplicated, choose one of them, but right row somehow both of them are used
        sqlCreateIndex = logicalDdlPlan.getSqlCreateIndex();
        sqlCreateIndex.setPrimaryTableDefinition(primaryTableInfo.getKey());
        sqlCreateIndex.setPrimaryTableNode(primaryTableInfo.getValue());
    }

}
