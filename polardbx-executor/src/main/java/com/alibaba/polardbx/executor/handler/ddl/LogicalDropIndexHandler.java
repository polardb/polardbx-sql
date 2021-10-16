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

import com.alibaba.polardbx.executor.ddl.job.factory.DropIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.DropGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import org.apache.calcite.sql.SqlDropIndex;

/**
 * DROP INDEX xxx ON xxx
 *
 * @author jicheng, guxu, moyi
 * @since 2021/07
 */
public class LogicalDropIndexHandler extends LogicalCommonDdlHandler {

    public LogicalDropIndexHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalDropIndex logicalDropIndex = (LogicalDropIndex) logicalDdlPlan;
        logicalDropIndex.prepareData();

        if (logicalDropIndex.isGsi()) {
            return buildDropGsiJob(logicalDropIndex, executionContext);
        } else {
            return buildDropLocalIndexJob(logicalDropIndex, executionContext);
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlDropIndex sqlDropIndex = (SqlDropIndex) logicalDdlPlan.getNativeSqlNode();

        final String tableName = sqlDropIndex.getOriginTableName().getLastName();
        TableValidator.validateTableName(tableName);
        TableValidator.validateTableExistence(logicalDdlPlan.getSchemaName(), tableName, executionContext);

        final String indexName = sqlDropIndex.getIndexName().getLastName();
        IndexValidator.validateIndexExistence(logicalDdlPlan.getSchemaName(), tableName, indexName);

        return false;
    }

    public DdlJob buildDropLocalIndexJob(LogicalDropIndex logicalDropIndex, ExecutionContext executionContext) {
        return DropIndexJobFactory.createDropLocalIndex(
            logicalDropIndex.relDdl, logicalDropIndex.getNativeSqlNode(),
            logicalDropIndex.getDropLocalIndexPreparedDataList(),
            false,
            executionContext);
    }

    public DdlJob buildDropGsiJob(LogicalDropIndex logicalDropIndex, ExecutionContext executionContext) {
        DropIndexWithGsiPreparedData dropIndexPreparedData = logicalDropIndex.getDropIndexWithGsiPreparedData();
        DropGlobalIndexPreparedData preparedData = dropIndexPreparedData.getGlobalIndexPreparedData();

        // gsi job
        ExecutableDdlJob gsiJob = DropGsiJobFactory.create(preparedData, executionContext, false, true);

        // local index job
        ExecutableDdlJob localIndexJob = DropIndexJobFactory.createDropLocalIndex(
            logicalDropIndex.relDdl, logicalDropIndex.getNativeSqlNode(),
            dropIndexPreparedData.getLocalIndexPreparedDataList(),
            true,
            executionContext);
        if (localIndexJob != null) {
            gsiJob.appendJob(localIndexJob);
        }
        return gsiJob;
    }

}
