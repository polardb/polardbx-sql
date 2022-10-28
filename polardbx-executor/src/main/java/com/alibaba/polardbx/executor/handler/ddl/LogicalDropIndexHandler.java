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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.factory.DropIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.DropGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import org.apache.calcite.sql.SqlDropIndex;

import java.util.HashMap;
import java.util.Map;

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
        IndexValidator.validateDropLocalIndex(logicalDdlPlan.getSchemaName(), tableName, indexName);
        IndexValidator.validateDropPrimaryKey(indexName);
        TableValidator.validateTableEngine(logicalDdlPlan, executionContext);
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

    public DdlJob buildDropLocalIndexJob(LogicalDropIndex logicalDropIndex, ExecutionContext executionContext) {
        ExecutableDdlJob dropLocalIndexJob = DropIndexJobFactory.createDropLocalIndex(
            logicalDropIndex.relDdl, logicalDropIndex.getNativeSqlNode(),
            logicalDropIndex.getDropLocalIndexPreparedDataList(),
            false,
            executionContext);

        if (dropLocalIndexJob != null && GeneralUtil.isNotEmpty(logicalDropIndex.getDropLocalIndexPreparedDataList())) {
            DropLocalIndexPreparedData preparedData = logicalDropIndex.getDropLocalIndexPreparedDataList().get(0);
            Map<String, Long> tableVersions = new HashMap<>();
            tableVersions.put(preparedData.getTableName(), preparedData.getTableVersion());
            ValidateTableVersionTask validateTableVersionTask =
                new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions);

            dropLocalIndexJob.addTask(validateTableVersionTask);
            dropLocalIndexJob.addTaskRelationship(validateTableVersionTask, dropLocalIndexJob.getHead());
        }
        return dropLocalIndexJob;
    }

    public DdlJob buildDropGsiJob(LogicalDropIndex logicalDropIndex, ExecutionContext executionContext) {
        DropIndexWithGsiPreparedData dropIndexPreparedData = logicalDropIndex.getDropIndexWithGsiPreparedData();
        DropGlobalIndexPreparedData preparedData = dropIndexPreparedData.getGlobalIndexPreparedData();

        // gsi job
        ExecutableDdlJob gsiJob = DropGsiJobFactory.create(preparedData, executionContext, false, true);

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions);

        gsiJob.addTask(validateTableVersionTask);
        gsiJob.addTaskRelationship(validateTableVersionTask, gsiJob.getHead());

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
