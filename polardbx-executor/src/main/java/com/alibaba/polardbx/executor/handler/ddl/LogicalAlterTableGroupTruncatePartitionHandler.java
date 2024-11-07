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

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupTruncatePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupTruncatePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import org.apache.calcite.sql.SqlAlterTableGroup;

public class LogicalAlterTableGroupTruncatePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupTruncatePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupTruncatePartition logicalAlterTableGroupTruncatePartition =
            (LogicalAlterTableGroupTruncatePartition) logicalDdlPlan;

        logicalAlterTableGroupTruncatePartition.prepareData(executionContext);

        AlterTableGroupTruncatePartitionPreparedData preparedData =
            logicalAlterTableGroupTruncatePartition.getPreparedData();

        String dbName = preparedData.getSchemaName();
        String tgName = preparedData.getTableGroupName();

        CheckOSSArchiveUtil.checkTableGroupWithoutOSS(dbName, tgName);

        return new AlterTableGroupTruncatePartitionJobFactory(logicalAlterTableGroupTruncatePartition.relDdl,
            preparedData, executionContext, preparedData.getDdlVersionId()).create();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) logicalDdlPlan.relDdl.getSqlNode(),
            logicalDdlPlan.getSchemaName(),
            executionContext);
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
