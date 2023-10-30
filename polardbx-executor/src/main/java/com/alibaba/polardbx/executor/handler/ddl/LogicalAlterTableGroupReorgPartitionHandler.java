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

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupReorgPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupReorgPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;

public class LogicalAlterTableGroupReorgPartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupReorgPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupReorgPartition logicalAlterTableGroupReorgPartition =
            (LogicalAlterTableGroupReorgPartition) logicalDdlPlan;

        logicalAlterTableGroupReorgPartition.prepareData(executionContext);

        String dbName = logicalAlterTableGroupReorgPartition.getPreparedData().getSchemaName();
        String tgName = logicalAlterTableGroupReorgPartition.getPreparedData().getTableGroupName();
        CheckOSSArchiveUtil.checkTableGroupWithoutOSS(dbName, tgName);

        return AlterTableGroupReorgPartitionJobFactory.create(logicalAlterTableGroupReorgPartition.relDdl,
            logicalAlterTableGroupReorgPartition.getPreparedData(), executionContext);
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
