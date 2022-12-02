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

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupRenamePartitionJobFactory;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CheckOSSArchiveUtil;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupRenamePartition;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.sql.SqlAlterTableGroup;

public class LogicalAlterTableGroupRenamePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupRenamePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupRenamePartition logicalAlterTableGroupRenamePartition =
            (LogicalAlterTableGroupRenamePartition) logicalDdlPlan;
        logicalAlterTableGroupRenamePartition.prepareData(executionContext);
        CheckOSSArchiveUtil.checkWithoutOSS(logicalAlterTableGroupRenamePartition.getPreparedData());
        return AlterTableGroupRenamePartitionJobFactory.create(logicalAlterTableGroupRenamePartition.relDdl,
            logicalAlterTableGroupRenamePartition.getPreparedData(), executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) ((logicalDdlPlan).relDdl.getSqlNode()),
            logicalDdlPlan.getSchemaName(),
            executionContext);
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
