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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupAddTableJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddTable;

public class LogicalAlterTableGroupAddTableHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupAddTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupAddTable logicalAlterTableGroupAddTable =
            (LogicalAlterTableGroupAddTable) logicalDdlPlan;
        if (logicalAlterTableGroupAddTable.getPreparedData() == null) {
            logicalAlterTableGroupAddTable.preparedData(executionContext);
        }
        return AlterTableGroupAddTableJobFactory
            .create(logicalAlterTableGroupAddTable.relDdl, logicalAlterTableGroupAddTable.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(executionContext.getSchemaName());
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "only support this execute in auto mode database");
        }
        LogicalAlterTableGroupAddTable logicalAlterTableGroupAddTable =
            (LogicalAlterTableGroupAddTable) logicalDdlPlan;
        if (logicalAlterTableGroupAddTable.getPreparedData() == null) {
            logicalAlterTableGroupAddTable.preparedData(executionContext);
        }
        AlterTableGroupUtils.alterTableGroupAddTableCheck(logicalAlterTableGroupAddTable.getPreparedData(),
            executionContext);
        String tableGroup = logicalAlterTableGroupAddTable.getPreparedData().getTableGroupName();
        String schemaName = logicalAlterTableGroupAddTable.getPreparedData().getSchemaName();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroup);
        assert tableGroupConfig != null;
        final boolean onlyManualTableGroupAllowed =
            executionContext.getParamManager().getBoolean(ConnectionParams.ONLY_MANUAL_TABLEGROUP_ALLOW);
        if (!tableGroupConfig.isManuallyCreated() && onlyManualTableGroupAllowed) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_AUTO_CREATED,
                String.format(
                    "only the tablegroup create by user manually could by use explicitly, the table group[%s] is created internally",
                    tableGroup));
        }
        return false;
    }

}
