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
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupRenamePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableSplitPartitionByHotValueJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CheckOSSArchiveUtil;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSplitPartitionByHotValue;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSplitPartitionByHotValuePreparedData;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.util.Util;

public class LogicalAlterTableSplitPartitionByHotValueHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableSplitPartitionByHotValueHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableSplitPartitionByHotValue logicalAlterTableSplitPartitionByHotValue =
            (LogicalAlterTableSplitPartitionByHotValue) logicalDdlPlan;
        logicalAlterTableSplitPartitionByHotValue.preparedData(executionContext);
        AlterTableSplitPartitionByHotValuePreparedData preparedData =
            (AlterTableSplitPartitionByHotValuePreparedData) logicalAlterTableSplitPartitionByHotValue.getPreparedData();
        CheckOSSArchiveUtil.checkWithoutOSS(preparedData);
        ExecutableDdlJob executableDdlJob = AlterTableSplitPartitionByHotValueJobFactory
            .create(logicalAlterTableSplitPartitionByHotValue.relDdl,
                preparedData,
                executionContext);
        if (executableDdlJob instanceof TransientDdlJob) {
            if (preparedData.hotPartitionNameNeedChange()) {
                AlterTableGroupRenamePartitionPreparedData renamePartitionPreparedData =
                    new AlterTableGroupRenamePartitionPreparedData();
                renamePartitionPreparedData.setSchemaName(preparedData.getSchemaName());
                renamePartitionPreparedData.setTableGroupName(preparedData.getTableGroupName());
                renamePartitionPreparedData.setChangePartitionsPair(preparedData.getChangeHotPartitionNames());
                executableDdlJob = AlterTableGroupRenamePartitionJobFactory
                    .create(logicalAlterTableSplitPartitionByHotValue.relDdl, renamePartitionPreparedData,
                        executionContext);
            }
        }
        return executableDdlJob;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableSplitPartitionByHotValue logicalAlterTableSplitPartitionByHotValue =
            (LogicalAlterTableSplitPartitionByHotValue) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) logicalAlterTableSplitPartitionByHotValue.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue =
            (SqlAlterTableSplitPartitionByHotValue) sqlAlterTable.getAlters().get(0);
        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        boolean isNewPart =
            DbInfoManager.getInstance().isNewPartitionDb(logicalAlterTableSplitPartitionByHotValue.getSchemaName());
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the split partition command in drds mode");
        }

        AlterTableGroupUtils.alterTableSplitPartitionByHotValueCheck(
            logicalAlterTableSplitPartitionByHotValue.getSchemaName(), logicalTableName,
            sqlAlterTableSplitPartitionByHotValue, alterTable.getAllRexExprInfo(), executionContext);
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
