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

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupMovePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CheckOSSArchiveUtil;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMovePartition;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;

public class LogicalAlterTableGroupMovePartitionHandler extends LogicalAlterTableMovePartitionHandler {

    public LogicalAlterTableGroupMovePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupMovePartition logicalAlterTableGroupMovePartition =
            (LogicalAlterTableGroupMovePartition) logicalDdlPlan;

        AlterTableGroupMovePartition alterTableGroupMovePartition =
            (AlterTableGroupMovePartition) logicalAlterTableGroupMovePartition.relDdl;
        SqlAlterTableGroup sqlAlterTableGroup =
            (SqlAlterTableGroup) alterTableGroupMovePartition.getSqlNode();

        assert sqlAlterTableGroup.getAlters().size() == 1;
        SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition =
            (SqlAlterTableGroupMovePartition) sqlAlterTableGroup.getAlters().get(0);

        String schemaName = logicalAlterTableGroupMovePartition.getSchemaName();

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        String tableGroupName = alterTableGroupMovePartition.getTableGroupName();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);

        if (preProcessMovePartitionPlan(sqlAlterTableGroupMovePartition, tableGroupConfig)) {
            return new TransientDdlJob();
        }
        logicalAlterTableGroupMovePartition.preparedData(executionContext);
        CheckOSSArchiveUtil.checkWithoutOSS(logicalAlterTableGroupMovePartition.getPreparedData());
        return AlterTableGroupMovePartitionJobFactory
            .create(logicalAlterTableGroupMovePartition.relDdl, logicalAlterTableGroupMovePartition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) ((logicalDdlPlan).relDdl.getSqlNode()),
            logicalDdlPlan.getSchemaName(),
            executionContext);
        TableValidator.validateTableEngine(logicalDdlPlan, executionContext);
        return false;
    }

}
