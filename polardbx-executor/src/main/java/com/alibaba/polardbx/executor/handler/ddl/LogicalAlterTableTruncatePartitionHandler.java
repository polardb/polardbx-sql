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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableTruncatePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableTruncatePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

public class LogicalAlterTableTruncatePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableTruncatePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableTruncatePartition logicalAlterTableTruncatePartition =
            (LogicalAlterTableTruncatePartition) logicalDdlPlan;

        logicalAlterTableTruncatePartition.prepareData();

        AlterTableGroupTruncatePartitionPreparedData preparedData =
            logicalAlterTableTruncatePartition.getPreparedData();
        preparedData.setDdlVersionId(DdlUtils.generateVersionId(executionContext));

        return new AlterTableTruncatePartitionJobFactory(logicalAlterTableTruncatePartition.relDdl,
            preparedData, executionContext, preparedData.getDdlVersionId()).create();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTable alterTable = (AlterTable) logicalDdlPlan.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        assert sqlAlterTable.getAlters().size() == 1;
        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableTruncatePartition;

        SqlAlterTableTruncatePartition sqlAlterTableTruncatePartition =
            (SqlAlterTableTruncatePartition) sqlAlterTable.getAlters().get(0);

        assert sqlAlterTableTruncatePartition.getPartitionNames().size() >= 1;

        String schemaName = logicalDdlPlan.getSchemaName();
        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            throw new TddlNestableRuntimeException(
                "can't execute the truncate partition command in a non-auto mode database");
        }

        TableValidator.validateTruncatePartition(schemaName, logicalTableName, sqlAlterTable, executionContext);

        TableValidator.validateTableNotReferenceFk(schemaName, logicalTableName, executionContext);

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(
                partitionInfo.getTableGroupId());

        AlterTableGroupUtils.alterTableGroupTruncatePartitionCheck(sqlAlterTableTruncatePartition, tableGroupConfig,
            executionContext, true, logicalTableName);

        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
