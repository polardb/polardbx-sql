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
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableReorgPartitionJobFactory;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableReorgPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableReorgPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableReorgPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

public class LogicalAlterTableReorgPartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableReorgPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableReorgPartition logicalAlterTableReorgPartition =
            (LogicalAlterTableReorgPartition) logicalDdlPlan;

        logicalAlterTableReorgPartition.prepareData(executionContext);
        logicalAlterTableReorgPartition.getPreparedData().setDdlVersionId(DdlUtils.generateVersionId(executionContext));

        return AlterTableReorgPartitionJobFactory.create(logicalAlterTableReorgPartition.relDdl,
            (AlterTableReorgPartitionPreparedData) logicalAlterTableReorgPartition.getPreparedData(), executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTable alterTable = (AlterTable) logicalDdlPlan.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        assert sqlAlterTable.getAlters().size() == 1;
        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableReorgPartition;

        SqlAlterTableReorgPartition sqlAlterTableReorgPartition =
            (SqlAlterTableReorgPartition) sqlAlterTable.getAlters().get(0);

        assert sqlAlterTableReorgPartition.getNames().size() >= 1;
        assert sqlAlterTableReorgPartition.getPartitions().size() >= 1;

        String schemaName = logicalDdlPlan.getSchemaName();
        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            throw new TddlNestableRuntimeException(
                "can't execute the reorganize partition command in a non-auto mode database");
        }

        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(
                partitionInfo.getTableGroupId());

        AlterTableGroupUtils.alterTableGroupReorgPartitionCheck(sqlAlterTableReorgPartition, tableGroupConfig,
            alterTable.getAllRexExprInfo(), false, executionContext);

        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
