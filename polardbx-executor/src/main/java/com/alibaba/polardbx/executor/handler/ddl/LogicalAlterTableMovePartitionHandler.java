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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableMovePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

public class LogicalAlterTableMovePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableMovePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableMovePartition logicalAlterTableMovePartition = (LogicalAlterTableMovePartition) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) logicalAlterTableMovePartition.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        SqlAlterTableMovePartition sqlAlterTableMovePartition =
            (SqlAlterTableMovePartition) sqlAlterTable.getAlters().get(0);

        if (GeneralUtil.isEmpty(sqlAlterTableMovePartition.getTargetPartitions())) {
            return new TransientDdlJob();
        }

        logicalAlterTableMovePartition.preparedData(executionContext);

        return AlterTableMovePartitionJobFactory.create(alterTable,
            (AlterTableMovePartitionPreparedData) logicalAlterTableMovePartition.getPreparedData(), executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableMovePartition logicalAlterTableMovePartition = (LogicalAlterTableMovePartition) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) logicalAlterTableMovePartition.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        assert sqlAlterTable.getAlters().size() == 1;
        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableMovePartition;

        SqlAlterTableMovePartition sqlAlterTableMovePartition =
            (SqlAlterTableMovePartition) sqlAlterTable.getAlters().get(0);

        String schemaName = logicalAlterTableMovePartition.getSchemaName();
        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the move partition command in a non-auto mode database");
        }

        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);

        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig =
            tableGroupInfoManager.getTableGroupConfigById(tableMeta.getPartitionInfo().getTableGroupId());

//        AlterTableGroupUtils.alterTableGroupMovePartitionCheck(sqlAlterTableMovePartition, tableGroupConfig,
//            schemaName);
//
//        return super.validatePlan(logicalDdlPlan, executionContext);

//        AlterTableGroupUtils.alterTableGroupMovePartitionCheck(sqlAlterTableMovePartition,
//            tableGroupConfig,
//            executionContext);
//        return false;

        AlterTableGroupUtils.alterTableGroupMovePartitionCheck(sqlAlterTableMovePartition,
            tableGroupConfig,
            schemaName);
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
