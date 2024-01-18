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
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableMergePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LogicalAlterTableMergePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableMergePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableMergePartition alterTableMergePartition =
            (LogicalAlterTableMergePartition) logicalDdlPlan;
        alterTableMergePartition.preparedData(executionContext);
        return AlterTableMergePartitionJobFactory
            .create(alterTableMergePartition.relDdl,
                (AlterTableMergePartitionPreparedData) alterTableMergePartition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableMergePartition alterTableMergePartition =
            (LogicalAlterTableMergePartition) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) alterTableMergePartition.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        SqlAlterTableMergePartition sqlAlterTableGroupMergePartition =
            (SqlAlterTableMergePartition) sqlAlterTable.getAlters().get(0);

        String targetPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableGroupMergePartition.getTargetPartitionName())).names);
        Set<String> partitionsToBeMerged = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        partitionsToBeMerged.addAll(sqlAlterTableGroupMergePartition.getOldPartitions().stream()
            .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                Collectors.toSet()));

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        String schemaName = alterTableMergePartition.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the merge partition command in drds mode");
        }
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig =
            tableGroupInfoManager.getTableGroupConfigById(tableMeta.getPartitionInfo().getTableGroupId());

        AlterTableGroupUtils.alterTableGroupMergePartitionCheck(sqlAlterTableGroupMergePartition, tableGroupConfig,
            targetPartitionName,
            partitionsToBeMerged, executionContext);
        return false;
    }

}
