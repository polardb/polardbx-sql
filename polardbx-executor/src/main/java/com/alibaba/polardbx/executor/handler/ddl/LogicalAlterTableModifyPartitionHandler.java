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
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableModifyPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupModifyPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableModifyPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.List;

public class LogicalAlterTableModifyPartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableModifyPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableModifyPartition logicalAlterTableModifyPartition =
            (LogicalAlterTableModifyPartition) logicalDdlPlan;
        logicalAlterTableModifyPartition.preparedData(executionContext);
        return AlterTableModifyPartitionJobFactory
            .create(logicalAlterTableModifyPartition.relDdl,
                (AlterTableModifyPartitionPreparedData) logicalAlterTableModifyPartition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableModifyPartition logicalAlterTableModifyPartition =
            (LogicalAlterTableModifyPartition) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) logicalAlterTableModifyPartition.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        String schemaName = logicalAlterTableModifyPartition.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the modify partition command in drds mode");
        }
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);

        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST_COLUMNS) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("it's not allow to add/drop values for table[%s], which partition strategy is [%s]",
                    logicalTableName,
                    partitionInfo.getPartitionBy().getStrategy().toString()));
        }
        for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpec()) {
            if (spec.getIsDefaultPartition()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("It's not allowed to modify partition when table contain default partition"));
            }
        }
        if (sqlAlterTableModifyPartitionValues.isDrop()) {
            String partName =
                ((SqlIdentifier) (sqlAlterTableModifyPartitionValues.getPartition().getName())).getLastName();
            PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> o.getName().equalsIgnoreCase(partName)).findFirst().orElse(null);
            if (partitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition[%s] is not exists", partName));
            }
            List<SearchDatumInfo> originalDatums = partitionSpec.getBoundSpec().getMultiDatums();

            if (originalDatums.size() == 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition[%s] has only one value now, can't drop value any more", partName));
            }
            if (originalDatums.size() <= sqlAlterTableModifyPartitionValues.getPartition().getValues().getItems()
                .size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format(
                        "the number of drop values should less than the number of values contain by partition[%s]",
                        partName));
            }
            if (tableMeta.withGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("it's not support to drop value when table[%s] with GSI", logicalTableName));
            }
        }
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
