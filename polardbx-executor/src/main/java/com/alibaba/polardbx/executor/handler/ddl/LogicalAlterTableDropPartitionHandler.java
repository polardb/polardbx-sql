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
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableDropPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.Set;
import java.util.stream.Collectors;

public class LogicalAlterTableDropPartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableDropPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableDropPartition logicalAlterTableDropPartition =
            (LogicalAlterTableDropPartition) logicalDdlPlan;
        logicalAlterTableDropPartition.preparedData(executionContext);
        return AlterTableDropPartitionJobFactory
            .create(logicalAlterTableDropPartition.relDdl,
                (AlterTableDropPartitionPreparedData) logicalAlterTableDropPartition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {

        AlterTable alterTable = (AlterTable) logicalDdlPlan.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableDropPartition;
        SqlAlterTableDropPartition sqlAlterTableDropPartition =
            (SqlAlterTableDropPartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        final SchemaManager schemaManager = executionContext.getSchemaManager();

        TableMeta tableMeta = schemaManager.getTable(logicalTableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        Set<String> partitionGroupNames =
            partitionInfo.getPartitionBy().getPartitions().stream().map(o -> o.getName().toLowerCase())
                .collect(Collectors.toSet());

        for (SqlNode sqlNode : sqlAlterTableDropPartition.getPartitionNames()) {
            if (!partitionGroupNames.contains(((SqlIdentifier) sqlNode).getLastName().toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "partition name:" + ((SqlIdentifier) sqlNode).getLastName() + " is not exists");
            }
        }

        if (partitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH
            || partitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to drop partition for hash/key partition strategy tables");
        }
        if (partitionInfo.getPartitionBy().getPartitions().size() <= 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to drop partition for tables only has one partition");
        }
        if (tableMeta.withGsi()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("it's not support to drop partition when table[%s] with GSI", logicalTableName));
        }
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
