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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableDropPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.Map;

public class LogicalAlterTableDropPartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableDropPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        executionContext.getParamManager().getProps()
                .put(ConnectionProperties.ENABLE_PREEMPTIVE_MDL, Boolean.FALSE.toString());
        LogicalAlterTableDropPartition logicalAlterTableDropPartition =
            (LogicalAlterTableDropPartition) logicalDdlPlan;
        logicalAlterTableDropPartition.preparedData(executionContext);
        logicalAlterTableDropPartition.getPreparedData().setDdlVersionId(DdlUtils.generateVersionId(executionContext));
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

        String schemaName = logicalDdlPlan.getSchemaName();
        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the drop partition command in a non-auto mode database");
        }

        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

//        if (partitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH
//            || partitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                "it's not allow to drop partition for hash/key partition strategy tables");
//        }
//        if (partitionInfo.getPartitionBy().getPartitions().size() <= 1) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                "it's not allow to drop partition for tables only has one partition");
//        }
//        if (tableMeta.withGsi()) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                String.format("it's not support to drop partition when table[%s] with GSI", logicalTableName));
//        }
//        if (tableMeta.withGsi()) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                String.format("it's not support to drop partition when table[%s] with GSI", logicalTableName));
//        }
//        return false;

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(
                partitionInfo.getTableGroupId());

        AlterTableGroupUtils.alterTableGroupDropPartitionCheck(sqlAlterTableDropPartition, tableGroupConfig,
            executionContext, true, logicalTableName);

        // can't drop partition where referencing by other tables
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks();
        if (checkForeignKey) {
            final TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
            for (Map.Entry<String, ForeignKeyData> e : tableMeta.getReferencedForeignKeys().entrySet()) {
                String referencedSchemaName = e.getValue().schema;
                String referencedTableName = e.getValue().tableName;
                if (referencedTableName.equalsIgnoreCase(logicalTableName)) {
                    continue;
                }
                String constraint = tableMeta.getReferencedForeignKeys().get(e.getKey()).constraint;
                throw new TddlRuntimeException(ErrorCode.ERR_DROP_PARTITION_FK_CONSTRAINT, logicalTableName, constraint,
                    referencedSchemaName, referencedTableName);
            }
        }

        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
