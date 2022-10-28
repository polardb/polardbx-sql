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
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableAddPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

public class LogicalAlterTableAddPartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableAddPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableAddPartition logicalAlterTableAddPatition =
            (LogicalAlterTableAddPartition) logicalDdlPlan;
        logicalAlterTableAddPatition.preparedData(executionContext);
        return AlterTableAddPartitionJobFactory
            .create(logicalAlterTableAddPatition.relDdl,
                (AlterTableAddPartitionPreparedData) logicalAlterTableAddPatition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {

        LogicalAlterTableAddPartition alterTableAddPartition =
            (LogicalAlterTableAddPartition) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) alterTableAddPartition.relDdl;

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        String schemaName = alterTableAddPartition.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the add partition command in drds mode");
        }

        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        TableMeta tableMeta = schemaManager.getTable(logicalTableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't add the partition group for single/broadcast tables");
        }
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
