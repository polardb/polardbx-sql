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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;

/**
 * @author chenmo.cm
 */
public class LogicalShowBroadcastsHandler extends HandlerCommon {
    public LogicalShowBroadcastsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("BROADCASTS");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.initMeta();
        int index = 0;

        boolean isTableWithPrivileges = false;

        String schemaName = PlannerContext.getPlannerContext(logicalPlan).getSchemaName();

        Collection<TableRule> tables =
            OptimizerContext.getContext(schemaName).getRuleManager()
                .getTableRules();
        for (TableRule table : tables) {
            isTableWithPrivileges = CanAccessTable.verifyPrivileges(
                executionContext.getSchemaName(),
                table.getVirtualTbName(),
                executionContext);
            if (table.isBroadcast() && isTableWithPrivileges) {
                result.addRow(new Object[] {index++, table.getVirtualTbName()});
            }
        }

        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        for (PartitionInfo partitionInfo : partitionInfoManager.getPartitionInfos()) {
            if (partitionInfo.isBroadcastTable()) {
                isTableWithPrivileges = CanAccessTable.verifyPrivileges(
                    executionContext.getSchemaName(),
                    partitionInfo.getTableName(),
                    executionContext);
                if (isTableWithPrivileges) {
                    result.addRow(new Object[] {index++, partitionInfo.getTableName()});
                }
            }
        }

        return result;
    }
}
