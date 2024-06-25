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
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddPartition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.util.Util;

public class LogicalAlterTableAddPartitionProxyHandler extends LogicalAlterTableAddPartitionHandler {
    public LogicalAlterTableAddPartitionProxyHandler(IRepository repo) {
        super(repo);
    }

    final public static String DEFAULT_ALGORITHM = "default";
    final public static String INSTANT_ALGORITHM = "instant";

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableAddPartition alterTableAddPartition =
            (LogicalAlterTableAddPartition) logicalDdlPlan;

        SqlAlterTable sqlNode = (SqlAlterTable) (logicalDdlPlan.relDdl.getSqlNode());
        String objectName = alterTableAddPartition.getTableName();
        PartitionInfo partitionInfo =
            executionContext.getSchemaManager(logicalDdlPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
        SqlAlterTableAddPartition sqlAlterTableAddPartition = (SqlAlterTableAddPartition) sqlNode.getAlters().get(0);
        boolean isAddSubPartition = sqlAlterTableAddPartition.isSubPartition();
        if (!isListStrategyAndContainDefaultPartition(partitionInfo, isAddSubPartition, sqlAlterTableAddPartition)
            || INSTANT_ALGORITHM.equalsIgnoreCase(sqlAlterTableAddPartition.getAlgorithm())) {
            return super.buildDdlJob(logicalDdlPlan, executionContext);
        } else {
            /**
             * 1. convert
             *      sql 'add partition (partition p2 values in(3,4), partition p3 values in(5,6))'
             *    to
             *      sql 'split partition default_part into (partition p2 values in(3,4), partition p3 values in(5,6), partition default_part values in(default))'
             * 2. use new sql to build split partition subJob
             *  */
            String splitSql;
            if (!isAddSubPartition) {
                splitSql = AlterTableGroupUtils.convertAddListRelToSplitListSql(alterTableAddPartition, true,
                    executionContext);
            } else {
                splitSql =
                    AlterTableGroupUtils.convertAddListRelToSplitListSqlForSubPartition(alterTableAddPartition, true,
                        executionContext);
            }
            return ActionUtils.convertToDelegatorJob(executionContext.getSchemaName(), splitSql);
        }
    }

    protected boolean isListStrategyAndContainDefaultPartition(PartitionInfo partitionInfo, boolean isAlterSubPartition,
                                                               SqlAlterTableAddPartition sqlAlterTableAddPartition) {
        if (!isAlterSubPartition) {
            PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
            boolean isList = (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS);
            boolean hasDefaultPartition = false;
            for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpecs()) {
                if (spec.isDefaultPartition()) {
                    hasDefaultPartition = true;
                    break;
                }
            }
            return isList && hasDefaultPartition;
        } else {
            //alter subpartition
            boolean isList = partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy() == PartitionStrategy.LIST
                || partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy() == PartitionStrategy.LIST_COLUMNS;
            boolean hasDefaultInSubPartition = false;
            if (partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate()) {
                //template subpartition
                for (PartitionSpec spec : partitionInfo.getPartitionBy().getSubPartitionBy()
                    .getOrderedPartitionSpecs()) {
                    if (spec.isDefaultPartition()) {
                        hasDefaultInSubPartition = true;
                        break;
                    }
                }
            } else {
                String partitionName =
                    ((SqlPartition) sqlAlterTableAddPartition.getPartitions().get(0)).getName().toString();
                PartitionSpec parentSpec = partitionInfo.getPartitionBy().getPartitionByPartName(partitionName);
                if (parentSpec == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition %s not found", partitionName));
                }
                for (PartitionSpec subSpec : parentSpec.getSubPartitions()) {
                    if (subSpec.isDefaultPartition()) {
                        hasDefaultInSubPartition = true;
                        break;
                    }
                }
            }

            return isList && hasDefaultInSubPartition;
        }
    }

}
