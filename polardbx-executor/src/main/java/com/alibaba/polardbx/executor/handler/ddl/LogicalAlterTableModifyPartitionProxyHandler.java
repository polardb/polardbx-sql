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
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableModifyPartition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.util.Util;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalAlterTableModifyPartitionProxyHandler extends LogicalAlterTableModifyPartitionHandler {
    final public static String DEFAULT_ALGORITHM = "default";
    final public static String INSTANT_ALGORITHM = "instant";

    public LogicalAlterTableModifyPartitionProxyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableModifyPartition alterTableModifyPartition
            = (LogicalAlterTableModifyPartition) logicalDdlPlan;

        AlterTable alterTable = (AlterTable) logicalDdlPlan.relDdl;
        String objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo partitionInfo =
            executionContext.getSchemaManager(logicalDdlPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
        SqlAlterTable sqlNode =
            (SqlAlterTable) (alterTable.getSqlNode());
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) sqlNode.getAlters().get(0);
        boolean isSubPartition = sqlAlterTableModifyPartitionValues.isSubPartition();
        if (!isListStrategyAndContainDefaultPartition(partitionInfo, isSubPartition, sqlAlterTableModifyPartitionValues)
            || INSTANT_ALGORITHM.equalsIgnoreCase(sqlAlterTableModifyPartitionValues.getAlgorithm())) {
            if (sqlAlterTableModifyPartitionValues.isDrop() && INSTANT_ALGORITHM.equalsIgnoreCase(
                sqlAlterTableModifyPartitionValues.getAlgorithm())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "instant algorithm only support add operation");
            }
            return super.buildDdlJob(logicalDdlPlan, executionContext);
        } else {
            /**
             * partition p1 values in(1,2,3), partition pd values in(default)
             * 1. modify partition p1 drop values(1)
             *      convert to sql 'reorganize partition p1, pd into (partition p1 values in(2,3), partition pd values in (default))'
             * 2. modify partition p1 add values(4,5,6)
             *      convert to sql 'reorganize partition p1, pd into (partition p1 values in(1,2,3,4,5,6), partition pd values in (default))'
             *
             * */

            if (sqlAlterTableModifyPartitionValues.isAdd()) {
                String reorganizeSql;
                if (isSubPartition) {
                    reorganizeSql = AlterTableGroupUtils.convertModifyListPartitionValueRelToReorganizeForSubpartition(
                        alterTableModifyPartition,
                        true,
                        executionContext,
                        false
                    );
                } else {
                    reorganizeSql = AlterTableGroupUtils.convertModifyListPartitionValueRelToReorganizePartitionSql(
                        alterTableModifyPartition,
                        true,
                        executionContext,
                        false
                    );
                }
                return ActionUtils.convertToDelegatorJob(executionContext.getSchemaName(), reorganizeSql);
            } else if (sqlAlterTableModifyPartitionValues.isDrop()) {
                String reorganizeSql;
                if (isSubPartition) {
                    reorganizeSql = AlterTableGroupUtils.convertModifyListPartitionValueRelToReorganizeForSubpartition(
                        alterTableModifyPartition,
                        true,
                        executionContext,
                        true
                    );
                } else {
                    reorganizeSql = AlterTableGroupUtils.convertModifyListPartitionValueRelToReorganizePartitionSql(
                        alterTableModifyPartition,
                        true,
                        executionContext,
                        true
                    );
                }
                return ActionUtils.convertToDelegatorJob(executionContext.getSchemaName(), reorganizeSql);
            } else {
                //impossible
                return new TransientDdlJob();
            }
        }

    }

    protected boolean isListStrategyAndContainDefaultPartition(PartitionInfo partitionInfo, boolean isAlterSubPartition,
                                                               SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues) {
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
                //for template subpartition
                for (PartitionSpec spec : partitionInfo.getPartitionBy().getSubPartitionBy()
                    .getOrderedPartitionSpecs()) {
                    if (spec.isDefaultPartition()) {
                        hasDefaultInSubPartition = true;
                        break;
                    }
                }
            } else {
                //for non-template subpartition
                String subPartitionName =
                    ((SqlSubPartition) sqlAlterTableModifyPartitionValues.getPartition().getSubPartitions()
                        .get(0)).getName().toString();
                //find parent partition
                PartitionSpec parentSpec = null;
                for (PartitionSpec firstLvPartition : partitionInfo.getPartitionBy().getPartitions()) {
                    for (PartitionSpec subSpec : firstLvPartition.getSubPartitions()) {
                        if (subSpec.getName().equalsIgnoreCase(subPartitionName)) {
                            parentSpec = firstLvPartition;
                            break;
                        }
                    }
                }
                if (parentSpec == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "1st level partition not found");
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

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTable alterTable = (AlterTable) logicalDdlPlan.relDdl;
        SqlAlterTable sqlNode =
            (SqlAlterTable) (alterTable.getSqlNode());
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) sqlNode.getAlters().get(0);

        String algorithm = sqlAlterTableModifyPartitionValues.getAlgorithm();
        if (algorithm != null && !DEFAULT_ALGORITHM.equalsIgnoreCase(algorithm) && !INSTANT_ALGORITHM.equalsIgnoreCase(
            algorithm)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("unknown algorithm [%s]", algorithm));
        }

        return super.validatePlan(logicalDdlPlan, executionContext);
    }
}
