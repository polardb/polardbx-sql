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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Set;

public class TableTopologyUtil {

    public static boolean isBroadcast(TableMeta tableMeta) {
        return OptimizerContext.getContext(tableMeta.getSchemaName()).getRuleManager()
            .isBroadCast(tableMeta.getTableName());
    }

    public static boolean isSingle(TableMeta tableMeta) {
        return OptimizerContext.getContext(tableMeta.getSchemaName()).getRuleManager()
            .isTableInSingleDb(tableMeta.getTableName());
    }

    public static boolean isShard(TableMeta tableMeta) {
        return OptimizerContext.getContext(tableMeta.getSchemaName()).getRuleManager()
            .isShard(tableMeta.getTableName());
    }

    public static boolean isAllSingleTableInSamePhysicalDB(Set<RelOptTable> scans) {
        String currentSchema = null;
        int index = 0;
        for (RelOptTable scan : scans) {
            final List<String> qualifiedName = scan.getQualifiedName();
            final String schemaName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;

            if (index == 0) {
                currentSchema = schemaName;
            }
            if (currentSchema == null) {
                if (currentSchema != schemaName) {
                    return false;
                }
            } else if (!currentSchema.equalsIgnoreCase(schemaName)) {
                return false;
            }
            index++;
        }

        //currentSchema maybe null
        String schemaName = OptimizerContext.getContext(currentSchema).getSchemaName();

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        if (isNewPartDb) {
            //auto
            long tableGroupId = -1;
            for (RelOptTable scan : scans) {
                final List<String> qualifiedName = scan.getQualifiedName();
                final String tableName = Util.last(qualifiedName);
                if (!or.isTableInSingleDb(tableName)) {
                    return false;
                }

                PartitionInfo partitionInfo = or.getPartitionInfoManager().getPartitionInfo(tableName);
                if (partitionInfo == null) {
                    return false;
                }
                if (tableGroupId == -1) {
                    tableGroupId = partitionInfo.getTableGroupId();
                } else if (tableGroupId != partitionInfo.getTableGroupId()) {
                    return false;
                }
            }
        } else {
            //drds
            for (RelOptTable scan : scans) {
                final List<String> qualifiedName = scan.getQualifiedName();
                final String tableName = Util.last(qualifiedName);
                if (!or.isTableInSingleDb(tableName)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean supportPushTheSingleAutoTable(
        String leftTable, String rightTable, TddlRuleManager or) {
        PartitionInfo leftPartitionInfo = or.getPartitionInfoManager().getPartitionInfo(leftTable);
        PartitionInfo rightPartitionInfo = or.getPartitionInfoManager().getPartitionInfo(rightTable);
        if (leftPartitionInfo.getTableGroupId() != null && rightPartitionInfo.getTableGroupId() != null) {
            return leftPartitionInfo.isSingleTable() && rightPartitionInfo.isSingleTable() &&
                leftPartitionInfo.getTableGroupId().equals(rightPartitionInfo.getTableGroupId());

        }
        return false;
    }

    /**
     * push-down the single or broadcast table on drds mode.
     */
    public static boolean supportPushSingleOrBroadcastDrdsTable(
        String leftTable, String rightTable, TddlRuleManager or, JoinRelType joinType) {

        // 两个单表，可以下推
        if (or.isTableInSingleDb(leftTable) && or.isTableInSingleDb(rightTable)) {
            return true;
        }

        // 两个广播表
        if (or.isBroadCast(leftTable) && or.isBroadCast(rightTable)) {
            return true;
        }

        // 一个广播表，一个单表
        if (or.isTableInSingleDb(leftTable) && or.isBroadCast(rightTable)) {
            return true;
        }

        if (or.isBroadCast(leftTable) && or.isTableInSingleDb(rightTable)) {
            return true;
        }

        if (joinType != null) {
            /**
             * inner join, 任意一个是广播表就可以下推
             */
            if (joinType == JoinRelType.INNER) {
                return or.isBroadCast(leftTable) || (or.isBroadCast(rightTable));
            }

            /**
             * left join，右表是广播表
             */
            if (joinType == JoinRelType.LEFT) {
                return or.isBroadCast(rightTable);
            }

            /**
             * right join, 左表是广播表
             */
            if (joinType == JoinRelType.RIGHT) {
                return or.isBroadCast(leftTable);
            }

            /**
             * 其他
             */
            if ((joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI || joinType == JoinRelType.LEFT_SEMI)) {
                return or.isBroadCast(rightTable);
            }
        }

        return false;
    }
}