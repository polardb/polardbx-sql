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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

/**
 * This class finds equality in a rexNode
 *
 * @author shengyu
 */
public class ShardColumnFinder {
    private RelMetadataQuery mq;

    private RelNode rel;

    private ShardColumnEdges shardColumnEdges;

    private int count;

    private ParamManager paramManager;

    public ShardColumnFinder(ParamManager paramManager) {
        this.paramManager = paramManager;
    }

    public void init(RelMetadataQuery mq, RelNode rel,
                     ShardColumnEdges shardColumnEdges, int count) {
        this.mq = mq;
        this.rel = rel;
        this.shardColumnEdges = shardColumnEdges;
        this.count = count;
    }

    /**
     * check whether a join can be pushed down to dn
     *
     * @param leftColumn the left column
     * @param rightColumn the right column
     * @return true if the two table can be sharded or broadcast
     */
    private boolean checkPushable(RelColumnOrigin leftColumn, RelColumnOrigin rightColumn) {
        if (leftColumn == null || rightColumn == null) {
            return false;
        }
        TableMeta leftTableMeta = CBOUtil.getTableMeta(leftColumn.getOriginTable());

        TableMeta rightTableMeta = CBOUtil.getTableMeta(rightColumn.getOriginTable());

        // the same database
        if (!leftTableMeta.getSchemaName().equalsIgnoreCase(rightTableMeta.getSchemaName())) {
            return false;
        }

        boolean leftBroadCast = leftColumn.getOriginTable().getRowCount() <
            paramManager.getInt(ConnectionParams.SHARDING_ADVISOR_BROADCAST_THRESHOLD);
        boolean rightBroadCast = rightColumn.getOriginTable().getRowCount() <
            paramManager.getInt(ConnectionParams.SHARDING_ADVISOR_BROADCAST_THRESHOLD);

        if (!leftBroadCast && !AdvisorUtil.columnHashable(leftTableMeta, leftColumn.getOriginColumnOrdinal())) {
            return false;
        }
        if (!rightBroadCast && !AdvisorUtil.columnHashable(rightTableMeta, rightColumn.getOriginColumnOrdinal())) {
            return false;
        }
        if (!AdvisorUtil.notBacktick(leftTableMeta.getTableName()) ||
            !AdvisorUtil.notBacktick(rightTableMeta.getTableName())) {
            return false;
        }
        return true;
    }

    private RelColumnOrigin getColumnOrigin(RexInputRef op) {
        // pass through outer and semi join
        Set<RelColumnOrigin> columns = mq.getColumnOrigins(rel, op.getIndex());
        if (columns == null) {
            return null;
        }
        // make sure the reference only consist of one column
        RelColumnOrigin column = null;
        if (columns.size() == 1) {
            column = columns.stream().findFirst().get();
        }
        if (column == null) {
            return null;
        }
        // ignore gsi
        if (CBOUtil.getTableMeta(column.getOriginTable()).isGsi()) {
            return null;
        }
        return column;
    }

    /**
     * make sure the column chosen has high cardinality or no statistics
     *
     * @param columnOrigin the column to be chosen
     * @return true if the column can be chosen to be sharded on
     */
    private boolean isHighCardinality(RelColumnOrigin columnOrigin) {
        if (!AdvisorUtil.columnHashable(
            CBOUtil.getTableMeta(columnOrigin.getOriginTable()),
            columnOrigin.getOriginColumnOrdinal())) {
            return false;
        }

        String schemaName = CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getSchemaName();
        String tableName = CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getTableName();
        if (!AdvisorUtil.notBacktick(tableName)) {
            return false;
        }

        long cardinality = StatisticManager.getInstance()
            .getCardinality(schemaName, tableName, columnOrigin.getColumnName(), true).getLongValue();
        if (cardinality <= 0) {
            return true;
        }
        long rowCount = StatisticManager.getInstance().getRowCount(schemaName, tableName).getLongValue();
        return cardinality > rowCount * 0.000001;
    }

    public void findEqualCondition(List<RexNode> node) {
        for (RexNode operand : node) {
            findEqualCondition(operand);
        }
    }

    public void findEqualCondition(RexNode node) {
        List<RexNode> operands;
        switch (node.getKind()) {
        case AND:
            operands = ((RexCall) node).getOperands();
            for (RexNode operand : operands) {
                findEqualCondition(operand);
            }
            break;
        case EQUALS:
            operands = ((RexCall) node).getOperands();
            if (operands.size() != 2) {
                return;
            }
            // equal condition
            if (operands.get(0) instanceof RexInputRef
                && operands.get(1) instanceof RexInputRef) {
                final RexInputRef op0 = (RexInputRef) operands.get(0);
                final RexInputRef op1 = (RexInputRef) operands.get(1);

                RelColumnOrigin leftColumn = getColumnOrigin(op0);
                RelColumnOrigin rightColumn = getColumnOrigin(op1);
                if (leftColumn == null || rightColumn == null) {
                    return;
                }

                if (checkPushable(leftColumn, rightColumn)) {
                    shardColumnEdges.addShardColumnEqual(leftColumn, rightColumn);
                }

                if (isHighCardinality(leftColumn)) {
                    shardColumnEdges.addShardColumnFilter(leftColumn, count);
                }
                if (isHighCardinality(rightColumn)) {
                    shardColumnEdges.addShardColumnFilter(rightColumn, count);
                }
                return;
            }

            // filter
            if (operands.get(0) instanceof RexInputRef
                && (operands.get(1) instanceof RexLiteral || operands.get(1) instanceof RexDynamicParam)) {
                RelColumnOrigin column = getColumnOrigin((RexInputRef) operands.get(0));
                if (column == null || !isHighCardinality(column)) {
                    return;
                }
                shardColumnEdges.addShardColumnFilter(column, count);
                return;
            }
            if (operands.get(1) instanceof RexInputRef
                && (operands.get(0) instanceof RexLiteral || operands.get(0) instanceof RexDynamicParam)) {
                RelColumnOrigin column = getColumnOrigin((RexInputRef) operands.get(1));
                if (column == null || !isHighCardinality(column)) {
                    return;
                }
                shardColumnEdges.addShardColumnFilter(column, count);
            }
            break;
        default:
        }
    }
}

