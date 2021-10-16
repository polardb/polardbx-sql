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

package com.alibaba.polardbx.optimizer.core.rel.segmented.rule;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ParallelTableScan;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * abstract rule for adding condition filter
 *
 * @author hongxi.chx
 */
public abstract class TableScanSplitRule extends RelOptRule {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Creates a TableScanSplitRule.
     */
    public TableScanSplitRule(RelBuilderFactory relBuilderFactory, String description) {
        super(operand(TableScan.class, none()), relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableScan tableScan = (TableScan) call.rels[0];

        if (!(tableScan instanceof ParallelTableScan)) {
            return;
        }
        final ParallelTableScan parallelTableScan = (ParallelTableScan) tableScan;
        if (parallelTableScan.isParalleled()) {
            return;
        }
        final TddlRuleManager or = OptimizerContext.getContext(tableScan.getSchemaName()).getRuleManager();
        final List<String> qualifiedName = tableScan.getTable().getQualifiedName();
        if (or.isBroadCast(qualifiedName.get(qualifiedName.size() - 1))) {
            call.transformTo(tableScan);
            return;
        }
        final ParallelTableScan copy = ParallelTableScan.copy(tableScan, true);
        //1. multi-table -> 
        //2. collect all can be split tables
        //3. find the tables join key is （max row count/foreign key）s from all split tables.
        //4. add rule to split，or direct split it
        //5. transform
        //6. node that: if all split tables size is one, split it direct.
        RelNode filter = getSplitFilter(copy);
        call.transformTo(filter);
    }

    protected abstract RelNode getSplitFilter(TableScan tableScan);

    protected final RexNode getRexNode(TableScan tableScan, SqlBinaryOperator operator, int index) {
        final List<String> primaryKeys = getPrimaryKeys(tableScan);
        final RexNode rexNode = makeComparison(operator, primaryKeys.get(0), tableScan, index);
        return rexNode;
    }

    private RexNode makeComparison(SqlBinaryOperator operator, String columnName, RelNode relNode, int index) {
        final RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
        final RexNode expr = rexBuilder.makeRangeReference(relNode);
        final RexNode rexFieldNode = rexBuilder.makeFieldAccess(expr, columnName, false);
        final RexDynamicParam dynamicParam = makeDynimac(rexFieldNode, index);
        final RexNode rexNode = rexBuilder.makeCall(operator, rexFieldNode, dynamicParam);
        return rexNode;

    }

    protected RexNode makeAnd(RexBuilder rexBuilder, RexNode... rexNodes) {
        if (rexNodes == null || rexNodes.length == 0) {
            return null;
        }
        if (rexNodes.length == 1) {
            return rexNodes[0];
        }
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, rexNodes);
    }

    private RexDynamicParam makeDynimac(RexNode rexFieldNode, int index) {
        final RelDataType type = rexFieldNode.getType();
        return new RexDynamicParam(type, index, RexDynamicParam.DYNAMIC_TYPE_VALUE.SINGLE_PARALLEL);
    }

    private List<String> getPrimaryKeys(TableScan scan) {
        final String tableName = Util.last(scan.getTable().getQualifiedName());
        final TableMeta table =
            PlannerContext.getPlannerContext(scan).getExecutionContext().getSchemaManager(scan.getSchemaName())
                .getTable(tableName);
        final Collection<ColumnMeta> primaryKeys = table.getPrimaryKey();
        if (primaryKeys != null && primaryKeys.size() > 0) {
            final ColumnMeta next = primaryKeys.iterator().next();
            List<String> primaryKeyList = new ArrayList<>();
            final String originColumnName = next.getField().getOriginColumnName();
            primaryKeyList.add(originColumnName);
            return primaryKeyList;
        }
        return null;
    }

}
