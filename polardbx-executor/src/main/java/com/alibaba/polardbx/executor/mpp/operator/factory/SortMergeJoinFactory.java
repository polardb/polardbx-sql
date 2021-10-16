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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.SortMergeJoinExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SortMergeJoinFactory extends ExecutorFactory {
    private Join join;
    private List<Integer> leftColumns;
    private List<Integer> rightColumns;
    private RexNode otherCond;
    private List<RexNode> operands;
    private boolean maxOneRow;
    private List<Boolean> columnIsAscending;

    public SortMergeJoinFactory(Join join, List<Integer> leftColumns, List<Integer> rightColumns,
                                List<Boolean> columnIsAscending, RexNode otherCond,
                                List<RexNode> operands, boolean maxOneRow, ExecutorFactory inner, ExecutorFactory outer
    ) {
        this.join = join;
        this.leftColumns = leftColumns;
        this.rightColumns = rightColumns;
        this.otherCond = otherCond;
        this.operands = operands;
        this.maxOneRow = maxOneRow;
        this.columnIsAscending = columnIsAscending;
        addInput(inner);
        addInput(outer);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        //inner 和 outer 分区对齐
        Executor inner = getInputs().get(0).createExecutor(context, index);
        Executor outer = getInputs().get(1).createExecutor(context, index);
        IExpression otherCondition = convertExpression(otherCond, context);

        List<EquiJoinKey> joinKeys = EquiJoinUtils.buildEquiJoinKeys(join.getOuter(), join.getInner(),
            join.getJoinType().outerSide(leftColumns, rightColumns),
            join.getJoinType().innerSide(leftColumns, rightColumns)
        );
        List<IExpression> antiJoinOperands = null;
        if (operands != null && join.getJoinType() == JoinRelType.ANTI && !operands.isEmpty()) {
            antiJoinOperands =
                operands.stream().map(ele -> convertExpression(ele, context)).collect(Collectors.toList());
        }

        Executor ret =
            new SortMergeJoinExec(outer, inner, join.getJoinType(), maxOneRow, joinKeys, columnIsAscending,
                otherCondition,
                antiJoinOperands, context);
        ret.setId(join.getRelatedId());
        if (context.getRuntimeStatistics() != null) {
            RuntimeStatHelper.registerStatForExec(join, ret, context);
        }
        return ret;
    }

    private IExpression convertExpression(RexNode rexNode, ExecutionContext context) {
        return RexUtils.buildRexNode(rexNode, context, new ArrayList<>());
    }
}
