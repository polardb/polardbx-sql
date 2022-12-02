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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author <a href="mailto:shitianshuo.sts@alibaba-inc.com"></a>
 */
public abstract class SetOpToSemiJoinRule extends RelOptRule {
    public static final SetOpToSemiJoinRule MINUS = new SetOpToSemiJoinRule(
        operand(LogicalMinus.class, any()), "SetOpToSemiJoinRule:Minus") {

        @Override
        public void onMatch(RelOptRuleCall call) {
            handleMinus(call);
        }
    };
    public static final SetOpToSemiJoinRule INTERSECT = new SetOpToSemiJoinRule(
        operand(LogicalIntersect.class, any()), "SetOpToSemiJoinRule:Intersect") {

        @Override
        public void onMatch(RelOptRuleCall call) {
            handleIntersect(call);
        }
    };

    public void handleMinus(RelOptRuleCall call) {
        final LogicalMinus minus = call.rel(0);
        Preconditions.checkState(minus.getInputs().size() == 2,
            String.format("MINUS RelNode receive %d input", minus.getInputs().size()));

        final RelOptCluster cluster = minus.getCluster();
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        final RelNode left = getLeft(minus.getInput(0), minus.getRowType(), relBuilder, rexBuilder);
        final RelNode right = minus.getInput(1);
        int columnNum = minus.getRowType().getFieldCount();
        ImmutableList<Integer> keys = ImmutableList.copyOf(IntStream.rangeClosed(0, columnNum - 1)
            .boxed().collect(Collectors.toList()));

        final RexNode condition =
            RelOptUtil.createNullSafeEquiJoinCondition(left,
                keys, right, keys,
                rexBuilder);
        relBuilder.push(left).push(right).logicalSemiJoin(ImmutableList.of(condition),
            JoinRelType.ANTI,
            minus.getHints(), true);
        call.transformTo(relBuilder.build());
    }

    public void handleIntersect(RelOptRuleCall call) {
        final LogicalIntersect intersect = call.rel(0);
        Preconditions.checkState(intersect.getInputs().size() == 2,
            String.format("INTERSECT RelNode receive %d input", intersect.getInputs().size()));

        final RelOptCluster cluster = intersect.getCluster();
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        RelNode left = intersect.getInput(0);
        RelNode right = intersect.getInput(1);

        final RelMetadataQuery mq = call.getMetadataQuery();
        if (left.estimateRowCount(mq) > right.estimateRowCount(mq)) {
            RelNode tmp = left;
            left = right;
            right = tmp;
        }

        left = getLeft(left, intersect.getRowType(), relBuilder, rexBuilder);
        int columnNum = intersect.getRowType().getFieldCount();
        ImmutableList<Integer> keys = ImmutableList.copyOf(IntStream.rangeClosed(0, columnNum - 1)
            .boxed().collect(Collectors.toList()));

        final RexNode condition =
            RelOptUtil.createNullSafeEquiJoinCondition(left,
                keys, right, keys,
                rexBuilder);
        relBuilder.push(left).push(right).logicalSemiJoin(ImmutableList.of(condition),
            JoinRelType.SEMI,
            intersect.getHints(), true);
        call.transformTo(relBuilder.build());
    }

    private static RelNode getLeft(RelNode left, RelDataType targetType, RelBuilder relBuilder, RexBuilder rexBuilder) {
        return relBuilder.push(left).distinct().convertWithCast(targetType, false).build();
    }

    SetOpToSemiJoinRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }
}
