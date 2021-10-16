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

package com.alibaba.polardbx.optimizer.sharding.label;

import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * @author fangwu
 */
public class CorrelateLabel extends AbstractLabel {

    private SemiJoinType type;
    private CorrelationId correlationId;
    private PredicateNode inferredCorrelateCondition;
    private ExtractorContext context;

    protected CorrelateLabel(@Nonnull LogicalCorrelate rel, Label left, Label right, ExtractorContext context) {
        super(LabelType.CORRELATE, rel, ImmutableList.of(left, right));
        this.type = rel.getJoinType();
        correlationId = rel.getCorrelationId();
        List<RexNode> leftRexNode = rel.getLeftConditions();
        inferredCorrelateCondition = new PredicateNode(this, null, inferCorrelation(leftRexNode), context);
        this.context = context;
    }

    public static CorrelateLabel create(@Nonnull LogicalCorrelate rel, Label left, Label right,
                                        ExtractorContext context) {
        final CorrelateLabel correlateLabel = new CorrelateLabel(rel, left, right, context);
        return correlateLabel;
    }

    public Label left() {
        return getInput(0);
    }

    public Label right() {
        return getInput(1);
    }

    @Override
    public RelDataType deriveRowType() {
        return this.getRel().getRowType();
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new CorrelateLabel(
            getRel(), inputs.get(0), inputs.get(1), context);
    }

    @Override
    public Label accept(LabelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public String toString() {
        return super.toString() + " # " + left().toString() + " # " + right().toString();
    }

    /**
     * Infer correlation conditions for IN/NOT IN subquery
     */
    private List<RexNode> inferCorrelation(List<RexNode> left) {
        List<RexNode> inferredCorrelations = Lists.newArrayList();
        final RelOptCluster cluster = getRel().getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        if (left.size() == 1) {
            if (left.get(0) instanceof RexInputRef) {
                final RexInputRef leftCol = (RexInputRef) left.get(0);

                final RexNode corrVar = rexBuilder.makeCorrel(left().getRowType(), correlationId);
                final RexNode fieldAccess = rexBuilder.makeFieldAccess(corrVar, leftCol.getIndex());

                inferredCorrelations.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    ImmutableList.of(fieldAccess, rexBuilder.makeInputRef(right().getRowType(), 0))));
            } else if (left.get(0) instanceof RexFieldAccess) {
                // TODO support multi level subquery
            }
        } else if (left.size() > 1) {
            // TODO support multi column in
        }
        return inferredCorrelations;
    }

    /**
     * for IN/NOT IN subquery
     */
    public PredicateNode getInferredCorrelateCondition() {
        return inferredCorrelateCondition;
    }
}
