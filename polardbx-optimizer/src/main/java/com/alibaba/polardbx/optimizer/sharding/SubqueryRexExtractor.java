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

package com.alibaba.polardbx.optimizer.sharding;

import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryWrapperLabel;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Default condition extractor for subquery, create a new
 * {@link SubqueryRexExtractor} foreach RelNode contains subquery
 *
 * @author chenmo.cm
 */
public class SubqueryRexExtractor extends RexVisitorImpl<RexNode> {

    protected final RelNode relNode;
    protected final RexExtractorContext context;

    protected boolean subqueryWrapperLabelInitialized;

    protected SubqueryRexExtractor(RelNode relNode, RexExtractorContext context) {
        super(true);

        Preconditions.checkNotNull(context.getRexExtractor());
        Preconditions.checkNotNull(context.getRelToLabelConverter());

        this.relNode = relNode;
        this.context = context;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        return context.getRexExtractor().visitCall(call);
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        return context.getRexExtractor().visitLiteral(literal);
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
        if (context.isOperandOfOr()) {
            return null;
        }

        final SqlOperator operator = subQuery.getOperator();

        switch (operator.getKind()) {
        case SOME:
        case ALL:
        case NOT_IN:
        case IN:
        case NOT_EXISTS:
        case EXISTS:
            handleSubQuery(subQuery);
            break;
        default:
        }

        return null;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        if (dynamicParam.getIndex() == -3 && null != dynamicParam.getRel()) {
            // TODO
        }
        return null;
    }

    /**
     * Handle subquery which is operand of AND condition
     */
    private void handleSubQuery(RexSubQuery subQuery) {
        // Generate inferred correlate equality
        final Set<CorrelationId> newCorIds = new LinkedHashSet<>();
        final List<RexNode> inferred = new ArrayList<>();
        if (subQuery.getKind() == SqlKind.IN) {
            inferCorrelation(subQuery, newCorIds, inferred);
        }

        // Update SubqueryWrapperLabel
        final RelToLabelConverter relToLabelConverter = context.getRelToLabelConverter();
        if (!subqueryWrapperLabelInitialized) {
            subqueryWrapperLabelInitialized = (null != relToLabelConverter.relWithSubquery(relNode, newCorIds));
        } else if (!newCorIds.isEmpty()) {
            final Label root = relToLabelConverter.getRoot();
            if (root.getType().isSubqueryWrapper()) {
                ((SubqueryWrapperLabel) root).addInferredCorrelationId(newCorIds);
            } else {
                throw new UnsupportedOperationException("Do not support subquery in " + root.getType());
            }
        }

        // Convert RelNode in subquery
        subQuery.rel.accept(relToLabelConverter);

        // Store RexSubQuery
        relToLabelConverter.subQuery(subQuery, inferred);
    }

    /**
     * Infer correlation conditions for IN/NOT IN subquery
     */
    private void inferCorrelation(RexSubQuery subQuery, Set<CorrelationId> inferredCorIds,
                                  List<RexNode> inferredCorrelations) {
        final List<RexNode> left = subQuery.getOperands();
        final Set<CorrelationId> current = Optional.ofNullable(relNode.getVariablesSet()).orElseGet(ImmutableSet::of);
        final RelOptCluster cluster = relNode.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        if (left.size() == 1) {
            if (left.get(0) instanceof RexInputRef) {
                final RexInputRef leftCol = (RexInputRef) left.get(0);
                final CorrelationId corId = current.stream().findFirst().orElseGet(() -> {
                    final CorrelationId correl = cluster.createCorrel();
                    inferredCorIds.add(correl);
                    return correl;
                });

                final RexNode corrVar = rexBuilder.makeCorrel(relNode.getRowType(), corId);
                final RexNode fieldAccess = rexBuilder.makeFieldAccess(corrVar, leftCol.getIndex());

                inferredCorrelations.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    ImmutableList.of(fieldAccess, rexBuilder.makeInputRef(subQuery.rel, 0))));
            } else if (left.get(0) instanceof RexFieldAccess) {
                // TODO support multi level subquery
            }
        } else if (left.size() > 1) {
            // TODO support multi column in
        }
    }

    public boolean withCorrelateSubquery() {
        return subqueryWrapperLabelInitialized;
    }
}
