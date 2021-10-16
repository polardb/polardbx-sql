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

import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import java.util.LinkedList;
import java.util.List;

/**
 * Default condition extractor, extract all kinds of RexNode
 *
 * @author chenmo.cm
 */
public class RexExtractor extends RexVisitorImpl<RexNode> {

    protected final RexExtractorContext context;

    protected RexExtractor(RexExtractorContext context) {
        super(true);
        this.context = context;
    }

    @Override
    public RexNode visitCall(RexCall call) {

        switch (call.getKind()) {
        case EQUALS:
        case GREATER_THAN:
        case LESS_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL:
        case NOT_EQUALS:
            return handleComparison(context.getBuilder(), call);
        case AND:
            return RexUtil.composeConjunction(context.getBuilder(), visitAndOrParam(call, false), true);
        case OR:
            try {
                context.getOperandOfOr().push(true);
                List<RexNode> subOr = visitAndOrParam(call, true);
                if (null == subOr) {
                    // for OR expression, return NULL if non-RexCall child exists;
                    return null;
                }
                // Deduplicate by digest
                return RexUtil.composeDisjunction(context.getBuilder(), subOr, true);
            } finally {
                context.getOperandOfOr().pop();
            }

        default:
            break;
        } // end of switch

        return handleRexCall(context.getBuilder(), call);
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        return literal;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
        return context.getSubqueryExtractor().visitSubQuery(subQuery);
    }

    private List<RexNode> visitAndOrParam(RexCall call, boolean isOr) {
        List<RexNode> comparisons = new LinkedList<>();
        for (RexNode operand : call.getOperands()) {
            RexNode r = operand.accept(this);
            if (null != r) {
                if ((isOr && r.isAlwaysTrue()) || (!isOr && r.isAlwaysFalse())) {
                    // Return boolean literal for OR TRUE or AND FALSE
                    return ImmutableList.of(r);
                }
                if ((isOr && r.isAlwaysFalse()) || (!isOr && r.isAlwaysTrue())) {
                    // Ignore OR FALSE or AND TRUE
                    continue;
                }
                comparisons.add(r);
            } else if (isOr) {
                return null;
            }
        }

        return comparisons;
    }

    protected RexNode handleComparison(RexBuilder builder, RexCall call) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        if (left.getKind() == SqlKind.ROW && right.getKind() == SqlKind.ROW) {
            return handleRowComparison(builder, call);
        }

        if (RexUtil.isReferenceOrAccess(left, true) || RexUtil.isReferenceOrAccess(right, true)) {
            return call;
        }

        if (RexUtils.allLiteral(call.getOperands())) {
            final RexSimplify simplify = new RexSimplify(builder, RelOptPredicateList.EMPTY, true, RexUtil.EXECUTOR);
            return simplify.simplify(call);
        }

        // Skip predicates like ? = ? , because it might invalidate whole expression
        return null;
    }

    protected RexNode handleRowComparison(RexBuilder builder, RexCall call) {
        RexCall leftRow = (RexCall) call.getOperands().get(0);
        RexCall rightRow = (RexCall) call.getOperands().get(1);

        if (leftRow.getOperands().size() != rightRow.getOperands().size()) {
            return null;
        }
        int opCnt = leftRow.getOperands().size();

        boolean allInputInLeft = true;
        for (int i = 0; i < opCnt; i++) {
            RexNode node = leftRow.getOperands().get(i);
            if (!RexUtil.isReferenceOrAccess(node, true)) {
                allInputInLeft = false;
                break;
            }
        }
        if (allInputInLeft) {
            boolean allConstInRight = true;
            for (int i = 0; i < opCnt; i++) {
                RexNode node = rightRow.getOperands().get(i);
                if (!RexUtil.isConstant(node)) {
                    allConstInRight = false;
                    break;
                }
            }
            if (!allConstInRight) {
                return null;
            } else {
                return call;
            }
        }

        boolean allInputInRight = true;
        for (int i = 0; i < opCnt; i++) {
            RexNode node = rightRow.getOperands().get(i);
            if (!RexUtil.isReferenceOrAccess(node, true)) {
                allInputInRight = false;
                break;
            }
        }
        if (allInputInRight) {
            boolean allConstInLeft = true;
            for (int i = 0; i < opCnt; i++) {
                RexNode node = leftRow.getOperands().get(i);
                if (!RexUtil.isConstant(node)) {
                    allConstInLeft = false;
                    break;
                }
            }
            if (!allConstInLeft) {
                return null;
            } else {
                return call;
            }
        }

        return null;

    }

    protected RexNode handleRexCall(RexBuilder builder, RexCall call) {
        return call;
    }

    public boolean isOperandOfOr() {
        return context.isOperandOfOr();
    }
}
