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

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

/**
 * @author chenmo.cm
 */
public class ColumnEqualityRexExtractor extends RexExtractor {

    protected ColumnEqualityRexExtractor(RexExtractorContext context) {
        super(context);
    }

    @Override
    protected RexNode handleRexCall(RexBuilder builder, RexCall call) {
        switch (call.getKind()) {
        case EQUALS:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL:
            return handleComparison(builder, call);
        case NOT:
            // TODO: Handle NOT EXISTS/SOME/ALL correctly
            if (call.getOperands().get(0) instanceof RexSubQuery) {
                return visitSubQuery((RexSubQuery) call.getOperands().get(0));
            }
            break;
        case CAST:
            return call.getOperands().get(0).accept(this);
        case IN:
            if (RexUtil.isReferenceOrAccess(call.getOperands().get(0), true)) {
                return call;
            } else if (call.getOperands().get(1).getKind() == SqlKind.ROW) {
                RexCall row = (RexCall) call.getOperands().get(1);
                for (RexNode rowValue : row.getOperands()) {
                    if (RexUtil.isReferenceOrAccess(rowValue, true)) {
                        return call;
                    }
                }
            }
            break;
        default:
            break;
        }

        return null;
    }

    @Override
    protected RexNode handleComparison(RexBuilder builder, RexCall call) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        if (!(RexUtil.isReferenceOrAccess(left, true)) && !(RexUtil.isReferenceOrAccess(right, true))) {
            return null;
        } else {
            return call;
        }
    }
}
