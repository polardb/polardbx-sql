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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlRuntimeFilterBuildFunction;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;

import java.util.Optional;

/**
 * Used to check whether a rex node can be converted to vectorized expression
 * Currently rex can't be converted when it meets any of following rules:
 * 1. Contains subquery
 */
public class Rex2VectorizedExpressionChecker extends RexVisitorImpl<Boolean> {
    private static final Rex2VectorizedExpressionChecker SINGLETON = new Rex2VectorizedExpressionChecker();

    private Rex2VectorizedExpressionChecker() {
        super(true);
    }

    public static boolean check(RexNode rexNode) {
        return Optional.ofNullable(rexNode.accept(SINGLETON))
            .orElse(false);
    }

    /**
     * Contains subquery.
     */
    @Override
    public Boolean visitSubQuery(RexSubQuery subQuery) {
        return false;
    }

    /**
     * Contains correlated subquery.
     */
    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
        return false;
    }

    /**
     * Contains correlated subquery.
     */
    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
        return dynamicParam.getIndex() != -3 && dynamicParam.getIndex() != -2;
    }

    @Override
    public Boolean visitCall(RexCall call) {
        return !call.getType().isStruct() &&
            !(call.getOperator() instanceof SqlRuntimeFilterBuildFunction) &&
            !(call.getOperator() instanceof SqlRuntimeFilterFunction) &&
            call.getOperands()
                .stream()
                .map(r -> r.accept(this))
                .allMatch(p -> Optional.ofNullable(p).orElse(false));
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
        return !literal.getType().isStruct();
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
        return !inputRef.getType().isStruct();
    }
}
