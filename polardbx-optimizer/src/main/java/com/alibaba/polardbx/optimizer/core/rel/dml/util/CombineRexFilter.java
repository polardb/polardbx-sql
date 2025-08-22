/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.rel.dml.util;

import lombok.RequiredArgsConstructor;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RequiredArgsConstructor
public class CombineRexFilter implements RexFilter {
    private final RexFilterOperator operator;
    private final List<RexFilter> filters;

    public static CombineRexFilter create(RexFilterOperator operator,
                                          RexFilter... operands) {
        return new CombineRexFilter(operator, Arrays.asList(operands));
    }

    @Override
    public boolean test(RexNode rex) {
        switch (operator) {
        case AND:
            for (RexFilter filter : filters) {
                if (!filter.test(rex)) {
                    return false;
                }
            }
            return true;
        case OR:
            for (RexFilter filter : filters) {
                if (filter.test(rex)) {
                    return true;
                }
            }
            return false;
        case NEGATE:
            if (filters.size() != 1) {
                throw new IllegalArgumentException("Negate filter should have only one filter");
            }
            return !filters.get(0).test(rex);
        default:
            throw new IllegalArgumentException("Unknown operation: " + operator);
        }
    }

    @Override
    public RexFilter and(RexFilter... others) {
        return combine(RexFilter.RexFilterOperator.AND, this, others);
    }

    @Override
    public RexFilter or(RexFilter... others) {
        return combine(RexFilter.RexFilterOperator.OR, this, others);
    }

    @Override
    public RexFilter negative() {
        return combine(RexFilter.RexFilterOperator.NEGATE, this);
    }

    public static CombineRexFilter combine(RexFilterOperator op, RexFilter left,
                                           RexFilter... others) {
        final List<RexFilter> operands = new ArrayList<>(others.length + 1);
        operands.add(left);
        operands.addAll(Arrays.asList(others));
        return new CombineRexFilter(op, operands);
    }
}
