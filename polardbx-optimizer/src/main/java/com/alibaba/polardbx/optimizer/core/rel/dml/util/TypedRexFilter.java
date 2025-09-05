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

import java.util.function.BiPredicate;

@RequiredArgsConstructor
public abstract class TypedRexFilter<T extends RexNode, C extends RexHandlerCall> implements
    RexFilter {
    protected final Class<T> type;
    protected final C handlerCall;

    @Override
    public boolean test(RexNode rex) {
        return type.isInstance(rex) && testWithTypeChecked(type.cast(rex));
    }

    abstract boolean testWithTypeChecked(T rex);

    @Override
    public RexFilter and(RexFilter... others) {
        return CombineRexFilter.combine(RexFilter.RexFilterOperator.AND, this, others);
    }

    @Override
    public RexFilter or(RexFilter... others) {
        return CombineRexFilter.combine(RexFilter.RexFilterOperator.OR, this, others);
    }

    @Override
    public RexFilter negative() {
        return CombineRexFilter.combine(RexFilter.RexFilterOperator.NEGATE, this);
    }

    public static <T extends RexNode, C extends RexHandlerCall> RexFilter of(C handlerCall,
                                                                             BiPredicate<RexNode, C> predicate) {
        return of(RexNode.class, handlerCall, predicate);
    }

    public static <T extends RexNode, C extends RexHandlerCall> RexFilter of(Class<T> rexType,
                                                                             C handlerCall,
                                                                             BiPredicate<T, C> predicate) {
        return new TypedRexFilter<T, C>(rexType, handlerCall) {
            @Override
            boolean testWithTypeChecked(T rex) {
                return predicate.test(rex, handlerCall);
            }
        };
    }
}
