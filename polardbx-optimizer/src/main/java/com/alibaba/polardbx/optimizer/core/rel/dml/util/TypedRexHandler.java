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

import org.apache.calcite.rex.RexNode;

/**
 * Rex handler matches specified RexNode type
 */
public abstract class TypedRexHandler<T extends RexNode, C extends RexHandlerCall> implements RexHandler<C> {
    private final Class<T> type;

    public TypedRexHandler(Class<T> type) {
        this.type = type;
    }

    @Override
    public final boolean matches(C handlerCall) {
        return type.isInstance(handlerCall.getRex()) && matchesWithTypeChecked(handlerCall);
    }

    private boolean matchesWithTypeChecked(C call) {
        return call.buildRexFilterTree().test(call.getRex());
    }

    @Override
    public final RexHandlerChain.ChainState handle(C call) {
        call.transformTo(call.buildTransformer().transform());

        return RexHandlerChain.ChainState.STOP;
    }
}
