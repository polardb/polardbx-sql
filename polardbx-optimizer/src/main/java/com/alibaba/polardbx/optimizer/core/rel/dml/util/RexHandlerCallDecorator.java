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

@RequiredArgsConstructor
public abstract class RexHandlerCallDecorator implements RexHandlerCall {
    public final RexHandlerCall rexHandlerCall;

    @Override
    public <T extends RexNode> T getRex() {
        return rexHandlerCall.getRex();
    }

    @Override
    public void transformTo(RexNode newRex) {
        rexHandlerCall.transformTo(newRex);
    }

    @Override
    public RexNode getResult() {
        return rexHandlerCall.getResult();
    }

    @Override
    public RexFilter buildRexFilterTree() {
        return rexHandlerCall.buildRexFilterTree().or(buildRexFilter());
    }

    @SuppressWarnings("unchecked")
    public final <R extends RexHandlerCall> R unwrap(Class<R> clazz) {
        if (null == clazz) {
            return null;
        }

        if (clazz.isInstance(rexHandlerCall)) {
            return (R) rexHandlerCall;
        } else if (rexHandlerCall instanceof RexHandlerCallDecorator) {
            return ((RexHandlerCallDecorator) rexHandlerCall).unwrap(clazz);
        }

        return null;
    }
}
