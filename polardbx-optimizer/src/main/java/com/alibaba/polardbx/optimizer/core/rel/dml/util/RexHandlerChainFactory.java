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

import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.DynamicImplicitDefaultHandlerCall;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.ReplaceRexWithParamHandlerCall;
import com.google.common.collect.ImmutableList;

public class RexHandlerChainFactory {
    public static RexHandlerChain<ReplaceRexWithParamHandlerCall> create(ReplaceRexWithParamHandlerCall call) {

        final ImmutableList.Builder<RexHandler<ReplaceRexWithParamHandlerCall>> builder =
            ImmutableList.builder();
        builder.add(RexHandlerFactory.REPLACE_REX_CALL_WITH_PARAM_HANDLER,
            RexHandlerFactory.REPLACE_REX_LITERAL_WITH_PARAM_HANDLER,
            RexHandlerFactory.REPLACE_REX_INPUT_REF_WITH_PARAM_HANDLER);

        final DynamicImplicitDefaultHandlerCall dynamicImplicitCall =
            call.unwrap(DynamicImplicitDefaultHandlerCall.class);
        if (null != dynamicImplicitCall
            && (dynamicImplicitCall.withImplicitDefault
            || dynamicImplicitCall.withDynamicImplicitDefault)) {
            builder.add(RexHandlerFactory.REPLACE_REX_DYNAMIC_PARAM_WITH_PARAM_HANDLER);
        }

        return RexHandlerChain.create(builder.build());
    }
}
