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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

public class RexHandlerFactory {
    public static final LogicalWriteUtil.ReplaceRexWithParamHandler<RexLiteral> REPLACE_REX_LITERAL_WITH_PARAM_HANDLER =
        new LogicalWriteUtil.ReplaceRexWithParamHandler<>(RexLiteral.class);

    public static final LogicalWriteUtil.ReplaceRexWithParamHandler<RexInputRef>
        REPLACE_REX_INPUT_REF_WITH_PARAM_HANDLER =
        new LogicalWriteUtil.ReplaceRexWithParamHandler<>(RexInputRef.class);

    public static final LogicalWriteUtil.ReplaceRexWithParamHandler<RexCall> REPLACE_REX_CALL_WITH_PARAM_HANDLER =
        new LogicalWriteUtil.ReplaceRexWithParamHandler<>(RexCall.class);

    public static final LogicalWriteUtil.ReplaceRexWithParamHandler<RexDynamicParam>
        REPLACE_REX_DYNAMIC_PARAM_WITH_PARAM_HANDLER =
        new LogicalWriteUtil.ReplaceRexWithParamHandler<>(RexDynamicParam.class);

    public static final List<RexHandler<LogicalWriteUtil.ReplaceRexWithParamHandlerCall>>
        REPLACE_REX_WITH_PARAM_ALL_HANDLERS =
        ImmutableList.of(
            REPLACE_REX_LITERAL_WITH_PARAM_HANDLER,
            REPLACE_REX_INPUT_REF_WITH_PARAM_HANDLER,
            REPLACE_REX_CALL_WITH_PARAM_HANDLER,
            REPLACE_REX_DYNAMIC_PARAM_WITH_PARAM_HANDLER
        );

    public static final LogicalWriteUtil.DynamicImplicitDefaultHandler<RexLiteral>
        DYNAMIC_IMPLICIT_DEFAULT_REX_LITERAL_HANDLER =
        new LogicalWriteUtil.DynamicImplicitDefaultHandler<>(RexLiteral.class);

    public static final LogicalWriteUtil.DynamicImplicitDefaultHandler<RexInputRef>
        DYNAMIC_IMPLICIT_DEFAULT_REX_INPUT_REF_HANDLER =
        new LogicalWriteUtil.DynamicImplicitDefaultHandler<>(RexInputRef.class);

    public static final LogicalWriteUtil.DynamicImplicitDefaultHandler<RexCall>
        DYNAMIC_IMPLICIT_DEFAULT_REX_CALL_HANDLER =
        new LogicalWriteUtil.DynamicImplicitDefaultHandler<>(RexCall.class);

    public static final LogicalWriteUtil.DynamicImplicitDefaultHandler<RexDynamicParam>
        DYNAMIC_IMPLICIT_DEFAULT_REX_DYNAMIC_PARAM_HANDLER =
        new LogicalWriteUtil.DynamicImplicitDefaultHandler<>(RexDynamicParam.class);

    public static final List<RexHandler<LogicalWriteUtil.DynamicImplicitDefaultHandlerCall>>
        DYNAMIC_IMPLICIT_DEFAULT_ALL_HANDLERS =
        ImmutableList.of(
            DYNAMIC_IMPLICIT_DEFAULT_REX_LITERAL_HANDLER,
            DYNAMIC_IMPLICIT_DEFAULT_REX_INPUT_REF_HANDLER,
            DYNAMIC_IMPLICIT_DEFAULT_REX_CALL_HANDLER,
            DYNAMIC_IMPLICIT_DEFAULT_REX_DYNAMIC_PARAM_HANDLER
        );
}
