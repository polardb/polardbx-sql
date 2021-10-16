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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;

/**
 * @author chenghui.lch
 */
public class ExprEvaluator {
    protected ExprContextProvider contextHolder;
    protected IExpression predCalcExpr;
    public ExprEvaluator(ExprContextProvider contextHolder, IExpression predCalcExpr) {
        this.contextHolder = contextHolder;
        this.predCalcExpr = predCalcExpr;
    }

    public Object eval(ExecutionContext context) {
        Object result = predCalcExpr.eval(null, context);
        return result;
    }
 }
