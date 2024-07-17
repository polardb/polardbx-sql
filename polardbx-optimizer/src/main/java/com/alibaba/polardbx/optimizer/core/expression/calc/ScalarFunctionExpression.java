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

package com.alibaba.polardbx.optimizer.core.expression.calc;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;

import java.util.List;

/**
 * Created by chuanqin on 17/8/8.
 */
public class ScalarFunctionExpression extends AbstractExpression {

    private List<IExpression> args;
    private IScalarFunction function;
    private ExprContextProvider contextHolder;

    public ScalarFunctionExpression() {
    }

    public List<IExpression> getArgs() {
        return args;
    }

    @Override
    public Object eval(Row row, ExecutionContext ec) {

        if (function != null) {
            Object[] actualArgs = new Object[args.size()];
            for (int i = 0; i < args.size(); i++) {
                actualArgs[i] = args.get(i).eval(row, ec);
            }
            return function.compute(actualArgs, ec);
        } else {
            GeneralUtil.nestedException("invoke function of null");
        }
        return null;
    }

    @Override
    public Object eval(Row row) {
        return eval(row, contextHolder.getContext());
    }

    public static IExpression getScalarFunctionExp(List<IExpression> args, IScalarFunction function,
                                                   ExecutionContext executionContext) {
        ScalarFunctionExpression scalarFunctionExpression = new ScalarFunctionExpression();
        scalarFunctionExpression.args = args;
        scalarFunctionExpression.function = function;
        scalarFunctionExpression.contextHolder = new ExprContextProvider(executionContext);
        return scalarFunctionExpression;
    }

    public static IExpression getScalarFunctionExp(List<IExpression> args, IScalarFunction function,
                                                   ExprContextProvider contextHolder) {
        ScalarFunctionExpression scalarFunctionExpression = new ScalarFunctionExpression();
        scalarFunctionExpression.args = args;
        scalarFunctionExpression.function = function;
        scalarFunctionExpression.contextHolder = contextHolder;
        return scalarFunctionExpression;
    }

    public boolean isA(Class<? extends AbstractScalarFunction> funcType) {
        return function.getClass() == funcType;
    }

    /**
     * both two args are InputRefExpression
     */
    public boolean isInputRefArgs() {
        if (args != null && args.size() == 2) {
            if (args.get(0) instanceof InputRefExpression &&
                args.get(1) instanceof InputRefExpression) {
                return true;
            }
        }
        return false;
    }
}
