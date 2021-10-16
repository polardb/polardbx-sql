/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.util.List;

public class SQLTransformUtils {
    public static SQLExpr transformDecode(SQLMethodInvokeExpr x) {
        if (x == null) {
            return null;
        }

        if (FnvHash.Constants.DECODE != x.methodNameHashCode64()) {
            throw new IllegalArgumentException(x.getMethodName());
        }

        List<SQLExpr> arguments = x.getArguments();
        SQLCaseExpr caseExpr = new SQLCaseExpr();
        caseExpr.setParent(x.getParent());
        caseExpr.setValueExpr(arguments.get(0));

        if (arguments.size() == 4) {
            SQLExpr param1 = arguments.get(1);

            x.setMethodName("if");

            SQLBinaryOpExpr condition;
            if (param1 instanceof SQLNullExpr) {
                condition = new SQLBinaryOpExpr(arguments.get(0), SQLBinaryOperator.Is, param1);
            } else {
                condition = new SQLBinaryOpExpr(arguments.get(0), SQLBinaryOperator.Equality, param1);
            }
            condition.setParent(x);
            arguments.set(0, condition);
            arguments.set(1, arguments.get(2));
            arguments.set(2, arguments.get(3));
            arguments.remove(3);
            return x;
        }

        for (int i = 1; i + 1 < arguments.size(); i += 2) {
            SQLCaseExpr.Item item = new SQLCaseExpr.Item();
            SQLExpr conditionExpr = arguments.get(i);

            item.setConditionExpr(conditionExpr);

            SQLExpr valueExpr = arguments.get(i + 1);

            if (valueExpr instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) valueExpr;
                if (FnvHash.Constants.DECODE == methodInvokeExpr.methodNameHashCode64()) {
                    valueExpr = transformDecode(methodInvokeExpr);
                }
            }

            item.setValueExpr(valueExpr);
            caseExpr.addItem(item);
        }

        if (arguments.size() % 2 == 0) {
            SQLExpr defaultExpr = arguments.get(arguments.size() - 1);

            if (defaultExpr instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) defaultExpr;
                if (FnvHash.Constants.DECODE == methodInvokeExpr.methodNameHashCode64()) {
                    defaultExpr = transformDecode(methodInvokeExpr);
                }
            }

            caseExpr.setElseExpr(defaultExpr);
        }

        caseExpr.setParent(x.getParent());

        return caseExpr;
    }

}
