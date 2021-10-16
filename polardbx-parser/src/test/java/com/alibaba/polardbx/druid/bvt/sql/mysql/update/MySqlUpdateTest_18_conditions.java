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
package com.alibaba.polardbx.druid.bvt.sql.mysql.update;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MySqlUpdateTest_18_conditions extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "update table set col1 = 'asdf', col2 = 'sss' where col3 = 'asdf' and col4 = 'asdf1' and col5 = 'asfff' and col6 = 'ssss';";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLUpdateStatement stmt = (SQLUpdateStatement) statementList.get(0);

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        SQLExpr where = stmt.getWhere();
        List<SQLExpr> conditions = SQLBinaryOpExpr.split(where, SQLBinaryOperator.BooleanAnd);
        for (SQLExpr condition : conditions) {
            if (condition instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr binaryOpCondition = (SQLBinaryOpExpr) condition;
                if (binaryOpCondition.getOperator() == SQLBinaryOperator.Equality
                        && binaryOpCondition.isNameAndLiteral()) {
                    SQLExpr left = binaryOpCondition.getLeft();
                    SQLExpr right = binaryOpCondition.getRight();

                    String name;
                    Object value;
                    if (left instanceof SQLName) {
                        name = left.toString();
                        value = ((SQLValuableExpr) right).getValue();
                    } else {
                        name = right.toString();
                        value = ((SQLValuableExpr) left).getValue();
                    }

                    values.put(name, value);
                }
            }
        }
        System.out.println(values);




    }


}
