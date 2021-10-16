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

package com.alibaba.polardbx.druid.bvt.sql.expr;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * @author lijun.cailj 2019-07-03
 */
public class SQLExprUtilsTest {

    @Test
    public void test_fromJavaObj() {

        List<Object> params = new ArrayList<Object>();
        params.add(107);
        params.add(10.2F);
        params.add(200);
        params.add(new BigDecimal("100.34"));
        params.add(new java.sql.Date(1562160037416L));
        params.add(null);

        String insertSql = "INSERT INTO test.test8 VALUES (?,?,?,?,?,?)";

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(insertSql);
        SQLStatement clone = statement.clone();
        clone.accept(new VariantRefReplacer(params));

        Assert.assertEquals(clone.toString(), "INSERT INTO test.test8\n" +
                "VALUES (107, 10.2, 200, 100.34, '2019-07-03 21:20:37'\n" +
                "\t, NULL)");
    }


    @Test
    public void test_fromJavaObj2() {

        List<Object> params = new ArrayList<Object>();
        params.add(107);
        params.add(10.2F);
        params.add(200);
        params.add(200);
        params.add(200);
        params.add(new BigDecimal("100.34"));
        params.add(new java.sql.Date(1562160037416L));
        params.add(null);

        String insertSql = "UPDATE test.lmtest1_nopk SET a = ?,b = ?,c = ?,d = ? WHERE x = ? AND y = ? AND z = ? AND t = cast(? as decimal(28,3)) LIMIT 1";

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(insertSql);

        SQLStatement clone = statement.clone();

        clone.accept(new VariantRefReplacer(params));

        Assert.assertEquals(clone.toString(), "UPDATE test.lmtest1_nopk\n" +
                "SET a = 107, b = 10.2, c = 200, d = 200\n" +
                "WHERE x = 200\n" +
                "\tAND y = 100.34\n" +
                "\tAND z = '2019-07-03 21:20:37'\n" +
                "\tAND t = CAST(NULL AS decimal(28, 3))");

    }

    static class ADB3SQLExprUtils {

        public static SQLExpr fromJavaObjectForAdb(Object o) {
            return fromJavaObjectForAdb(o, null);
        }

        public static SQLExpr fromJavaObjectForAdb(Object o, TimeZone timeZone) {
            if (o == null) {
                return new SQLNullExpr();
            }

            if (o instanceof String) {
                return new SQLCharExpr((String) o);
            }

            if (o instanceof BigDecimal) {
                return new SQLNumberExpr((BigDecimal) o);
            }

            if (o instanceof Byte || o instanceof Short || o instanceof Integer || o instanceof Long || o instanceof BigInteger) {
                return new SQLIntegerExpr((Number) o);
            }

            if (o instanceof Number) {
                return new SQLNumberExpr((Number) o);
            }

            if (o instanceof Date) {
                SQLTimestampExpr timestampExpr = new SQLTimestampExpr((Date) o, timeZone);
                return new SQLCharExpr(timestampExpr.getLiteral());
            }

            throw new ParserException("not support class : " + o.getClass());
        }
    }


    public class VariantRefReplacer extends MySqlASTVisitorAdapter {
        private int index = 0;
        private List<Object> params;

        public VariantRefReplacer(List<Object> params) {
            this.params = params;
        }

        public boolean visit(SQLVariantRefExpr variantRefExpr) {
            Object param = params.get(index++);
            SQLExpr newExpr = ADB3SQLExprUtils.fromJavaObjectForAdb(param);
            SQLUtils.replaceInParent(variantRefExpr, newExpr);
            return false;
        }
    }
}
