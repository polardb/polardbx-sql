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

package com.alibaba.polardbx.druid.bvt.sql.builder;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.FnvHash;
import junit.framework.TestCase;

import java.util.Arrays;

public class DLAConvert extends TestCase {
    public void test_0() throws Exception {
        String sql = "SHOW FULL TABLES \n" +
                "FROM `mengdoutest1` \n" +
                "\tLIKE 'world%'";

        final SQLShowTablesStatement stmt = (SQLShowTablesStatement) SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.IgnoreNameQuotes).get(0);

        final SQLName database = stmt.getFrom(); // from

        SQLSelectQueryBlock queryBlock = new SQLSelectQueryBlock();

        queryBlock.setFrom("information_schema.oa_table", null);

        queryBlock.addSelectItem("Table_Name", null);
        queryBlock.addSelectItem("Table_Types", null);

        queryBlock.addWhere(
                SQLBinaryOpExpr.conditionEq("schema_id", "93602f31-8885-454d-be06-a5b94351249c"));
        queryBlock.addWhere(
                new SQLInListExpr("table_id", "29f72848-a130-42be-8f6b-3bdf4b6ec637", "463040de-cc9e-474e-86cb-a7b9b862f114"));
        final SQLExpr like = stmt.getLike();
        if (like != null) {
            queryBlock.addWhere(
                    SQLBinaryOpExpr.conditionLike("table_name", like.clone()));
        }

        assertEquals("SELECT Table_Name, Table_Types\n" +
                "FROM information_schema.oa_table\n" +
                "WHERE schema_id = '93602f31-8885-454d-be06-a5b94351249c'\n" +
                "\tAND table_id IN ('29f72848-a130-42be-8f6b-3bdf4b6ec637', '463040de-cc9e-474e-86cb-a7b9b862f114')\n" +
                "\tAND table_name LIKE 'world%'", queryBlock.toString());
    }

    public void test_1() {
        SQLSelectStatement stmt = (SQLSelectStatement) com.alibaba.polardbx.druid.sql.SQLUtils.parseSingleStatement(
                "SELECT * from `customer` where 1=0", DbType.mysql, SQLParserFeature.IgnoreNameQuotes);
        assertEquals(
                "SELECT *\n" +
                        "FROM customer\n" +
                        "WHERE 1 = 0",
                stmt.toString()
        );

        MySqlASTVisitorAdapter v = new MySqlASTVisitorAdapter() {
            public boolean visit(SQLExprTableSource x) {
                return true;
            }
        };
        stmt.accept(v);
    }

    final static long SCHEMA_HASHCODE = FnvHash.hashCode64("SCHEMA");
    public void test_2() {
        String sql = "SELECT t.table_schema, t.table_type, s.schema_name \n" +
                "FROM tables t INNER JOIN schema s \n" +
                " ON t.table_schema = s.schema_name \n" +
                "WHERE s.schema_name = 'information_schema'";

        SQLSelectStatement stmt = (SQLSelectStatement) com.alibaba.polardbx.druid.sql.SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.IgnoreNameQuotes);


        MySqlASTVisitorAdapter v = new MySqlASTVisitorAdapter() {
            public boolean visit(SQLExprTableSource x) {
                final SQLExpr expr = x.getExpr();
                if (expr instanceof SQLIdentifierExpr
                        && ((SQLIdentifierExpr) expr).nameHashCode64() == SCHEMA_HASHCODE) {
                    SQLSelectQueryBlock queryBlock = new SQLSelectQueryBlock();
                    queryBlock.addSelectItem("schema_name", null);
                    queryBlock.setFrom("oa_schema", "s");
                    queryBlock.addWhere(
                            SQLBinaryOpExpr.conditionLike("s.schema_name", "information_schema"));
                    queryBlock.addWhere(
                            SQLExprUtils.conditionIn("s.schema_id", Arrays.<Object>asList("schema1", "schema2"), null));

                    SQLSubqueryTableSource replaceTo = new SQLSubqueryTableSource(queryBlock);
                    replaceTo.setAlias(x.getAlias());
                    SQLUtils.replaceInParent(x, replaceTo);
                    return false;
                }

                return true;
            }
        };
        stmt.accept(v);

        assertEquals("SELECT t.table_schema, t.table_type, s.schema_name\n" +
                "FROM tables t\n" +
                "\tINNER JOIN (\n" +
                "\t\tSELECT schema_name\n" +
                "\t\tFROM oa_schema s\n" +
                "\t\tWHERE s.schema_name LIKE 'information_schema'\n" +
                "\t\t\tAND s.schema_id IN ('schema1', 'schema2')\n" +
                "\t) s\n" +
                "\tON t.table_schema = s.schema_name\n" +
                "WHERE s.schema_name = 'information_schema'", stmt.toString());
    }
}
