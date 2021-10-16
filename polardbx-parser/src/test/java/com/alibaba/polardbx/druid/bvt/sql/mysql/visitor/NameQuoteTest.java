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

package com.alibaba.polardbx.druid.bvt.sql.mysql.visitor;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import junit.framework.TestCase;

public class NameQuoteTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "create table t1 (fid bigint);";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);


        assertEquals("CREATE TABLE `t1` (\n" +
                "\t`fid` bigint\n" +
                ");", stmt.toString(VisitorFeature.OutputNameQuote));
    }

    public void test_1() throws Exception {
        String sql = "select fid id from a.b.`t1` where `fid` = 3";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);


        assertEquals("SELECT `fid` AS `id`\n" +
                "FROM `a`.`b`.`t1`\n" +
                "WHERE `fid` = 3", stmt.toString(VisitorFeature.OutputNameQuote));
    }



    public void test_4() throws Exception {
        String sql = "create table \"t1\" (\"fid\" bigint);";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);


        assertEquals("CREATE TABLE `t1` (\n" +
                "\t`fid` bigint\n" +
                ");", stmt.toString(VisitorFeature.OutputNameQuote));
    }

    public void test_10() throws Exception {
        String sql =  "select * from `hello`.`world`";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);

        String result = toSQLString(stmt, DbType.mysql, null, VisitorFeature.OutputNameQuote);

        assertEquals("SELECT *\n" +
                "FROM \"hello\".\"world\"", result);
    }

    public void test_11() throws Exception {
        String sql = "select json_remove('{\"a\":\"1\", \"b\":\"2\", \"c\":\"3\", \"d\":\"4\"}', array['$.d', '$.c']) as a;";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);

        String result = toSQLString(stmt, DbType.mysql, null, VisitorFeature.OutputNameQuote);

        assertEquals("SELECT json_remove('{\"a\":\"1\", \"b\":\"2\", \"c\":\"3\", \"d\":\"4\"}', array['$.d', '$.c']) AS \"a\";", result);
    }


    public void test_12() throws Exception {
        String sql =  "select json_extract(`text`,concat('$.',`name`)) " +
                "from ots_test.integration_test where id = 1";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.SelectItemGenerateAlias);

        String result = toSQLString1(stmt, DbType.mysql, null, VisitorFeature.OutputNameQuote);

        assertEquals("SELECT json_extract(`text`, concat('$.', `name`)) AS `json_extract(``text``,concat('$.',``name``))`\n" +
                "FROM `ots_test`.`integration_test`\n" +
                "WHERE `id` = 1", result);

        SQLStatement stmt2 = SQLUtils.parseSingleStatement(result, DbType.mysql, SQLParserFeature.SelectItemGenerateAlias);
        String result2 = toSQLString1(stmt2, DbType.mysql, null, VisitorFeature.OutputNameQuote);

        assertEquals("SELECT json_extract(`text`, concat('$.', `name`)) AS `json_extract(``text``,concat('$.',``name``))`\n" +
                "FROM `ots_test`.`integration_test`\n" +
                "WHERE `id` = 1", result2);
    }

    public static String toSQLString(SQLObject sqlObject, DbType dbType, VisitorFeature... features) {
        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, dbType);
        visitor.config(VisitorFeature.OutputNameQuote, true);
        visitor.setNameQuote('"');

        sqlObject.accept(visitor);

        String sql = out.toString();
        return sql;
    }


    public static String toSQLString1(SQLObject sqlObject, DbType dbType, VisitorFeature... features) {
        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, dbType);
        visitor.config(VisitorFeature.OutputNameQuote, true);
        visitor.setNameQuote('`');

        sqlObject.accept(visitor);

        String sql = out.toString();
        return sql;
    }
}
