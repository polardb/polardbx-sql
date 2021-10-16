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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;

import java.util.List;

/**
 * @version 1.0
 * @ClassName DrdsBaselineTest
 * @description
 * @Author zzy
 * @Date 2019/9/5 14:04
 */
public class DrdsBaselineTest extends MysqlTest {

    public void test_0() {
        String sql = "baseline list";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE LIST", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline list", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_1() {
        String sql = "baseline list;";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE LIST;", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline list;", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_2() {
        String sql = "baseline list 123;";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE LIST 123;", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline list 123;", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_3() {
        String sql = "baseline list 123,-456;";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE LIST 123, -456;", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline list 123, -456;", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_4() {
        String sql = "baseline list 123";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE LIST 123", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline list 123", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_5() {
        String sql = "baseline list 123,-456";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE LIST 123, -456", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline list 123, -456", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_6() {
        String sql = "baseline persist 12345656789098873";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE PERSIST 12345656789098873", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline persist 12345656789098873", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_7() {
        String sql = "baseline load -12345656789098873";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE LOAD -12345656789098873", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline load -12345656789098873", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_8() {
        String sql = "baseline clear 123,-12345656789098873;";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE CLEAR 123, -12345656789098873;", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline clear 123, -12345656789098873;", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_9() {
        String sql = "baseline validate 123,-12345656789098873;";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE VALIDATE 123, -12345656789098873;", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline validate 123, -12345656789098873;", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_10() {
        String sql = "baseline add sql /*+TDDL:cmd_extra(enable_hash_join=false) SORT_MERGE_JOIN(l2,l3)*/ select count(*) from l2, l3 where l2.id = l3.id and l2.name = \"hello\";";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE ADD SQL\n" +
                "/*+TDDL:cmd_extra(enable_hash_join=false) SORT_MERGE_JOIN(l2,l3)*/\n" +
                "SELECT count(*)\n" +
                "FROM l2, l3\n" +
                "WHERE l2.id = l3.id\n" +
                "\tAND l2.name = 'hello';", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline add sql\n" +
                "/*+TDDL:cmd_extra(enable_hash_join=false) SORT_MERGE_JOIN(l2,l3)*/\n" +
                "select count(*)\n" +
                "from l2, l3\n" +
                "where l2.id = l3.id\n" +
                "\tand l2.name = 'hello';", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_11() {
        String sql = "baseline add SQL select count(*) from l2, l3 where l2.id = l3.id and l2.name = \"hello\";";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE ADD SQL\n" +
                "SELECT count(*)\n" +
                "FROM l2, l3\n" +
                "WHERE l2.id = l3.id\n" +
                "\tAND l2.name = 'hello';", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline add sql\n" +
                "select count(*)\n" +
                "from l2, l3\n" +
                "where l2.id = l3.id\n" +
                "\tand l2.name = 'hello';", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_12() {
        String sql = "baseline delete 123,-12345656789098873;";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE DELETE 123, -12345656789098873;", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline delete 123, -12345656789098873;", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }


    public void test_13() {
        String sql = "baseline fix SQL /*+TDDL:cmd_extra(enable_hash_join=false) SORT_MERGE_JOIN(l2,l3)*/ select count(*) from l2, l3 where l2.id = l3.id and l2.name = \"hello\";";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE FIX SQL\n" +
                "/*+TDDL:cmd_extra(enable_hash_join=false) SORT_MERGE_JOIN(l2,l3)*/\n" +
                "SELECT count(*)\n" +
                "FROM l2, l3\n" +
                "WHERE l2.id = l3.id\n" +
                "\tAND l2.name = 'hello';", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline fix sql\n" +
                "/*+TDDL:cmd_extra(enable_hash_join=false) SORT_MERGE_JOIN(l2,l3)*/\n" +
                "select count(*)\n" +
                "from l2, l3\n" +
                "where l2.id = l3.id\n" +
                "\tand l2.name = 'hello';", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_14() {
        String sql = "baseline fix sql select count(*) from l2, l3 where l2.id = l3.id and l2.name = \"hello\";";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL, SQLParserFeature.DRDSBaseline);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("BASELINE FIX SQL\n" +
                "SELECT count(*)\n" +
                "FROM l2, l3\n" +
                "WHERE l2.id = l3.id\n" +
                "\tAND l2.name = 'hello';", SQLUtils.toMySqlString(result));
        Assert.assertEquals("baseline fix sql\n" +
                "select count(*)\n" +
                "from l2, l3\n" +
                "where l2.id = l3.id\n" +
                "\tand l2.name = 'hello';", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }
}
