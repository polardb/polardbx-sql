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

package com.alibaba.polardbx.planner.parser;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 * @create 2018-07-23 14:34
 */
@Ignore
public class SqlParserEscapeCharTest {
    @Test
    public void testscapeharacter() throws Exception{
        String sql2 = "select \"\"\"1\"\"\"\"\" as a;";
        String sql3 = "select \"\\\"1\"\"\"\"\" as a;";
        String sql4 = "select \"\"\"1\"\"\\\"\" as a;";
        MySqlStatementParser parser = new MySqlStatementParser(sql2,
                SQLParserFeature.TDDLHint,
                SQLParserFeature.IgnoreNameQuotes);
        List<SQLStatement> stmtList = parser.parseStatementList();

        MySqlStatementParser parser1 = new MySqlStatementParser(sql3,
                SQLParserFeature.TDDLHint,
                SQLParserFeature.IgnoreNameQuotes);
        List<SQLStatement> stmtList1 = parser1.parseStatementList();
        MySqlStatementParser parser2 = new MySqlStatementParser(sql4,
                SQLParserFeature.TDDLHint,
                SQLParserFeature.IgnoreNameQuotes);
        List<SQLStatement> stmtList2 = parser2.parseStatementList();
        String equalsSql = "SELECT '\"1\"\"' AS a;";
        Assert.assertEquals(equalsSql,stmtList.get(0).toString());
        Assert.assertEquals(equalsSql,stmtList1.get(0).toString());
        Assert.assertEquals(equalsSql,stmtList2.get(0).toString());
    }

    @Test
    public void testscapeharacter2() throws Exception{
        String sql1 = "insert  into t1 values('\\\\abc');";
        String sql2 = "insert into t1 values(  \"\\\\abc\")";
        MySqlStatementParser parser1 = new MySqlStatementParser(sql1,
                SQLParserFeature.TDDLHint,
                SQLParserFeature.IgnoreNameQuotes);
        List<SQLStatement> stmtList1 = parser1.parseStatementList();
        MySqlStatementParser parser2 = new MySqlStatementParser(sql2,
                SQLParserFeature.TDDLHint,
                SQLParserFeature.IgnoreNameQuotes);
        List<SQLStatement> stmtList2 = parser2.parseStatementList();
        String equalsSql1 = "INSERT INTO t1\n" + "VALUES ('\\\\abc');";
        String equalsSql2 = "INSERT INTO t1\n" + "VALUES ('\\\\\\\\abc')";
        Assert.assertEquals(equalsSql1,stmtList1.get(0).toString());
        Assert.assertEquals(equalsSql2,stmtList2.get(0).toString());
    }

    @Test
    public void testscapeharacter3() throws Exception{
        String sql1 = "select '拔萝卜车\\\\';";

        MySqlStatementParser parser1 = new MySqlStatementParser(sql1,
                SQLParserFeature.TDDLHint,
                SQLParserFeature.IgnoreNameQuotes);
        List<SQLStatement> stmtList1 = parser1.parseStatementList();

        String equalsSql1 = "INSERT INTO t1\n" + "VALUES ('\\\\');";
        String equalsSql2 = "INSERT INTO t1\n" + "VALUES ('\\\\\\\\abc')";
        Assert.assertEquals(equalsSql1,stmtList1.get(0).toString());
    }

    @Test
    public void testscapeharacter4() throws Exception{
        String sql1 = "select \"拔萝卜车\\\\\";";

        MySqlStatementParser parser1 = new MySqlStatementParser(sql1,
                SQLParserFeature.TDDLHint,
                SQLParserFeature.IgnoreNameQuotes);
        List<SQLStatement> stmtList1 = parser1.parseStatementList();

        String equalsSql1 = "INSERT INTO t1\n" + "VALUES ('\\\\');";
        String equalsSql2 = "INSERT INTO t1\n" + "VALUES ('\\\\\\\\abc')";
        Assert.assertEquals(equalsSql1,stmtList1.get(0).toString());
    }

}
