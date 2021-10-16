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

package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

public class MySqlCreateTableTest137 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE `c1` (\n" +
                "        order bigint);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE `c1` (\n" +
                "\torder bigint\n" +
                ");", stmt.toString());

        assertEquals("create table `c1` (\n" +
                "\torder bigint\n" +
                ");", stmt.toLowerCaseString());

        SQLUtils.parseSingleMysqlStatement("insert into w (order) values (1)");
        SQLUtils.parseSingleMysqlStatement("select order from w1 where order = 1");

    }


    public void test_1() throws Exception {
        SQLUtils.parseSingleMysqlStatement("create table order (fid bigint)");
        SQLUtils.parseSingleMysqlStatement("insert into order (f1) values (1)");
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("/*+engine=mpp*/select f1 from pt_dc.order where f1 = 1");
        List<SQLCommentHint> hints = stmt.getHeadHintsDirect();
        System.out.println(hints);

    }




}