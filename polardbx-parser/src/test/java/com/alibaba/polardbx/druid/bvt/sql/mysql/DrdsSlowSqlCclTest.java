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
import org.junit.Assert;
import org.junit.Test;

/**
 * @author busu
 * date: 2021/5/23 7:32 下午
 */
public class DrdsSlowSqlCclTest extends MysqlTest {

    @Test
    public void testSlowSqlCcl_GO_0() {
        String sql = "SLOW_SQL_CCL GO";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql, output);
    }

    @Test
    public void testSlowSqlCcl_GO_1() {
        String sql = "SLOW_SQL_CCL GO 10";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql, output);
    }

    @Test
    public void testSlowSqlCcl_GO_2() {
        String sql = "SLOW_SQL_CCL GO 10 11 12 13";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql, output);
    }

    @Test
    public void testSlowSqlCcl_GO_3() {
        String sql = "SLOW_SQL_CCL GO ";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql.trim(), output);
    }

    @Test
    public void testSlowSqlCcl_BACK_0() {
        String sql = "SLOW_SQL_CCL BACK";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql, output);
    }

    @Test
    public void testSlowSqlCcl_BACK_1() {
        String sql = "SLOW_SQL_CCL BACK 1 ";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql.trim(), output);
    }

    @Test
    public void testSlowSqlCCl_BACK_2() {
        String sql = "SLOW_SQL_CCL BACK ";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql.trim(), output);
    }

    private MySqlStatementParser newParser(String sql) {
        return new MySqlStatementParser(sql, SQLParserFeature.DrdsCCL);
    }
}
