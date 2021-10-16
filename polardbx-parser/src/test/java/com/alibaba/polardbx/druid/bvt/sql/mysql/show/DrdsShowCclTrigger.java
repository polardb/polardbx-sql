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

package com.alibaba.polardbx.druid.bvt.sql.mysql.show;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author busu
 * date: 2021/4/16 11:19 上午
 */
public class DrdsShowCclTrigger extends MysqlTest {

    @Test
    public void testShowCclTrigger1() {
        String sql = "SHOW CCL_TRIGGERS";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql, output);
    }

    @Test
    public void testShowCclTrigger2() {
        String sql = "SHOW CCL_TRIGGER `busutrigger1`, `busutrigger2`";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql, output);
    }

    @Test
    public void testShowCclTrigger3() {
        String sql = "SHOW CCL_TRIGGER `busutrigger`";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql, output);
    }

    private MySqlStatementParser newParser(String sql) {
        return new MySqlStatementParser(sql, SQLParserFeature.DrdsCCL);
    }
}
