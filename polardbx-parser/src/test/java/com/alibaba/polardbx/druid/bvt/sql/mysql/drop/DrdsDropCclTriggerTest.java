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

package com.alibaba.polardbx.druid.bvt.sql.mysql.drop;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author busu
 * date: 2021/4/16 11:16 上午
 */
public class DrdsDropCclTriggerTest extends MysqlTest {

    @Test
    public void testDropCclTrigger1() {
        String sql = "DROP CCL_TRIGGER `busu`";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        Assert.assertEquals(sql, output);
    }

    @Test
    public void testDropCclTrigger2() {
        String sql = "DROP CCL_TRIGGER ";
        MySqlStatementParser parser = newParser(sql);
        String errorMessage = null;
        try {
            SQLStatement stmt = parser.parseStatement();
        } catch (Exception exception) {
            errorMessage = exception.getMessage();
        }
        System.out.println(errorMessage);
        Assert.assertEquals("illegal name, pos 17, line 1, column 18, token EOF", errorMessage);
    }

    private MySqlStatementParser newParser(String sql) {
        return new MySqlStatementParser(sql, SQLParserFeature.DrdsCCL);
    }
}
