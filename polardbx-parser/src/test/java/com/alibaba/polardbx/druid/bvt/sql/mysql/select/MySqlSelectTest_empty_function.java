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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Assert;

import java.util.List;

/**
 * @version 1.0
 */
public class MySqlSelectTest_empty_function extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "SELECT (1)*0.3 (剩)折后金额";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            List<SQLStatement> statementList = parser.parseStatementList();
        } catch (Exception e) {
            Assert.assertEquals("Empty method name with args:IDENTIFIER, pos 19, line 1, column 17, token IDENTIFIER 剩", e.getMessage());
        }
    }
}
