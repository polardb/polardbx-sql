/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql.update;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;

public class MySqlUpdateTest_19_clone extends MysqlTest {

    public void test_1() throws Exception {
        String sql = "update table1 set id = 1 , name = 'a' ";

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);


        SQLStatement clone = statement.clone();

        assertEquals(statement.toString(), clone.toString());
    }

    public void test_2() throws Exception {
        String sql = "update table1 set id = 1 , name = 'a' where id = 3";

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        SQLStatement clone = statement.clone();

        assertEquals(statement.toString(), clone.toString());
    }

    public void test_3() throws Exception {
        String sql = "update table1 set id = ? , name = ? where id = ?";

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        SQLStatement clone = statement.clone();

        assertEquals(statement.toString(), clone.toString());
    }
}
