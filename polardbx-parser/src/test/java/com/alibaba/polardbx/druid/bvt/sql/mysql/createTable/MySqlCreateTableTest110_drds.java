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
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlCreateTableTest110_drds extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "create table a (id int(10)) dbpartition by hash(id) dbpartitionS 2 tbpartition by hash(id) tbpartitions 2 ";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLCreateTableStatement stmt = (SQLCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(1, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE a (\n" +
                "\tid int(10)\n" +
                ")\n" +
                "DBPARTITION BY hash(id) DBPARTITIONS 2\n" +
                "TBPARTITION BY hash(id) TBPARTITIONS 2", stmt.toString());
    }

}