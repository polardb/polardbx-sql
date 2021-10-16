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

public class MySqlCreateTableTest113_drds extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "create table if not exists test_table(\n" +
                "  id INT,name VARCHAR(30) DEFAULT NULL,\n" +
                "  create_time DATETIME DEFAULT NULL\n" +
                ")ENGINE = InnoDB DEFAULT CHARSET = utf8 \n" +
                "dbpartition BY YYYYMM_NOLOOP (create_time) \n" +
                "tbpartition BY YYYYMM_NOLOOP (create_time) \n" +
                "  STARTWITH 20160108 ENDWITH 20170108;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLCreateTableStatement stmt = (SQLCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(3, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE IF NOT EXISTS test_table (\n" +
                "\tid INT,\n" +
                "\tname VARCHAR(30) DEFAULT NULL,\n" +
                "\tcreate_time DATETIME DEFAULT NULL\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
                "DBPARTITION BY YYYYMM_NOLOOP(create_time)\n" +
                "TBPARTITION BY YYYYMM_NOLOOP(create_time) BETWEEN 20160108 AND 20170108;", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "create table if not exists test_table(\n" +
                "  id INT,name VARCHAR(30) DEFAULT NULL,\n" +
                "  create_time DATETIME DEFAULT NULL\n" +
                ")ENGINE = InnoDB DEFAULT CHARSET = utf8 \n" +
                "dbpartition BY YYYYMM_NOLOOP (create_time) \n" +
                "tbpartition BY YYYYMM_NOLOOP (create_time) \n" +
                "  BETWEEN 20160108 AND 20170108;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLCreateTableStatement stmt = (SQLCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(3, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE IF NOT EXISTS test_table (\n" +
                "\tid INT,\n" +
                "\tname VARCHAR(30) DEFAULT NULL,\n" +
                "\tcreate_time DATETIME DEFAULT NULL\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
                "DBPARTITION BY YYYYMM_NOLOOP(create_time)\n" +
                "TBPARTITION BY YYYYMM_NOLOOP(create_time) BETWEEN 20160108 AND 20170108;", stmt.toString());
    }
}