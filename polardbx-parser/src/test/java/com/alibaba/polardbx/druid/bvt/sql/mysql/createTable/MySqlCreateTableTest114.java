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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.sql.Types;
import java.util.List;

public class MySqlCreateTableTest114 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE best_sign_cont_task ( \n" +
                "  sys_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL COMMENT '系统时间' \n" +
                ") ENGINE=INNODB DEFAULT CHARSET=utf8 COMMENT '上上签合同创建任务记录'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(1, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE best_sign_cont_task (\n" +
                "\tsys_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '系统时间'\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = utf8 COMMENT '上上签合同创建任务记录'", stmt.toString());

        SchemaStatVisitor v = SQLUtils.createSchemaStatVisitor(JdbcConstants.MYSQL);
        stmt.accept(v);

        assertEquals(1, v.getColumns().size());
        SQLColumnDefinition column = stmt.findColumn("sys_time");
        assertNotNull(column);
        assertEquals(1, column.getConstraints().size());
        assertFalse(column.isPrimaryKey());
        assertEquals(Types.TIMESTAMP, column.jdbcType());
    }

}