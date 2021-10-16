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

package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Test;

import java.util.List;

/**
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/create-role.html">Create Role</a>
 * @see com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement
 * @since 2.1.12.9_drds_3
 */
public class MySqlCreateRoleTest extends MysqlTest {
    /**
     * Inputs of tests
     */
    private String sql;

    /**
     * Outputs of tests
     */
    private int expectedRoleSpecs;
    private String expectedDefaultOutputSql;
    private String expectedLowerCaseOutputSql;
    private boolean ifNotExists;

    private void doTest() {
        final MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statements = parser.parseStatementList();
        assertEquals("Statement list size should be 1!",1, statements.size());

        assertTrue("Not create role state!", statements.get(0) instanceof MySqlCreateRoleStatement);

        MySqlCreateRoleStatement stmt = (MySqlCreateRoleStatement) statements.get(0);
        assertEquals("Role spec list size should be 1!",expectedRoleSpecs, stmt.getRoleSpecs().size());
        assertEquals("If not exists not equal!", ifNotExists, stmt.isIfNotExists());

        assertEquals(expectedDefaultOutputSql, SQLUtils.toMySqlString(stmt));
        assertEquals(expectedLowerCaseOutputSql, SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    @Test
    public void testCreateRole() {
        String[] inputs = {
            "Create role 'abc'; ",
            "CREATE rOLe abc;",
            "CREATE rOLe `abc`;",
            "CREATE rOLe '';",
            "CREATE rOLe abc@def;",
            "CREATE rOLe abc@'1.2.3.4';",
            "CREATE rOLe abc@'127.0.0.1';",
            "CREATE rOLe 'abc'@'%';",
            "CREATE rOLe `abc`@`%`;",
            "CREATE rOLe ''@'%';",
            "CREATE rOLe if not Exists 'abc'@'%';",
            "CREATE rOLe if not Exists 'abc'@'%', 'abc';",
        };

        Object[][] outputs = {
            {1, "CREATE ROLE 'abc';", "create role 'abc';", false},
            {1, "CREATE ROLE 'abc';", "create role 'abc';", false},
            {1, "CREATE ROLE '`abc`';", "create role '`abc`';", false},
            {1, "CREATE ROLE '';", "create role '';", false},
            {1, "CREATE ROLE 'abc'@'def';", "create role 'abc'@'def';", false},
            {1, "CREATE ROLE 'abc'@'1.2.3.4';", "create role 'abc'@'1.2.3.4';", false},
            {1, "CREATE ROLE 'abc'@'127.0.0.1';", "create role 'abc'@'127.0.0.1';", false},
            {1, "CREATE ROLE 'abc'@'%';", "create role 'abc'@'%';", false},
            {1, "CREATE ROLE '`abc`'@'`%`';", "create role '`abc`'@'`%`';", false},
            {1, "CREATE ROLE ''@'%';", "create role ''@'%';", false},
            {1, "CREATE ROLE IF NOT EXISTS 'abc'@'%';", "create role if not exists 'abc'@'%';", true},
            {2, "CREATE ROLE IF NOT EXISTS 'abc'@'%', 'abc';", "create role if not exists 'abc'@'%', 'abc';", true},
        };

        assertEquals("Input and output should have same length!", inputs.length, outputs.length);
        for (int i=0; i< inputs.length; i++) {
            // setup input
            sql = inputs[i];

            // setup outputs
            expectedRoleSpecs = (Integer) (outputs[i][0]);
            expectedDefaultOutputSql = (String) outputs[i][1];
            expectedLowerCaseOutputSql = (String) outputs[i][2];
            ifNotExists = (Boolean) outputs[i][3];

            // do test
            doTest();
        }
    }
}
