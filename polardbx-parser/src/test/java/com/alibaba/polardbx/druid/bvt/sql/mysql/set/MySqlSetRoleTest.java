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

package com.alibaba.polardbx.druid.bvt.sql.mysql.set;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

/**
 * @author bairui.lrj
 * @see com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetRoleStatement
 * @since 2.1.12.9_drds_3
 */
public class MySqlSetRoleTest extends MysqlTest {
    // Inputs
    private int caseId;
    private String sql;

    // Outputs
    private MySqlSetRoleStatement.RoleSpec expectedRoleSpec;
    private int expectedRoleCount;
    private String expectedDefaultOutput;
    private String expectedLowerCaseOutput;

    private void doTest() {
        final MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statements = parser.parseStatementList();
        assertEquals(String.format("Test case %d: Statement list size should be 1!", caseId), 1, statements.size());

        assertEquals(String.format("Test case %d: Unexpected statement type!", caseId),
            MySqlSetRoleStatement.class,
            statements.get(0).getClass());

        MySqlSetRoleStatement stmt = (MySqlSetRoleStatement) statements.get(0);

        assertEquals(String.format("Test case %d: Role spec not match!", caseId), expectedRoleSpec,
            stmt.getRoleSpec());
        assertEquals(String.format("Test case %d: Role count not match!", caseId), expectedRoleCount,
            stmt.getRoles().size());

        assertEquals(String.format("Test case %d: Default output not match!", caseId), expectedDefaultOutput,
            SQLUtils.toMySqlString(stmt));
        assertEquals(String.format("Test case %d: Lower case output not match!", caseId), expectedLowerCaseOutput,
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testMySqlSetRole() {
        String[] inputs = {
            "Set role default",
            "Set role None",
            "Set role All",
            "Set role All Except a, b@'localhost'",
            "Set role a, b@'localhost'",
        };

        Object[][] outputs = {
            {MySqlSetRoleStatement.RoleSpec.DEFAULT, 0, "SET ROLE DEFAULT", "set role default"},
            {MySqlSetRoleStatement.RoleSpec.NONE, 0, "SET ROLE NONE", "set role none"},
            {MySqlSetRoleStatement.RoleSpec.ALL, 0, "SET ROLE ALL", "set role all"},
            {MySqlSetRoleStatement.RoleSpec.ALL_EXCEPT, 2, "SET ROLE ALL EXCEPT 'a', 'b'@'localhost'",
                "set role all except 'a', 'b'@'localhost'"},
            {MySqlSetRoleStatement.RoleSpec.ROLES, 2, "SET ROLE 'a', 'b'@'localhost'",
                "set role 'a', 'b'@'localhost'"},
        };

        assertEquals("Inputs and outputs size not match!", inputs.length, outputs.length);

        for (int i = 0; i < inputs.length; i++) {
            // Inputs
            caseId = i;
            sql = inputs[i];

            // Outputs
            expectedRoleSpec = (MySqlSetRoleStatement.RoleSpec) outputs[i][0];
            expectedRoleCount = (Integer) outputs[i][1];
            expectedDefaultOutput = (String) outputs[i][2];
            expectedLowerCaseOutput = (String) outputs[i][3];

            doTest();
        }
    }
}
