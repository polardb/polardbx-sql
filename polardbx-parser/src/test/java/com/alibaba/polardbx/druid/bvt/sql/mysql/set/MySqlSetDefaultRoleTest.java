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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetDefaultRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

/**
 * @author bairui.lrj
 * @see com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetDefaultRoleStatement
 * @since 2.1.12.9_drds_3
 */
public class MySqlSetDefaultRoleTest extends MysqlTest {
    /**
     * Inputs
     */
    private int caseId;
    private String sql;

    /**
     * Outputs
     */
    private MySqlSetDefaultRoleStatement.DefaultRoleSpec expectedRoleSpec;
    private int expectedRoleCount;
    private int expectedUserCount;
    private String expectedDefaultOutput;
    private String expectedLowerCaseOutput;

    private void doTest() {
        final MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statements = parser.parseStatementList();
        assertEquals(String.format("Test case %d: Statement list size should be 1!", caseId), 1, statements.size());

        assertEquals(String.format("Test case %d: Unexpected statement type!", caseId),
            MySqlSetDefaultRoleStatement.class,
            statements.get(0).getClass());

        MySqlSetDefaultRoleStatement stmt = (MySqlSetDefaultRoleStatement) statements.get(0);

        assertEquals(String.format("Test case %d: Role spec not match!", caseId), expectedRoleSpec,
            stmt.getDefaultRoleSpec());
        assertEquals(String.format("Test case %d: Role count not match!", caseId), expectedRoleCount,
            stmt.getRoles().size());
        assertEquals(String.format("Test case %d: User count not match!", caseId), expectedUserCount,
            stmt.getUsers().size());

        assertEquals(String.format("Test case %d: Default output not match!", caseId), expectedDefaultOutput,
            SQLUtils.toMySqlString(stmt));
        assertEquals(String.format("Test case %d: Lower case output not match!", caseId), expectedLowerCaseOutput,
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testMySqlSetDefaultRole() {
        String[] inputs = {
            "Set default role NONE to a",
            "Set default role ALL to a",
            "Set default role a, b to c@'localhost', d",
        };

        Object[][] outputs = {
            {
                MySqlSetDefaultRoleStatement.DefaultRoleSpec.NONE, 0, 1, "SET DEFAULT ROLE NONE TO 'a'",
                "set default role none to 'a'"},
            {
                MySqlSetDefaultRoleStatement.DefaultRoleSpec.ALL, 0, 1, "SET DEFAULT ROLE ALL TO 'a'",
                "set default role all to 'a'"},
            {
                MySqlSetDefaultRoleStatement.DefaultRoleSpec.ROLES, 2, 2,
                "SET DEFAULT ROLE 'a', 'b' TO 'c'@'localhost', 'd'",
                "set default role 'a', 'b' to 'c'@'localhost', 'd'"},
        };

        assertEquals("Inputs and outputs size not match!", inputs.length, outputs.length);

        for (int i = 0; i < inputs.length; i++) {
            // Inputs
            caseId = i;
            sql = inputs[i];

            // Outputs
            expectedRoleSpec = (MySqlSetDefaultRoleStatement.DefaultRoleSpec) outputs[i][0];
            expectedRoleCount = (Integer) outputs[i][1];
            expectedUserCount = (Integer) outputs[i][2];
            expectedDefaultOutput = (String) outputs[i][3];
            expectedLowerCaseOutput = (String) outputs[i][4];

            doTest();
        }
    }
}
