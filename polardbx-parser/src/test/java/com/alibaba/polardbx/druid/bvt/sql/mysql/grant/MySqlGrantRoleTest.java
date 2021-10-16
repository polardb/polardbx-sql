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

package com.alibaba.polardbx.druid.bvt.sql.mysql.grant;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlGrantRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

/**
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/grant.html#grant-roles">Grant Role</a>
 * @see com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlGrantRoleStatement
 * @since 2.1.12.9_drds_3
 */
public class MySqlGrantRoleTest extends MysqlTest {
    /**
     * Inputs
     */
    private int testId;
    private String sql;

    /**
     * Outputs
     */
    private int sourceAccountsCount;
    private int destAccountsCount;
    private String expectedDefaultOutput;
    private String expectedLowerCaseOutput;
    private boolean expectedWithAdminOption;

    private void doTest() {
        final MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> sqlStatements = parser.parseStatementList();

        assertEquals(String.format("Test case %d: Sql statements size should be 1!", testId), 1, sqlStatements.size());
        assertTrue(String.format("Test case %d: Not grant role statement!", testId),
            sqlStatements.get(0) instanceof MySqlGrantRoleStatement);

        MySqlGrantRoleStatement stmt = (MySqlGrantRoleStatement) sqlStatements.get(0);

        assertEquals(String.format("Test case %d: Source accounts size not match!", testId), sourceAccountsCount,
            stmt.getSourceAccounts().size());
        assertEquals(String.format("Test case %d: Dest accounts size not match!", testId), destAccountsCount,
            stmt.getDestAccounts().size());
        assertEquals(String.format("Test case %d: Default output not match!", testId), expectedDefaultOutput,
            SQLUtils.toMySqlString(stmt));
        assertEquals(String.format("Test case %d: Lower case output not match!", testId), expectedLowerCaseOutput,
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
        assertEquals(String.format("Test case %d: With admin option not match!", testId), expectedWithAdminOption,
            stmt.isWithAdminOption());
    }

    public void testGrantRole() {
        String[] inputs = {
            "grAnt a to b;",
            "grAnt a@'%' to b;",
            "grAnt a@'%' to b@'localhost';",
            "grAnt a to b@'localhost';",
            "grAnt a, a@localhost to b, b@'localhost';",
            "Grant a, a@localhost to b, b@'localhost' With Admin Option;",
        };

        Object[][] outputs = {
            {1, 1, "GRANT 'a' TO 'b';", "grant 'a' to 'b';", false},
            {1, 1, "GRANT 'a'@'%' TO 'b';", "grant 'a'@'%' to 'b';", false},
            {1, 1, "GRANT 'a'@'%' TO 'b'@'localhost';", "grant 'a'@'%' to 'b'@'localhost';", false},
            {1, 1, "GRANT 'a' TO 'b'@'localhost';", "grant 'a' to 'b'@'localhost';", false},
            {2, 2, "GRANT 'a', 'a'@'localhost' TO 'b', 'b'@'localhost';",
                "grant 'a', 'a'@'localhost' to 'b', 'b'@'localhost';", false},
            {2, 2, "GRANT 'a', 'a'@'localhost' TO 'b', 'b'@'localhost' WITH ADMIN OPTION;",
                "grant 'a', 'a'@'localhost' to 'b', 'b'@'localhost' with admin option;", true},
        };

        assertEquals("Inputs and outputs size not match!", inputs.length, outputs.length);
        for (int i=0; i<inputs.length; i++) {
            testId = i;
            sql = inputs[i];

            sourceAccountsCount = (Integer) outputs[i][0];
            destAccountsCount = (Integer) outputs[i][1];
            expectedDefaultOutput = (String) outputs[i][2];
            expectedLowerCaseOutput = (String) outputs[i][3];
            expectedWithAdminOption = (Boolean) outputs[i][4];

            doTest();
        }
    }
}
