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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRevokeRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

/**
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/revoke.html">Revoke</a>
 * @see com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRevokeRoleStatement
 * @since 2.1.12.9_drds_3
 */
public class MySqlRevokeRoleTest extends MysqlTest {
    /**
     * Inputs
     */
    private int testId;
    private String sql;

    /**
     * Outputs
     */
    private int revokedAccountsCount;
    private int fromAccountsCount;
    private String expectedDefaultOutput;
    private String expectedLowerCaseOutput;

    private void doTest() {
        final MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> sqlStatements = parser.parseStatementList();

        assertEquals(String.format("Test case %d: Sql statements size should be 1!", testId), 1, sqlStatements.size());
        assertTrue(String.format("Test case %d: Not revoke role statement!", testId),
            sqlStatements.get(0) instanceof MySqlRevokeRoleStatement);

        MySqlRevokeRoleStatement stmt = (MySqlRevokeRoleStatement) sqlStatements.get(0);

        assertEquals(String.format("Test case %d: Revoked accounts size not matched!", testId), revokedAccountsCount,
            stmt.getRevokedAccounts().size());
        assertEquals(String.format("Test case %d: From accounts size not matched!", testId), fromAccountsCount,
            stmt.getFromAccounts().size());
        assertEquals(String.format("Test case %d: Default output not matched!", testId), expectedDefaultOutput,
            SQLUtils.toMySqlString(stmt));
        assertEquals(String.format("Test case %d: Lower case output not matched!", testId), expectedLowerCaseOutput,
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testGrantRole() {
        String[] inputs = {
            "revoke a from b;",
            "Revoke a@'%' from b;",
            "Revoke a@'%' From b@'localhost';",
            "Revoke a From b@'localhost';",
            "Revoke a, a@localhost From b, b@'localhost';",
        };

        Object[][] outputs = {
            {1, 1, "REVOKE 'a' FROM 'b';", "revoke 'a' from 'b';"},
            {1, 1, "REVOKE 'a'@'%' FROM 'b';", "revoke 'a'@'%' from 'b';"},
            {1, 1, "REVOKE 'a'@'%' FROM 'b'@'localhost';", "revoke 'a'@'%' from 'b'@'localhost';"},
            {1, 1, "REVOKE 'a' FROM 'b'@'localhost';", "revoke 'a' from 'b'@'localhost';"},
            {2, 2, "REVOKE 'a', 'a'@'localhost' FROM 'b', 'b'@'localhost';",
                "revoke 'a', 'a'@'localhost' from 'b', 'b'@'localhost';"},
        };

        assertEquals("Inputs and outputs size not match!", inputs.length, outputs.length);
        for (int i=0; i<inputs.length; i++) {
            testId = i;
            sql = inputs[i];

            revokedAccountsCount = (Integer) outputs[i][0];
            fromAccountsCount = (Integer) outputs[i][1];
            expectedDefaultOutput = (String) outputs[i][2];
            expectedLowerCaseOutput = (String) outputs[i][3];

            doTest();
        }
    }
}
