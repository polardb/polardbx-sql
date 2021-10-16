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

package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySql8ShowGrantsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySql8ShowGrantsStatement.UserSpec;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/show-grants.html">Show Grants</a>
 * @see MySql8ShowGrantsStatement
 * @since 2.1.12.9_drds_3
 */
public class MySql8ShowGrantsTest extends TestCase {
    /**
     * Inputs
     */
    private int testCase;
    private String sql;

    /**
     * Outputs
     */
    private MySql8ShowGrantsStatement.UserSpec expectedUserSpec;
    private int expectedRoleCount;
    private String expectedDefaultOutput;
    private String expectedLowCaseOutput;

    protected void doTest() {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmts = parser.parseStatementList();
        assertEquals(String.format("Test case %d: Statement list size not 1!", testCase), 1, stmts.size());
        assertTrue(String.format("Test case %d: Statement class incorrect!", testCase),
            stmts.get(0) instanceof MySql8ShowGrantsStatement);

        MySql8ShowGrantsStatement stmt = (MySql8ShowGrantsStatement) stmts.get(0);
        assertEquals(String.format("Test case %d: User spec not match!", testCase), expectedUserSpec,
            stmt.getUserSpec());
        assertEquals(String.format("Test case %d: Role count not match!", testCase), expectedRoleCount,
            stmt.getRolesToUse().size());

        assertEquals(String.format("Test case %d: Default output not match!", testCase), expectedDefaultOutput,
            SQLUtils.toMySqlString(stmt));
        assertEquals(String.format("Test case %d: Lower case output not match!", testCase), expectedLowCaseOutput,
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testShowGrants() {
        String[] inputs = {
            "Show Grants;",
            "Show Grants For Current_user;",
            "Show Grants For Current_user();",
            "Show Grants For a;",
            "Show Grants For a@'b';",
            "Show Grants For Current_user using a, b;",
            "Show Grants For Current_user() using a@'c';",
            "Show Grants For a using c, 'd'@'e';",
            "Show Grants For a@'b' using c, 'd'@'e';",
        };

        Object[][] outputs = {
            {UserSpec.NONE, 0, "SHOW GRANTS;", "show grants;"},
            {UserSpec.CURRENT_USER, 0, "SHOW GRANTS FOR CURRENT_USER;", "show grants for current_user;"},
            {UserSpec.CURRENT_USER_FUNC, 0, "SHOW GRANTS FOR CURRENT_USER();", "show grants for current_user();"},
            {UserSpec.USERNAME, 0, "SHOW GRANTS FOR 'a';", "show grants for 'a';"},
            {UserSpec.USERNAME, 0, "SHOW GRANTS FOR 'a'@'b';", "show grants for 'a'@'b';"},
            {
                UserSpec.CURRENT_USER, 2, "SHOW GRANTS FOR CURRENT_USER USING 'a', 'b';",
                "show grants for current_user using 'a', 'b';"},
            {
                UserSpec.CURRENT_USER_FUNC, 1, "SHOW GRANTS FOR CURRENT_USER() USING 'a'@'c';",
                "show grants for current_user() using 'a'@'c';"},
            {
                UserSpec.USERNAME, 2, "SHOW GRANTS FOR 'a' USING 'c', 'd'@'e';",
                "show grants for 'a' using 'c', 'd'@'e';"},
            {
                UserSpec.USERNAME, 2, "SHOW GRANTS FOR 'a'@'b' USING 'c', 'd'@'e';",
                "show grants for 'a'@'b' using 'c', 'd'@'e';"},
        };

        assertEquals("Input size not matching output size!", inputs.length, outputs.length);
        for (int i = 0; i < inputs.length; i++) {
            testCase = i;
            sql = inputs[i];

            expectedUserSpec = (UserSpec) outputs[i][0];
            expectedRoleCount = (Integer) outputs[i][1];
            expectedDefaultOutput = (String) outputs[i][2];
            expectedLowCaseOutput = (String) outputs[i][3];

            doTest();
        }
    }
}
