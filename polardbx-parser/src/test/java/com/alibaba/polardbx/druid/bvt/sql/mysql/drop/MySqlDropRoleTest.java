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

package com.alibaba.polardbx.druid.bvt.sql.mysql.drop;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDropRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Test;

import java.util.List;

/**
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/drop-role.html">Drop Role</a>
 * @see com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDropRoleStatement
 * @since 2.1.12.9_drds_3
 */
public class MySqlDropRoleTest extends MysqlTest {
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

    private void doTest() {
        final MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statements = parser.parseStatementList();
        assertEquals("Statement list size should be 1!",1, statements.size());

        assertTrue("Not drop role statement!", statements.get(0) instanceof MySqlDropRoleStatement);

        MySqlDropRoleStatement stmt = (MySqlDropRoleStatement) statements.get(0);
        assertEquals("Role spec list size should be 1!",expectedRoleSpecs, stmt.getRoleSpecs().size());

        assertEquals(expectedDefaultOutputSql, SQLUtils.toMySqlString(stmt));
        assertEquals(expectedLowerCaseOutputSql, SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    @Test
    public void testDropRole() {
        String[] inputs = {
            "DroP RoLE abc;",
            "Drop role 'abc'; ",
            "DROP rOLe '';",
            "DROP rOLe `abc`;",
            "Drop rOLe abc@def;",
            "Drop rOLe 'abc'@'%';",
            "Drop rOLe ''@'%';",
            "Drop rOLe ''@'%', abc;",
        };

        Object[][] outputs = {
            {1, "DROP ROLE 'abc';", "drop role 'abc';"},
            {1, "DROP ROLE 'abc';", "drop role 'abc';"},
            {1, "DROP ROLE '';", "drop role '';"},
            {1, "DROP ROLE '`abc`';", "drop role '`abc`';"},
            {1, "DROP ROLE 'abc'@'def';", "drop role 'abc'@'def';"},
            {1, "DROP ROLE 'abc'@'%';", "drop role 'abc'@'%';"},
            {1, "DROP ROLE ''@'%';", "drop role ''@'%';"},
            {2, "DROP ROLE ''@'%', 'abc';", "drop role ''@'%', 'abc';"},
        };

        assertEquals("Input and output should have same length!", inputs.length, outputs.length);
        for (int i=0; i< inputs.length; i++) {
            // setup input
            sql = inputs[i];

            // setup outputs
            expectedRoleSpecs = (Integer) (outputs[i][0]);
            expectedDefaultOutputSql = (String) outputs[i][1];
            expectedLowerCaseOutputSql = (String) outputs[i][2];

            // do test
            doTest();
        }
    }
}
