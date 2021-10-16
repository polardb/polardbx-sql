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

package com.alibaba.polardbx.transaction.rawsql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RawSqlStatementTest {

    @Test
    public void testRawSqlStatement_Execute() throws Exception {
        final String testSql = "SELECT * FROM country";
        RawSqlStatement statement = new RawSqlStatement();
        statement.execute(testSql);

        String rawSql = statement.toString();
        assertEquals(testSql, rawSql);
    }

    @Test
    public void testRawSqlStatement_ExecuteQuery() throws Exception {
        final String testSql = "SELECT * FROM country";
        RawSqlStatement statement = new RawSqlStatement();
        statement.executeQuery(testSql);

        String rawSql = statement.toString();
        assertEquals(testSql, rawSql);
    }

    @Test
    public void testRawSqlStatement_ExecuteUpdate() throws Exception {
        final String testSql = "UPDATE country SET country = 'China'";
        RawSqlStatement statement = new RawSqlStatement();
        statement.executeUpdate(testSql);

        String rawSql = statement.toString();
        assertEquals(testSql, rawSql);
    }

    @Test
    public void testRawSqlStatement_ExecuteBatch() throws Exception {
        final String testSql[] = new String[] { "INSERT INTO country VALUES (1, 'a')",
                                                "INSERT INTO country VALUES (2, 'b')",
                                                "INSERT INTO country VALUES (3, 'c')", };
        RawSqlStatement statement = new RawSqlStatement();
        for (String sql : testSql) {
            statement.addBatch(sql);
        }
        statement.executeBatch();

        final String expected = "INSERT INTO country VALUES (1, 'a');\n"
                              + "INSERT INTO country VALUES (2, 'b');\n"
                              + "INSERT INTO country VALUES (3, 'c');\n";
        String rawSql = statement.toString();
        assertEquals(expected, rawSql);
    }
}
