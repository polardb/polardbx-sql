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

import org.junit.Test;

import static org.junit.Assert.*;

public class RawSqlPreparedStatementTest {

    @Test
    public void testRawSqlPreparedStatement_Execute() throws Exception {
        final String sql = "INSERT INTO country VALUES (?, ?)";
        RawSqlPreparedStatement ps = new RawSqlPreparedStatement(sql);
        ps.setInt(1, 1);
        ps.setString(2, "a");
        ps.execute();

        final String expected = "INSERT INTO country VALUES (1, 'a')";
        String rawSql = ps.toString();
        assertEquals(expected, rawSql);
    }

    @Test
    public void testRawSqlPreparedStatement_SqlEndsWithPlaceholder() throws Exception {
        final String sql = "SELECT * FROM country WHERE id = ?";
        RawSqlPreparedStatement ps = new RawSqlPreparedStatement(sql);
        ps.setInt(1, 123);
        ps.execute();

        final String expected = "SELECT * FROM country WHERE id = 123";
        String rawSql = ps.toString();
        assertEquals(expected, rawSql);
    }

    @Test
    public void testRawSqlPreparedStatement_ExecuteBatch() throws Exception {
        final String testSqlTemplate = "INSERT INTO country VALUES (?, ?)";
        final String[] nameList = new String[] { "a", "b", "c" };

        RawSqlPreparedStatement ps = new RawSqlPreparedStatement(testSqlTemplate);
        for (int i = 0; i < nameList.length; i++) {
            ps.setInt(1, i + 1);
            ps.setString(2, nameList[i]);
            ps.addBatch();
        }
        ps.executeBatch();

        final String expected = "INSERT INTO country VALUES (1, 'a');\n"
                              + "INSERT INTO country VALUES (2, 'b');\n"
                              + "INSERT INTO country VALUES (3, 'c');\n";
        String rawSql = ps.toString();
        assertEquals(expected, rawSql);
    }
}
