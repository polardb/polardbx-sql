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

package com.alibaba.polardbx.druid.bvt.sql.builder;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class DynamicFilterTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "Select * from t where id = 5 limit 1";

        {
            Map<String, List<Object>> columnValues = new HashMap<String, List<Object>>();
            columnValues.put("schema_id", Arrays.<Object>asList(1, 2, 3));
            String result = addFilters(sql, columnValues, null);

            assertEquals("SELECT *\n" +
                    "FROM t\n" +
                    "WHERE id = 5\n" +
                    "\tAND schema_id IN (1, 2, 3)\n" +
                    "LIMIT 1", result);
            sql = result;
        }

        {
            Map<String, List<Object>> columnValues = new HashMap<String, List<Object>>();
            columnValues.put("schema_id", Arrays.<Object>asList(3, 4, 5));
            String result = addFilters(sql, columnValues, null);

            assertEquals("SELECT *\n" +
                    "FROM t\n" +
                    "WHERE id = 5\n" +
                    "\tAND schema_id IN (3)\n" +
                    "LIMIT 1", result);
        }
    }

    public void test_1() throws Exception {
        String sql = "Select * from t where schema_id = 5 limit 1";

        {
            Map<String, List<Object>> columnValues = new HashMap<String, List<Object>>();
            columnValues.put("schema_id", Arrays.<Object>asList(1, 2, 3));
            String result = addFilters(sql, columnValues, null);

            assertEquals("SELECT *\n" +
                    "FROM t\n" +
                    "WHERE false\n" +
                    "LIMIT 1", result);
            sql = result;
        }
    }

    public void test_2() throws Exception {
        String sql = "Select * from t where schema_id in(5) limit 1";

        {
            Map<String, List<Object>> columnValues = new HashMap<String, List<Object>>();
            columnValues.put("schema_id", Arrays.<Object>asList(1, 2, 3));
            String result = addFilters(sql, columnValues, null);

            assertEquals("SELECT *\n" +
                    "FROM t\n" +
                    "WHERE false\n" +
                    "LIMIT 1", result);
            sql = result;
        }
    }

    public void test_3() throws Exception {
        String sql = "Select * from t where schema_id in(3, 5, 7) limit 1";

        {
            Map<String, List<Object>> columnValues = new HashMap<String, List<Object>>();
            columnValues.put("schema_id", Arrays.<Object>asList(1, 2, 3));
            String result = addFilters(sql, columnValues, null);

            assertEquals("SELECT *\n" +
                    "FROM t\n" +
                    "WHERE schema_id IN (3)\n" +
                    "LIMIT 1", result);
            sql = result;
        }
    }

    public static String addFilters(String sql, Map<String, List<Object>> columnValues, TimeZone timeZone) {
        final SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);

        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();
        for (Map.Entry<String, List<Object>> entry : columnValues.entrySet()) {
            queryBlock.addWhere(
                    SQLExprUtils.conditionIn(entry.getKey(), entry.getValue(), timeZone));
        }

        return stmt.toString();
    }
}
