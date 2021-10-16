/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql.insert;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import junit.framework.TestCase;

public class MySqlInsertTest_48 extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "insert into table1(col1,col2) " +
                "with cte1 as" +
                "(select * from t1)" +
                "select c1,c2 from cte1\n";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        assertEquals("INSERT INTO table1 (col1, col2)\n" +
                "WITH cte1 AS (\n" +
                "\t\tSELECT *\n" +
                "\t\tFROM t1\n" +
                "\t)\n" +
                "SELECT c1, c2\n" +
                "FROM cte1", stmt.toString());
    }
}
