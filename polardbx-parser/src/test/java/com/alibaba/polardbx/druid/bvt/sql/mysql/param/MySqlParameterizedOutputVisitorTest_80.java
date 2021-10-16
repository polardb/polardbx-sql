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

package com.alibaba.polardbx.druid.bvt.sql.mysql.param;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class MySqlParameterizedOutputVisitorTest_80 extends TestCase {
    public void test1()  {

        String sql = "select ((0='x6') & 31) ^ (ROW(76, 4) NOT IN (ROW(1, 2 ),ROW(3, 4)) );";


        SQLStatement stmt = ParameterizedOutputVisitorUtils.parameterizeOf(sql, DbType.mysql);
        assertEquals("SELECT ((? = ?) & ?) ^ (ROW(?, ?) NOT IN (ROW(?, ?), ROW(?, ?)));", stmt.toString());


        List<Object> outParameters = new ArrayList<Object>();
        SQLStatement stmt2 = ParameterizedOutputVisitorUtils.parameterizeOf(sql, outParameters, DbType.mysql);

        assertEquals("SELECT ((? = ?) & ?) ^ (ROW(?, ?) NOT IN (ROW(?, ?), ROW(?, ?)));", stmt2.toString());

        assertEquals(9, outParameters.size());

        assertEquals(0, outParameters.get(0));
        assertEquals("x6", outParameters.get(1));
        assertEquals(31, outParameters.get(2));
        assertEquals(76, outParameters.get(3));
        assertEquals(4, outParameters.get(4));
        assertEquals(1, outParameters.get(5));
        assertEquals(2, outParameters.get(6));
        assertEquals(3, outParameters.get(7));
        assertEquals(4, outParameters.get(8));
    }

    public void test2()  {

        String sql = "select a from t group by 1 order by 1;";

        SQLStatement stmt = ParameterizedOutputVisitorUtils.parameterizeOf(sql, DbType.mysql);
        assertEquals("SELECT a\n" +
                "FROM t\n" +
                "GROUP BY 1\n" +
                "ORDER BY 1;", stmt.toString());
    }

}
