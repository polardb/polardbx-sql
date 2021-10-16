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
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

import java.util.List;

public class MySqlAnalyzeTest_1 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "ANALYZE TABLE t1,t2";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);
        
        assertEquals("ANALYZE TABLE t1, t2", //
                            stmt.toString());

        assertEquals(2, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());
    }

    public void test_1() {
        parseTrue("ANALYZE DATABASE tpch", "ANALYZE DATABASE tpch");
        parseTrue("ANALYZE TABLE tpch.lineitem where l_shipdate <= date '1998-12-01' - interval '120' day",
                "ANALYZE TABLE tpch.lineitem\n" +
                        " WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '120' DAY");
        parseTrue("ANALYZE COLUMN lineitem(l_shipdate, l_commitdate) where l_shipdate <=date '1998-12-01' - interval '120' day",
                "ANALYZE COLUMN lineitem(l_shipdate,l_commitdate)\n" +
                        " WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '120' DAY");
        parseTrue("ANALYZE COLUMNS GROUP tpch.lineitem(l_shipmode, l_quantity) where l_shipmode in('FOB', 'AIR') and l_quantity <24",
                "ANALYZE COLUMNS GROUP tpch.lineitem(l_shipmode,l_quantity)\n" +
                        " WHERE l_shipmode IN ('FOB', 'AIR')\n" +
                        "AND l_quantity < 24");
        parseTrue("ANALYZE COLUMNS GROUP lineitem(l_shipmode, l_quantity) where l_shipmode in('FOB', 'AIR') and l_quantity <24",
                "ANALYZE COLUMNS GROUP lineitem(l_shipmode,l_quantity)\n" +
                        " WHERE l_shipmode IN ('FOB', 'AIR')\n" +
                        "AND l_quantity < 24");
    }
}
