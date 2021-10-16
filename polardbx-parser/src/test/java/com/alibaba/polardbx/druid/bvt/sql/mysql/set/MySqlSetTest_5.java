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
package com.alibaba.polardbx.druid.bvt.sql.mysql.set;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import junit.framework.TestCase;

import java.util.List;

public class MySqlSetTest_5 extends TestCase {

    public void test_0() throws Exception {
        String sql = "set charset 'gbk'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();
        SQLSetStatement stmt = (SQLSetStatement) stmtList.get(0);
//        print(stmtList);

        assertEquals(1, stmtList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(0, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

        String text = SQLUtils.toMySqlString(stmt);

        assertEquals("SET CHARACTER SET 'gbk'", stmt.toString());
        assertEquals("set character set 'gbk'", stmt.toLowerCaseString());
    }
}
