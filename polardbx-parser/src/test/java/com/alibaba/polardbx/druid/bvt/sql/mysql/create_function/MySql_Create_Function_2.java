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
package com.alibaba.polardbx.druid.bvt.sql.mysql.create_function;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySql_Create_Function_2 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE FUNCTION F_TEST(PID INT) RETURNS VARCHAR\n" +
            "BEGIN\n" +
            "  DECLARE NAME_FOUND VARCHAR DEFAULT \"\";\n" +
            "\n" +
            "    SELECT EMPLOYEE_NAME INTO NAME_FOUND FROM TABLE_NAME WHERE ID = PID;\n" +
            "  RETURN NAME_FOUND;\n" +
            "END;//";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.MYSQL);
        stmt.accept(visitor);

        assertEquals("CREATE FUNCTION mysql.F_TEST (\n" +
                "\tPID INT\n" +
                ")\n" +
                "RETURNS VARCHAR\n" +
                "CONTAINS SQL\n" +
                "SQL SECURITY DEFINER\n" +
                "BEGIN\n" +
                "\tDECLARE NAME_FOUND VARCHAR DEFAULT '';\n" +
                "\tSELECT EMPLOYEE_NAME\n" +
                "\tINTO NAME_FOUND\n" +
                "\tFROM TABLE_NAME\n" +
                "\tWHERE ID = PID;\n" +
                "\tRETURN NAME_FOUND;\n" +
                "END;", //
            SQLUtils.toMySqlString(stmt));

        assertEquals("create function mysql.F_TEST (\n" +
                "\tPID INT\n" +
                ")\n" +
                "returns VARCHAR\n" +
                "contains sql\n" +
                "sql security definer\n" +
                "begin\n" +
                "\tdeclare NAME_FOUND VARCHAR default '';\n" +
                "\tselect EMPLOYEE_NAME\n" +
                "\tinto NAME_FOUND\n" +
                "\tfrom TABLE_NAME\n" +
                "\twhere ID = PID;\n" +
                "\treturn NAME_FOUND;\n" +
                "end;", //
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(3, visitor.getColumns().size());
        assertEquals(2, visitor.getConditions().size());

//        Assert.assertTrue(visitor.getTables().containsKey(new TableStat.Name("City")));
//        Assert.assertTrue(visitor.getTables().containsKey(new TableStat.Name("t2")));

//        Assert.assertTrue(visitor.getColumns().contains(new Column("t2", "id")));
    }

}
