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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by wenshao on 18/07/2017.
 */
public class CreateCompareTest_cycle extends TestCase {
    public void test_0() throws Exception {
        String sql = "CREATE TABLE t0 (\n" +
                "\tint bigint\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE t1 (\n" +
                "\tint bigint\n" +
                ");\n" +
                "CREATE TABLE t2 (\n" +
                "\tint bigint,\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t1 (id)\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE t3 (\n" +
                "\tint bigint,\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t2 (id)\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE t4 (\n" +
                "\tint bigint,\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t3 (id),\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t4 (id)\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE t5 (\n" +
                "\tint bigint,\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t4 (id)\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE t6 (\n" +
                "\tint bigint,\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t5 (id)\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE t7 (\n" +
                "\tint bigint,\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t6 (id)\n" +
                ");\n" +
                "\n" +
                "\n" +
                "CREATE TABLE t8 (\n" +
                "\tint bigint,\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t7 (id)\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE t9 (\n" +
                "\tint bigint,\n" +
                "\tFOREIGN KEY (id)\n" +
                "\t\tREFERENCES t8 (id)\n" +
                ");";

        List stmtList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);


        SQLCreateTableStatement.sort(stmtList);

        String sortedSql = SQLUtils.toSQLString(stmtList, JdbcConstants.MYSQL);
        System.out.println(sortedSql);

        assertEquals("t0", ((SQLCreateTableStatement)stmtList.get(9)).getName().getSimpleName());
        assertEquals("t9", ((SQLCreateTableStatement)stmtList.get(8)).getName().getSimpleName());
        assertEquals("t8", ((SQLCreateTableStatement)stmtList.get(7)).getName().getSimpleName());
        assertEquals("t7", ((SQLCreateTableStatement)stmtList.get(6)).getName().getSimpleName());
        assertEquals("t6", ((SQLCreateTableStatement)stmtList.get(5)).getName().getSimpleName());
        assertEquals("t5", ((SQLCreateTableStatement)stmtList.get(4)).getName().getSimpleName());
        assertEquals("t4", ((SQLCreateTableStatement)stmtList.get(3)).getName().getSimpleName());
        assertEquals("t3", ((SQLCreateTableStatement)stmtList.get(2)).getName().getSimpleName());
        assertEquals("t2", ((SQLCreateTableStatement)stmtList.get(1)).getName().getSimpleName());
        assertEquals("t1", ((SQLCreateTableStatement)stmtList.get(0)).getName().getSimpleName());

        assertEquals("t4", ((SQLAlterTableStatement)stmtList.get(10)).getName().getSimpleName());

        assertEquals(11, stmtList.size());



    }
}
