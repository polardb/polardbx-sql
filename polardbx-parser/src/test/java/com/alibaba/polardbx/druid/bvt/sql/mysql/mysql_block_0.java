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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class mysql_block_0 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "BEGIN\n" +
            "DELETE t0 FROM ktv_ind_columns t0 WHERE t0.dbid=?;\n" +
            "    INSERT INTO ktv_ind_columns(index_owner,index_name,table_owner,TABLE_NAME,COLUMN_NAME,column_position,column_length,descend,dbId,collection_time)\n"
            +
            "    SELECT DISTINCT index_owner,index_name,table_owner,TABLE_NAME,COLUMN_NAME,column_position,column_length,descend,dbId,now()\n"
            +
            "    FROM ktv_tmp_ind_columns WHERE dbid=?;\n" +
            "    COMMIT;\n" +
            "END;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        assertEquals(1, statementList.size());
        SQLStatement stmt = statementList.get(0);

        SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.MYSQL);
        for (SQLStatement statement : statementList) {
            statement.accept(visitor);
        }

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("relationships : " + visitor.getRelationships());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(2, visitor.getTables().size());

//        Assert.assertTrue(visitor.getTables().containsKey(new TableStat.Name("employees")));
//        Assert.assertTrue(visitor.getTables().containsKey(new TableStat.Name("emp_name")));

//        Assert.assertEquals(7, visitor.getColumns().size());
//        Assert.assertEquals(3, visitor.getConditions().size());
//        Assert.assertEquals(1, visitor.getRelationships().size());

        // Assert.assertTrue(visitor.getColumns().contains(new TableStat.Column("employees", "salary")));

        {
            String output = SQLUtils.toMySqlString(stmt);
            assertEquals("BEGIN\n" +
                    "\tDELETE t0\n" +
                    "\tFROM ktv_ind_columns t0\n" +
                    "\tWHERE t0.dbid = ?;\n" +
                    "\tINSERT INTO ktv_ind_columns (index_owner, index_name, table_owner, TABLE_NAME, COLUMN_NAME\n" +
                    "\t\t, column_position, column_length, descend, dbId, collection_time)\n" +
                    "\tSELECT DISTINCT index_owner, index_name, table_owner, TABLE_NAME, COLUMN_NAME\n" +
                    "\t\t, column_position, column_length, descend, dbId, now()\n" +
                    "\tFROM ktv_tmp_ind_columns\n" +
                    "\tWHERE dbid = ?;\n" +
                    "\tCOMMIT;\n" +
                    "END;",
                output);
        }
        {
            String output = SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);
            assertEquals("begin\n" +
                    "\tdelete t0\n" +
                    "\tfrom ktv_ind_columns t0\n" +
                    "\twhere t0.dbid = ?;\n" +
                    "\tinsert into ktv_ind_columns (index_owner, index_name, table_owner, TABLE_NAME, COLUMN_NAME\n" +
                    "\t\t, column_position, column_length, descend, dbId, collection_time)\n" +
                    "\tselect distinct index_owner, index_name, table_owner, TABLE_NAME, COLUMN_NAME\n" +
                    "\t\t, column_position, column_length, descend, dbId, now()\n" +
                    "\tfrom ktv_tmp_ind_columns\n" +
                    "\twhere dbid = ?;\n" +
                    "\tcommit;\n" +
                    "end;",
                output);
        }
    }
}
