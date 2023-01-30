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

public class mysql_block_2 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "BEGIN\n" +
            "    DELETE t0 FROM ktv_tmp_sqlarea t0 WHERE t0.dbid=?;\n" +
            "    INSERT INTO ktv_tmp_sqlarea(`dbid`,`sql_id`,`parsing_schema_name`,`sql_fulltext`,`cpu_time`,`buffer_gets`,`executions`,`command_name`,`sharable_mem`,`persiste\n"
            +
            "nt_mem`,`users_opening`,`fetches`,`loads`,`disk_reads`,`direct_writes`,`command_type`,`plan_hash_value`,`action`,`remote`,`is_obsolete`,`physical_read_requests`,`\n"
            +
            "physical_write_requests`,`elapsed_time`,`user_io_wait_time`,`collection_time`)\n" +
            "    SELECT `dbid`,`sql_id`,`parsing_schema_name`,`sql_fulltext`,sum(`cpu_time`),sum(`buffer_gets`),sum(`executions`),max(`command_name`),sum(`sharable_mem`),sum(`\n"
            +
            "persistent_mem`),sum(`users_opening`),sum(`fetches`),sum(`loads`),sum(`disk_reads`),sum(`direct_writes`),max(`command_type`),max(`plan_hash_value`),max(`action`),\n"
            +
            "max(`remote`),max(`is_obsolete`),sum(`physical_read_requests`),sum(`physical_write_requests`),sum(`elapsed_time`),sum(`user_io_wait_time`),max(`collection_time`)\n"
            +
            "    FROM ktv_sqlarea WHERE dbid=? GROUP BY sql_fulltext;\n" +
            "    DELETE FROM ktv_sqlarea WHERE dbid=?;\n" +
            "    INSERT INTO ktv_sqlarea(`dbid`,`sql_id`,`parsing_schema_name`,`sql_fulltext`,`cpu_time`,`buffer_gets`,`executions`,`command_name`,`sharable_mem`,`persistent_m\n"
            +
            "em`,`users_opening`,`fetches`,`loads`,`disk_reads`,`direct_writes`,`command_type`,`plan_hash_value`,`action`,`remote`,`is_obsolete`,`physical_read_requests`,`phys\n"
            +
            "ical_write_requests`,`elapsed_time`,`user_io_wait_time`,`collection_time`)\n" +
            "    SELECT `dbid`,`sql_id`,`parsing_schema_name`,`sql_fulltext`,`cpu_time`,`buffer_gets`,`executions`,`command_name`,`sharable_mem`,`persistent_mem`,`users_openin\n"
            +
            "g`,`fetches`,`loads`,`disk_reads`,`direct_writes`,`command_type`,`plan_hash_value`,`action`,`remote`,`is_obsolete`,`physical_read_requests`,`physical_write_reques\n"
            +
            "ts`,`elapsed_time`,`user_io_wait_time`,`collection_time`\n" +
            "    FROM ktv_tmp_sqlarea WHERE dbid=? and sql_fulltext is not null;\n" +
            "    ROLLBACK;\n" +
            "    DELETE FROM ktv_tmp_sqlarea WHERE dbid=?;" +
            "    END";

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
                    "\tFROM ktv_tmp_sqlarea t0\n" +
                    "\tWHERE t0.dbid = ?;\n" +
                    "\tINSERT INTO ktv_tmp_sqlarea (`dbid`, `sql_id`, `parsing_schema_name`, `sql_fulltext`, `cpu_time`\n" +
                    "\t\t, `buffer_gets`, `executions`, `command_name`, `sharable_mem`, `persiste\n" +
                    "nt_mem`\n" +
                    "\t\t, `users_opening`, `fetches`, `loads`, `disk_reads`, `direct_writes`\n" +
                    "\t\t, `command_type`, `plan_hash_value`, `action`, `remote`, `is_obsolete`\n" +
                    "\t\t, `physical_read_requests`, `\n" +
                    "physical_write_requests`, `elapsed_time`, `user_io_wait_time`, `collection_time`)\n" +
                    "\tSELECT `dbid`, `sql_id`, `parsing_schema_name`, `sql_fulltext`\n" +
                    "\t\t, sum(`cpu_time`), sum(`buffer_gets`)\n" +
                    "\t\t, sum(`executions`), max(`command_name`)\n" +
                    "\t\t, sum(`sharable_mem`), sum(`\n" +
                    "persistent_mem`)\n" +
                    "\t\t, sum(`users_opening`), sum(`fetches`)\n" +
                    "\t\t, sum(`loads`), sum(`disk_reads`)\n" +
                    "\t\t, sum(`direct_writes`), max(`command_type`)\n" +
                    "\t\t, max(`plan_hash_value`), max(`action`)\n" +
                    "\t\t, max(`remote`), max(`is_obsolete`)\n" +
                    "\t\t, sum(`physical_read_requests`), sum(`physical_write_requests`)\n" +
                    "\t\t, sum(`elapsed_time`), sum(`user_io_wait_time`)\n" +
                    "\t\t, max(`collection_time`)\n" +
                    "\tFROM ktv_sqlarea\n" +
                    "\tWHERE dbid = ?\n" +
                    "\tGROUP BY sql_fulltext;\n" +
                    "\tDELETE FROM ktv_sqlarea\n" +
                    "\tWHERE dbid = ?;\n" +
                    "\tINSERT INTO ktv_sqlarea (`dbid`, `sql_id`, `parsing_schema_name`, `sql_fulltext`, `cpu_time`\n" +
                    "\t\t, `buffer_gets`, `executions`, `command_name`, `sharable_mem`, `persistent_m\n" +
                    "em`\n" +
                    "\t\t, `users_opening`, `fetches`, `loads`, `disk_reads`, `direct_writes`\n" +
                    "\t\t, `command_type`, `plan_hash_value`, `action`, `remote`, `is_obsolete`\n" +
                    "\t\t, `physical_read_requests`, `phys\n" +
                    "ical_write_requests`, `elapsed_time`, `user_io_wait_time`, `collection_time`)\n" +
                    "\tSELECT `dbid`, `sql_id`, `parsing_schema_name`, `sql_fulltext`, `cpu_time`\n" +
                    "\t\t, `buffer_gets`, `executions`, `command_name`, `sharable_mem`, `persistent_mem`\n" +
                    "\t\t, `users_openin\n" +
                    "g`, `fetches`, `loads`, `disk_reads`, `direct_writes`\n" +
                    "\t\t, `command_type`, `plan_hash_value`, `action`, `remote`, `is_obsolete`\n" +
                    "\t\t, `physical_read_requests`, `physical_write_reques\n" +
                    "ts`, `elapsed_time`, `user_io_wait_time`, `collection_time`\n" +
                    "\tFROM ktv_tmp_sqlarea\n" +
                    "\tWHERE dbid = ?\n" +
                    "\t\tAND sql_fulltext IS NOT NULL;\n" +
                    "\tROLLBACK;\n" +
                    "\tDELETE FROM ktv_tmp_sqlarea\n"
                    + "\tWHERE dbid = ?;\n" +
                    "END;",
                output);
        }
        {
            String output = SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);
            assertEquals("begin\n" +
                    "\tdelete t0\n" +
                    "\tfrom ktv_tmp_sqlarea t0\n" +
                    "\twhere t0.dbid = ?;\n" +
                    "\tinsert into ktv_tmp_sqlarea (`dbid`, `sql_id`, `parsing_schema_name`, `sql_fulltext`, `cpu_time`\n" +
                    "\t\t, `buffer_gets`, `executions`, `command_name`, `sharable_mem`, `persiste\n" +
                    "nt_mem`\n" +
                    "\t\t, `users_opening`, `fetches`, `loads`, `disk_reads`, `direct_writes`\n" +
                    "\t\t, `command_type`, `plan_hash_value`, `action`, `remote`, `is_obsolete`\n" +
                    "\t\t, `physical_read_requests`, `\n" +
                    "physical_write_requests`, `elapsed_time`, `user_io_wait_time`, `collection_time`)\n" +
                    "\tselect `dbid`, `sql_id`, `parsing_schema_name`, `sql_fulltext`\n" +
                    "\t\t, sum(`cpu_time`), sum(`buffer_gets`)\n" +
                    "\t\t, sum(`executions`), max(`command_name`)\n" +
                    "\t\t, sum(`sharable_mem`), sum(`\n" +
                    "persistent_mem`)\n" +
                    "\t\t, sum(`users_opening`), sum(`fetches`)\n" +
                    "\t\t, sum(`loads`), sum(`disk_reads`)\n" +
                    "\t\t, sum(`direct_writes`), max(`command_type`)\n" +
                    "\t\t, max(`plan_hash_value`), max(`action`)\n" +
                    "\t\t, max(`remote`), max(`is_obsolete`)\n" +
                    "\t\t, sum(`physical_read_requests`), sum(`physical_write_requests`)\n" +
                    "\t\t, sum(`elapsed_time`), sum(`user_io_wait_time`)\n" +
                    "\t\t, max(`collection_time`)\n" +
                    "\tfrom ktv_sqlarea\n" +
                    "\twhere dbid = ?\n" +
                    "\tgroup by sql_fulltext;\n" +
                    "\tdelete from ktv_sqlarea\n" +
                    "\twhere dbid = ?;\n" +
                    "\tinsert into ktv_sqlarea (`dbid`, `sql_id`, `parsing_schema_name`, `sql_fulltext`, `cpu_time`\n" +
                    "\t\t, `buffer_gets`, `executions`, `command_name`, `sharable_mem`, `persistent_m\n" +
                    "em`\n" +
                    "\t\t, `users_opening`, `fetches`, `loads`, `disk_reads`, `direct_writes`\n" +
                    "\t\t, `command_type`, `plan_hash_value`, `action`, `remote`, `is_obsolete`\n" +
                    "\t\t, `physical_read_requests`, `phys\n" +
                    "ical_write_requests`, `elapsed_time`, `user_io_wait_time`, `collection_time`)\n" +
                    "\tselect `dbid`, `sql_id`, `parsing_schema_name`, `sql_fulltext`, `cpu_time`\n" +
                    "\t\t, `buffer_gets`, `executions`, `command_name`, `sharable_mem`, `persistent_mem`\n" +
                    "\t\t, `users_openin\n" +
                    "g`, `fetches`, `loads`, `disk_reads`, `direct_writes`\n" +
                    "\t\t, `command_type`, `plan_hash_value`, `action`, `remote`, `is_obsolete`\n" +
                    "\t\t, `physical_read_requests`, `physical_write_reques\n" +
                    "ts`, `elapsed_time`, `user_io_wait_time`, `collection_time`\n" +
                    "\tfrom ktv_tmp_sqlarea\n" +
                    "\twhere dbid = ?\n" +
                    "\t\tand sql_fulltext is not null;\n" +
                    "\trollback;\n" +
                    "\tdelete from ktv_tmp_sqlarea\n"
                    + "\twhere dbid = ?;\n"
                    + "end;",
                output);
        }
    }
}
