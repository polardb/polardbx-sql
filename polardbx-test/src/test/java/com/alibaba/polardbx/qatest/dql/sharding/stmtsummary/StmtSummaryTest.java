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

package com.alibaba.polardbx.qatest.dql.sharding.stmtsummary;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * @author busu
 * date: 2021/12/2 10:50 上午
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StmtSummaryTest extends ReadBaseTestCase {

    final static String STMT_SUMMARY_TEST_TABLE = "stmt_summary_test_tb";

    final static String ONE_NODE_HINT = "/!+TDDL:cmd_extra(ENABLE_REMOTE_SYNC_ACTION=false)*/ ";

    final static String STMT_SUMMARY_QUERY_CURRENT = "select * from information_schema.statements_summary";

    final static String STMT_SUMMARY_QUERY_HISTORY = "select * from information_schema.statements_summary_history";

    final static String instanceId = PropertiesUtil.configProp.getProperty("instanceId");

    static volatile boolean tableCreated = false;

    static volatile Runnable runnable = null;

    @Before
    public void before() throws Exception {

        if (!tableCreated) {
            tableCreated = true;
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute(
                    "/!+TDDL:cmd_extra(PURE_ASYNC_DDL_MODE=false,PURE_ASYNC_DDL_MODE=false)*/ create table if not exists "
                        + STMT_SUMMARY_TEST_TABLE
                        + "(id int not null, myname char(50) not null, primary key(id))");
            }
            try (Connection metaDbConn = getMetaConnection();
                Statement stmt = metaDbConn.createStatement()) {
                stmt.execute("begin");
                stmt.executeUpdate(
                    String.format(
                        "delete from inst_config where inst_id = '%s' and param_key = 'STATEMENTS_SUMMARY_RECORD_INTERNAL'",
                        instanceId));
                stmt.executeUpdate(String.format(
                    "insert into inst_config(inst_id,param_key,param_val) values('%s','STATEMENTS_SUMMARY_RECORD_INTERNAL','true')",
                    instanceId));
                stmt.executeUpdate(String.format(
                    "delete from inst_config where inst_id = '%s' and param_key = 'STATEMENTS_SUMMARY_PERCENT'",
                    instanceId));
                stmt.executeUpdate(String.format(
                    "insert into inst_config(inst_id,param_key,param_val) values('%s','STATEMENTS_SUMMARY_PERCENT','100')",
                    instanceId));
                stmt.executeUpdate(String.format(
                    "delete from inst_config where inst_id = '%s' and param_key = 'STATEMENTS_SUMMARY_PERIOD_SEC'",
                    instanceId));
                stmt.executeUpdate(String.format(
                    "insert into inst_config(inst_id,param_key,param_val) values('%s','STATEMENTS_SUMMARY_PERIOD_SEC','1800000')",
                    instanceId));
                stmt.executeUpdate(String.format(
                    "update config_listener set op_version = op_version + 1 where data_id = 'polardbx.inst.config.%s'",
                    instanceId));
                stmt.execute("commit");
                Thread.sleep(6000);
            }
        }

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("delete from " + STMT_SUMMARY_TEST_TABLE);
        }
    }

    @AfterClass
    public static void afterClass() {
        if (runnable != null) {
            runnable.run();
        }
    }

    @After
    public void after() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("delete from " + STMT_SUMMARY_TEST_TABLE);
        }

        runnable = () -> {
            try {
                try (Connection metaDbConn = getMetaConnection();
                    Statement stmt = metaDbConn.createStatement()) {
                    stmt.execute("begin");
                    stmt.executeUpdate(
                        String.format(
                            "delete from inst_config where inst_id = '%s' and param_key = 'STATEMENTS_SUMMARY_PERIOD_SEC'",
                            instanceId));
                    stmt.executeUpdate(
                        String.format(
                            "delete from inst_config where inst_id = '%s' and param_key = 'STATEMENTS_SUMMARY_RECORD_INTERNAL'",
                            instanceId));
                    stmt.executeUpdate(
                        String.format(
                            "update  inst_config where inst_id = '%s' and param_key = 'STATEMENTS_SUMMARY_PERIOD_SEC' set param_val='1800000'",
                            instanceId));
                    stmt.executeUpdate(String.format(
                        "update config_listener set op_version = op_version + 1 where data_id = 'polardbx.inst.config.%s'",
                        instanceId));
                    stmt.execute("commit");
                }

                try (Connection tddlConn = getPolardbxConnection(); Statement stmt = tddlConn.createStatement()) {
                    stmt.execute(
                        "/!+TDDL:cmd_extra(PURE_ASYNC_DDL_MODE=false,PURE_ASYNC_DDL_MODE=false)*/drop table if  exists "
                            + STMT_SUMMARY_TEST_TABLE);
                }
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        };

    }

    @Test
    public void test1NormalStatement() throws Exception {
        final boolean[] pass = {false};
        //try 3 times, because current statement summary could become a history one at some time
        int idCount = 1;
        for (int i = 0; i < 3 && !pass[0]; ++i) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.executeUpdate("insert into " + STMT_SUMMARY_TEST_TABLE + "(id,myname) values("
                    + (idCount++) + ",'busu')");
                stmt.executeUpdate("insert into " + STMT_SUMMARY_TEST_TABLE + "(id,myname) values("
                    + (idCount++) + ",'busu')");
                stmt.executeUpdate(
                    "insert into " + STMT_SUMMARY_TEST_TABLE + "(id,myname) values(" + (idCount++) + ",'busu')");
            }
            List<Map<String, Object>> stmtSummaryResult = getStmtSummaryResult(STMT_SUMMARY_QUERY_CURRENT);
            stmtSummaryResult.forEach((e) -> {
                String sqlTemplate = String.valueOf(e.get("SQL_TEMPLATE")).toUpperCase();
                if (sqlTemplate.contains("INSERT") && sqlTemplate.contains(STMT_SUMMARY_TEST_TABLE.toUpperCase())
                    && sqlTemplate.contains("VALUES")) {
                    int count = Integer.parseInt(String.valueOf(e.get("COUNT")));
                    pass[0] = count >= 3;
                }
            });
        }
        Assert.assertTrue(pass[0]);
    }

    @Test
    public void test2CommitStatement() throws Exception {
        final boolean[] pass = {false};
        int idCount = 1;
        for (int i = 0; i < 3 && !pass[0]; ++i) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("begin");
                stmt.executeUpdate("insert into " + STMT_SUMMARY_TEST_TABLE + "(id,myname) values(" + idCount
                    + ",'busu')");
                stmt.executeUpdate("update " + STMT_SUMMARY_TEST_TABLE + " set myname = 'dingfeng' where id = "
                    + idCount);
                stmt.execute("commit");
                idCount++;
            }
            List<Map<String, Object>> stmtSummaryResult = getStmtSummaryResult(STMT_SUMMARY_QUERY_CURRENT);
            stmtSummaryResult.forEach((e) -> {
                String sqlTemplate = String.valueOf(e.get("SQL_TEMPLATE")).toUpperCase();
                String prevSqlTemplate = String.valueOf(e.get("PREV_SQL_TEMPLATE")).toUpperCase();
                if (sqlTemplate.contains("COMMIT") && prevSqlTemplate.contains("UPDATE") && prevSqlTemplate
                    .contains("MYNAME")) {
                    int count = Integer.parseInt(String.valueOf(e.get("COUNT")));
                    pass[0] = count >= 1;
                }
            });
        }
        Assert.assertTrue(pass[0]);
    }

    @Test
    public void test3RollbackStatement() throws Exception {
        final boolean[] pass = {false};
        int idCount = 1;
        for (int i = 0; i < 3 && !pass[0]; ++i) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("begin");
                stmt.executeUpdate("insert into " + STMT_SUMMARY_TEST_TABLE + "(id,myname) values(" + idCount
                    + ",'busu')");
                stmt.executeUpdate("update " + STMT_SUMMARY_TEST_TABLE + " set myname = 'dingfeng' where id = "
                    + idCount);
                idCount++;
                stmt.executeUpdate("insert into " + STMT_SUMMARY_TEST_TABLE + "(id,myname) values(" + idCount
                    + ",'dingfeng')");
                stmt.execute("rollback");
                idCount++;
            }
            List<Map<String, Object>> stmtSummaryResult = getStmtSummaryResult(STMT_SUMMARY_QUERY_CURRENT);
            stmtSummaryResult.forEach((e) -> {
                String sqlTemplate = String.valueOf(e.get("SQL_TEMPLATE")).toUpperCase();
                String prevSqlTemplate = String.valueOf(e.get("PREV_SQL_TEMPLATE")).toUpperCase();
                if (sqlTemplate.contains("ROLLBACK") && prevSqlTemplate.contains("INSERT") && prevSqlTemplate
                    .contains("MYNAME")) {
                    int count = Integer.parseInt(String.valueOf(e.get("COUNT")));
                    pass[0] = count >= 1;
                }
            });
        }
        Assert.assertTrue(pass[0]);
    }

    @Test
    public void test4OneNodeCurrentStatementSummary() throws Exception {
        final boolean[] pass = {false};
        //try 3 times, because current statement summary could become a history one at some time.
        int idCount = 1;
        for (int i = 0; i < 3 && !pass[0]; ++i) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.executeUpdate(String
                    .format("insert into " + STMT_SUMMARY_TEST_TABLE
                        + "(id,myname) values(%d,'busu')", idCount++));
                stmt.executeUpdate(String
                    .format("insert into " + STMT_SUMMARY_TEST_TABLE
                        + "(id,myname) values(%d,'busu')", idCount++));
                stmt.executeUpdate(String
                    .format("insert into " + STMT_SUMMARY_TEST_TABLE
                        + "(id,myname) values(%d,'busu')", idCount++));
            }
            List<Map<String, Object>> stmtSummaryResult =
                getStmtSummaryResult(ONE_NODE_HINT + STMT_SUMMARY_QUERY_CURRENT);
            stmtSummaryResult.forEach((e) -> {
                String sqlTemplate = String.valueOf(e.get("SQL_TEMPLATE")).toUpperCase();
                if (sqlTemplate.contains("INSERT") && sqlTemplate.contains(STMT_SUMMARY_TEST_TABLE.toUpperCase())
                    && sqlTemplate
                    .contains("VALUES")) {
                    int count = Integer.parseInt(String.valueOf(e.get("COUNT")));
                    pass[0] = count >= 3;
                }
            });
        }
        Assert.assertTrue(pass[0]);
    }

    @Test
    public void test5StmtSummaryHistory() throws Exception {
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement()) {
            stmt.execute("begin");
            int count = stmt.executeUpdate(
                String.format(
                    "delete from inst_config where inst_id = '%s' and param_key = 'STATEMENTS_SUMMARY_PERIOD_SEC'",
                    instanceId));
            Assert.assertTrue(count <= 1);
            stmt.executeUpdate(String.format(
                "insert into inst_config(inst_id,param_key,param_val) values('%s','STATEMENTS_SUMMARY_PERIOD_SEC','1')",
                instanceId));
            stmt.executeUpdate(String.format(
                "update config_listener set op_version = op_version + 1 where data_id = 'polardbx.inst.config.%s'",
                instanceId));
            stmt.execute("commit");
        }
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select  myname from " + STMT_SUMMARY_TEST_TABLE);
        }
        final boolean[] result = {false};
        for (int i = 0; i < 20 && !result[0]; ++i) {
            List<Map<String, Object>> stmtSummaryResult = getStmtSummaryResult(STMT_SUMMARY_QUERY_HISTORY);
            stmtSummaryResult.forEach((e) -> {
                String sqlTemplate = String.valueOf(e.get("SQL_TEMPLATE")).toUpperCase();
                if (sqlTemplate.contains("SELECT") && sqlTemplate.contains("MYNAME") && sqlTemplate.contains("FROM")
                    && sqlTemplate.contains(STMT_SUMMARY_TEST_TABLE.toUpperCase())) {
                    result[0] = true;
                }
            });
            Thread.sleep(500);
        }
        Assert.assertTrue(result[0]);
    }

    private List<Map<String, Object>> getStmtSummaryResult(String sql) throws SQLException {
        List<Map<String, Object>> result = Lists.newArrayList();
        try (Statement stmt = tddlConnection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            while (resultSet.next()) {
                Map<String, Object> oneLine = Maps.newHashMap();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); ++i) {
                    oneLine.put(resultSetMetaData.getColumnName(i), resultSet.getObject(i));
                }
                result.add(oneLine);
            }
        }
        return result;
    }
}
