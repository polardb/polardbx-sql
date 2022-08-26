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

package com.alibaba.polardbx.qatest.dql.sharding;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author moyi
 */
public class BroadcastReadTest extends ReadBaseTestCase {

    private String testTableName = "broadcast_test";

    public String getExplainResult(Connection conn, String sql) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "explain " + sql);
        try {
            return JdbcUtil.resultsStr(rs);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    public void dropTableIfExists(String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testInTransactionRead() throws SQLException {
        if (usingNewPartDb()) {
            return;
        }

        String tableName = testTableName + "_in_txn";
        String normalTable = testTableName + "_dist";
        dropTableIfExists(tableName);
        dropTableIfExists(normalTable);

        String sql = "create table " + tableName + "(id int) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (usingNewPartDb()) {
            sql = "create table " + normalTable + "(id int) partition by key(id) partitions 3";
        } else {
            sql = "create table " + normalTable + "(id int) dbpartition by hash(id)";
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + normalTable + " (id) values (1), (2), (3), (4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try (Connection conn = getPolardbxDirectConnection()) {
            String explainSql = "select * from " + tableName + " where id = 1";
            String singleGroup = getGroupName(getExplainResult(conn,
                "/*+TDDL:enable_broadcast_random_read=false*/" + explainSql));

            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            JdbcUtil.setTxPolicy(ITransactionPolicy.XA, conn);

            /* transaction touched one group */
            sql = "insert into " + normalTable + " values(2)";
            JdbcUtil.executeUpdateSuccess(conn, sql);
            String expectedGroup = getGroupName(getExplainResult(conn, sql));
            String resultGroup = getGroupName(getExplainResult(conn, explainSql));
            Assert.assertTrue(expectedGroup.equals(resultGroup) || singleGroup.equals(resultGroup));
            conn.commit();

            /* transaction touched multi groups */
            Set<String> expectedGroups = new HashSet<>();
            expectedGroups.add(singleGroup);
            sql = "insert into " + normalTable + " values(1)";
            JdbcUtil.executeUpdateSuccess(conn, sql);
            expectedGroups.add(getGroupName(getExplainResult(conn, sql)));

            sql = "insert into " + normalTable + " values(2)";
            JdbcUtil.executeUpdateSuccess(conn, sql);
            expectedGroups.add(getGroupName(getExplainResult(conn, sql)));

            Assert.assertTrue(expectedGroups.contains(getGroupName(getExplainResult(conn, explainSql))));
            conn.commit();
        }
    }

    private String getGroupName(String explainResult) {
        int start = explainResult.indexOf("GROUP");
        if (start == -1) {
            return null;
        }
        return explainResult.substring(start - 2, start + 5);
    }

    /**
     * Read broadcast table should access random group, instead of the single-group
     */
    @Test
    public void testRandomRead() throws SQLException {

        if (usingNewPartDb()) {
            return;
        }

        String tableName = testTableName = "_rand";
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + "(id int) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String explainSql = "select * from " + tableName + " where id = 1";

        /* disable broadcast random read */
        try (Connection conn = getPolardbxDirectConnection()) {
            String expectedGroupName = null;
            for (int i = 0; i < 10; i++) {
                String result = getExplainResult(conn,
                    "/*+TDDL: enable_broadcast_random_read=false*/" + explainSql);
                String groupName = getGroupName(result);
                if (expectedGroupName == null) {
                    expectedGroupName = groupName;
                }
                Assert.assertEquals(expectedGroupName, groupName);
            }
        }

        /* enable broadcast random read */
        Set<String> accessedGroups = new HashSet<>();
        try (Connection conn = getPolardbxDirectConnection()) {
            for (int i = 0; i < 1000; i++) {
                String result = getExplainResult(conn, explainSql);
                String groupName = getGroupName(result);
                accessedGroups.add(groupName);

                if (usingNewPartDb()) {
                    /* all groups are accessed */
                    if (accessedGroups.size() == 2) {
                        return;
                    }
                } else {
                    /* all groups are accessed */
                    if (accessedGroups.size() == 4) {
                        return;
                    }
                }

            }
            Assert.fail("All group should be accessed after 1000 iterations ");
        }
    }

}
