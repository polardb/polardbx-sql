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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 事务控制指令测试
 */

public class TransactionCommandTest extends CrudBasedLockTestCase {

    public TransactionCommandTest() {
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
    }

    @Ignore
    public void testShowTrans() throws Exception {
        tddlConnection.setAutoCommit(false);
        //JdbcUtil.setTxPolicy(ITransactionPolicy.BEST_EFFORT, tddlConnection);

        String sql = "update " + baseOneTableName + " set integer_test=1";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        String expectedTransId;
        try (ResultSet rs = JdbcUtil.executeQuery("select CURRENT_TRANS_ID()", tddlConnection)) {
            Assert.assertTrue(rs.next());
            expectedTransId = rs.getString(1);
        }

        boolean found = false;
        try (ResultSet rs = JdbcUtil.executeQuery("show trans", tddlConnection)) {
            List<List<Object>> allResults = JdbcUtil.getAllResult(rs);
            for (List<Object> r : allResults) {
                if (expectedTransId.equals(r.get(0))) {
                    Assert.assertEquals("BEST_EFFORT", r.get(1));
                    found = true;
                }
            }
        }
        Assert.assertTrue("should see this transaction", found);

        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);
    }

    @Ignore
    public void testPurgeTrans() throws Exception {
        final String dbName = "test_purge_trans";
        final String tbName = "test_purge_trans_tb";
        try (final Connection tddlConnection2 = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(tddlConnection2, "drop database if exists " + dbName);
            JdbcUtil.executeUpdateSuccess(tddlConnection2, "create database " + dbName);

            try (final Connection tddlConnection3 = getPolardbxConnection(dbName)) {
                // Init data.
                String sql = "create table " + tbName
                    + "(id int primary key, a int, global index g_idx(a) dbpartition by hash(a))dbpartition by hash(id)";
                JdbcUtil.executeUpdateSuccess(tddlConnection3, sql);
                final int before = -60 * 60 * 24 * 3;
                // It should drop all partitions, and only the partition_unlimited left.
                System.out.println("Before purge before...");
                printTrxLogPartitions(tddlConnection3, tbName);
                JdbcUtil.executeUpdateSuccess(tddlConnection3, "PURGE TRANS BEFORE " + before);
                System.out.println("After purge before...");
                printTrxLogPartitions(tddlConnection3, tbName);
                // The following trans should be writen into the partition_unlimited.
                tddlConnection3.setAutoCommit(false);
                sql = "insert into " + tbName + " values (0,0), (1,1), (2,2), (3,3), (4,4), (5,5), (6,6)";
                JdbcUtil.executeSuccess(tddlConnection3, sql);
                tddlConnection3.commit();
                tddlConnection3.setAutoCommit(true);
                // It should split the partition_unlimited into 2 partitions: (-infinity, now] and (now, infinity)
                // And the above trans log should be split into the (-infinity, now] partition
                JdbcUtil.executeUpdateSuccess(tddlConnection3, "PURGE TRANS");
                System.out.println("After the 1st purge...");
                printTrxLogPartitions(tddlConnection3, tbName);

                final long beforeRows = getTrxLogRows(tddlConnection3, tbName);
                System.out.println(beforeRows);
                printTrxLogs(tddlConnection3, tbName);
                // It should drop the (-infinity, now] partition.
                JdbcUtil.executeUpdateSuccess(tddlConnection3, "PURGE TRANS");
                System.out.println("After the 2nd purge...");
                printTrxLogPartitions(tddlConnection3, tbName);
                final long afterRows = getTrxLogRows(tddlConnection3, tbName);
                printTrxLogs(tddlConnection3, tbName);
                System.out.println(afterRows);
                Assert.assertTrue("should purged some transactions", (beforeRows - afterRows) > 0);
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + dbName);
        }
    }

    private long getTrxLogRows(Connection tddlConnection2, String tbName) throws SQLException {
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection2, tbName);
        long ret = 0;
        for (Pair<String, String> physicalGroupAndTable : topology) {
            final String physicalGroup = physicalGroupAndTable.getKey();
            final String sql = "/*+TDDL:node(" + physicalGroup + ")*/select count(*) from __drds_global_tx_log";
            final ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection2);
            while (rs.next()) {
                ret += rs.getLong(1);
            }
        }
        return ret;
    }

    private void printTrxLogPartitions(Connection tddlConnection2, String tbName) throws SQLException {
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection2, tbName);
        final String physicalGroup = topology.get(0).getKey();
        final String sql = "/*+TDDL:node(" + physicalGroup + ")*/ show create table __drds_global_tx_log";
        final ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection2);
        System.out.println(sql);
        if (rs.next()) {
            System.out.println(rs.getString(2));
        }
    }

    private void printTrxLogs(Connection tddlConnection2, String tbName) throws SQLException {
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection2, tbName);
        for (Pair<String, String> physicalGroupAndTable : topology) {
            final String physicalGroup = physicalGroupAndTable.getKey();
            final String sql = "/*+TDDL:node(" + physicalGroup + ")*/ select TXID from __drds_global_tx_log";
            final ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection2);
            System.out.println(sql);
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }

}
