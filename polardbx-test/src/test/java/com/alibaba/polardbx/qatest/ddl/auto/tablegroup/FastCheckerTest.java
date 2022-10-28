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

package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalityTestUtils;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.BooleanUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.alibaba.polardbx.qatest.util.JdbcUtil;

import javax.validation.constraints.AssertFalse;

import static com.google.common.truth.Truth.assertWithMessage;

@NotThreadSafe
public class FastCheckerTest extends DDLBaseNewDBTestCase {
    private static final String dbName = "test_fastchecker_auto_db";
    private static final String tableNameBroadcast = "test_broadcast_tbl";
    private static final String tableNameSingle = "test_single_tbl";
    private static final String tableNamePartition = "test_partition_tbl";
    private static final String dropDbSql = "drop database if exists {0};";
    private static final String createDbSql = "create database {0} mode=\'auto\';";
    private static final String useSql = "use {0};";
    private static final String createBroadcastSql = "create table {0} (\n"
        + " id int(11) NOT NULL,\n"
        + " name varchar(50),\n"
        + " primary key(id) )\n"
        + " broadcast;";
    private static final String createSingleSql = "create table {0} (\n"
        + " id int(11) NOT NULL,\n"
        + " name varchar(50),\n"
        + " primary key(id) )\n"
        + " single;";
    private static final String createPartitionSql = "create table {0} (\n"
        + " id int(11) NOT NULL,\n"
        + " name varchar(50),\n"
        + " primary key(id) )\n"
        + " partition by key(id);";
    private static final String dropTbSql = "drop table if exists {0};";
    private static final String insertDataSql = "insert into {0} values({1}, ''abcd'');";
    private static final String drainNodeSql = "rebalance database drain_node={0} async=false";
    private static final String rebalanceSql = "rebalance database async=false";

    private static final String checkDdlEngineTaskSql =
        "select * from metadb.ddl_engine_task where job_id={0} and state != ''SUCCESS'';";

    public static class StorageNodeBean {
        public String instance;
        public String status;
        public String instKind;
        public boolean deletable;
        public Map<String, LocalityTestBase.ReplicaBean> replicas;

    }

    public static List<LocalityTestUtils.StorageNodeBean> getStorageInfo(Connection tddlConnection) {
        final String showStorageSql = "SHOW STORAGE REPLICAS";
        // STORAGE_INST_ID | polardbx-storage-1-master
        // LEADER_NODE     | 100.82.20.151:3318
        // IS_HEALTHY      | true
        // INST_KIND       | MASTER
        // DB_COUNT        | 3
        // GROUP_COUNT     | 5
        // STATUS          | 0
        // DELETABLE       | 1
        // REPLICAS        | LEADER/100.82.20.151:3318/az2,FOLLOWER(100.82.20.151:3308)(az1),FOLLOWER(100.82.20.151:3328)(az3)

        List<LocalityTestUtils.StorageNodeBean> res = new ArrayList<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, showStorageSql)) {
            while (result.next()) {
                String instance = result.getString(1);
                String instKind = result.getString(4);
                String status = result.getString(7);
                boolean deletable = BooleanUtils.toBoolean(result.getString(8));
                String replicaStr = result.getString(11);
                LocalityTestUtils.StorageNodeBean
                    storageNode =
                    new LocalityTestUtils.StorageNodeBean(instance, instKind, status, deletable, replicaStr);
                res.add(storageNode);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
//        LOG.info("getStorageInfo: " + res);
        return res;
    }

    public static String getDataNode(Connection tddlConnection) {
        List<LocalityTestUtils.StorageNodeBean> dnList = getStorageInfo(tddlConnection);
        List<String> names = dnList.stream()
            .filter(x -> "MASTER".equals(x.instKind))
            .filter(x -> x.deletable == true)
            .map(x -> x.instance)
            .collect(Collectors.toList());
        if (!names.isEmpty()) {
            return names.get(0);
        }
        return null;
    }

    @Before
    public void prepareDb() {
        //create database
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, dbName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createDbSql, dbName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(useSql, dbName));
    }

    @After
    public void dropDb() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, dbName));
    }

    private void prepareData() {
        //create tables in current db(auto mode)
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropTbSql, tableNameBroadcast));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropTbSql, tableNameSingle));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropTbSql, tableNamePartition));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createBroadcastSql, tableNameBroadcast));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createSingleSql, tableNameSingle));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createPartitionSql, tableNamePartition));

        //insert some data
        for (int i = 0; i < 10; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(insertDataSql, tableNameBroadcast, i));
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(insertDataSql, tableNameSingle, i));
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(insertDataSql, tableNameBroadcast, i));
        }
    }

    @Test
    public void testFastChecker() {

        String storageName = getDataNode(tddlConnection);
        if (storageName == null) {
            Assert.fail("no drainable data node");
        }
        //drain node
        String jobId = null;
        ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, MessageFormat.format(drainNodeSql, "'" + storageName + "'"));
        try {
            while (rs.next()) {
                jobId = rs.getString("JOB_ID");
            }
        } catch (SQLException ex) {
            assertWithMessage("语句并未按照预期执行成功:" + ex.getMessage()).fail();
        }
        if (jobId == null) {
            Assert.fail("rebalance drain node failed");
        }
        waitUntilDdlJobSucceed(jobId);
        //rebalance
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, rebalanceSql);
        try {
            while (rs.next()) {
                jobId = rs.getString("JOB_ID");
            }
        } catch (SQLException ex) {
            assertWithMessage("语句并未按照预期执行成功:" + ex.getMessage()).fail();
        }
        if (jobId == null) {
            Assert.fail("rebalance drain node failed");
        }
        waitUntilDdlJobSucceed(jobId);
    }

    private void waitUntilDdlJobSucceed(String jobId) {
        boolean succeed = false;
        while (!succeed) {
            ResultSet rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, MessageFormat.format(checkDdlEngineTaskSql, jobId));
            int unfinishedDdlTaskCount = 0;
            try {
                while (rs.next()) {
                    rs.getString(0);
                    unfinishedDdlTaskCount++;
                }
            } catch (SQLException e) {
                succeed = false;
                continue;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (unfinishedDdlTaskCount == 0) {
                succeed = true;
            }
        }
    }
}
