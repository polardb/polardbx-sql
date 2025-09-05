package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.BooleanUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@NotThreadSafe
public class BroadcastTableRandomReadTest extends DDLBaseNewDBTestCase {
    private static final String dbName = "test_brd_random_read_db";
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

    private static final String selectBrdTable =
        "trace select * from " + tableNameBroadcast + " where id = {0} ";


    public static String getDataNode(Connection tddlConnection) {
        List<LocalityTestBase.StorageNodeBean> dnList = LocalityTestBase.getStorageInfo(tddlConnection);
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
        prepareData();
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
                .format(insertDataSql, tableNamePartition, i));
        }
    }

    @Test
    public void testRandomReadAfterRebalance() {

        JdbcUtil.useDb(tddlConnection, dbName);
        String storageName = getDataNode(tddlConnection);
        if (storageName == null) {
            return;
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

        Set<String> groupNames = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            JdbcUtil.executeQuery(MessageFormat.format(selectBrdTable, 1), tddlConnection);
            List<List<String>> trace = getTrace(tddlConnection);
            Assert.assertThat(trace.toString(), trace.size(), is(1));
            groupNames.add(trace.get(0).get(4));
        }
        Assert.assertThat(groupNames.toString(), groupNames.size(), greaterThan(1));
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
