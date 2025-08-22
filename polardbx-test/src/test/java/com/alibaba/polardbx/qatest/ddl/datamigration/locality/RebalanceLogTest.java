package com.alibaba.polardbx.qatest.ddl.datamigration.locality;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@NotThreadSafe
public class RebalanceLogTest extends LocalityTestBase {

    private static final Log logger = LogFactory.getLog(RebalanceLogTest.class);

    private final String databaseName1 = "test_rebalance_drds_db";
    private final String databaseName2 = "test_rebalance_auto_db";

    @After
    public void dropDb() {
    }

    @Test
    public void testRebalanceDrdsDb() throws InterruptedException {
        final String createTableSql1 =
            "create table t1 (a int) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        final String tableName1 = "t1";
        JdbcUtil.dropDatabase(tddlConnection, databaseName1);

        // run
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists " + databaseName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + databaseName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName1);

        // before

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql1);

        // check information_schema.locality_info

        // check show topology
        List<String> dnListOfDb = getDnListOfDb(databaseName1, true);
        List<String> actualDn = getDnListOfTable(databaseName1, tableName1);
        Assert.assertEquals(new HashSet<String>(dnListOfDb), new HashSet<String>(actualDn));

        // drop and check again
        final String dn = chooseDatanode(tddlConnection, false);
        final String drainNodeSql = String.format("rebalance database drain_node = '%s'", dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, drainNodeSql);

        final String querySql =
            String.format("select count(1) from metadb.ddl_engine where schema_name = '%s' and state != 'SUCCESS'",
                databaseName1);

        // wait for rebalance complete
        boolean waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance database drain_node for " + databaseName1);
        }

        List<String> afterDrainNodeDbList = getDnListOfDb(databaseName1, true);
        List<String> afterDrainNodeTableDnList = getDnListOfTable(databaseName1, tableName1);
        Assert.assertEquals("db and table dn list not equal after drain node",
            new HashSet<String>(afterDrainNodeDbList), new HashSet<String>(afterDrainNodeTableDnList));
        List<String> expectedDrainNodeDnList = new ArrayList<>(dnListOfDb);
        expectedDrainNodeDnList.remove(dn);
        Assert.assertEquals("expected dn after drain node not meet", new HashSet<String>(afterDrainNodeTableDnList),
            new HashSet<String>(expectedDrainNodeDnList));

        final String rebalanceSql = String.format("rebalance database");
        JdbcUtil.executeUpdateSuccess(tddlConnection, rebalanceSql);

        // wait for rebalance complete
        waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance database for " + databaseName1);
        }

        List<String> afterRebalanceDbList = getDnListOfDb(databaseName1, true);
        List<String> afterRebalanceTableList = getDnListOfTable(databaseName1, tableName1);
        Assert.assertEquals("db and table dn list not equal after rebalance",
            new HashSet<String>(afterRebalanceTableList), new HashSet<String>(afterRebalanceTableList));
        Assert.assertEquals("expected dn after rebalance not meet", new HashSet<String>(afterRebalanceDbList),
            new HashSet<String>(dnListOfDb));
    }

    @Test
    public void testRebalanceAutoDb() throws InterruptedException {
        final String createTableSql1 = "create table t1 (a int) partition by hash(a) partitions 16";
        final String tableName1 = "t1";
        JdbcUtil.dropDatabase(tddlConnection, databaseName2);

        // run
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + databaseName2 + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + databaseName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName1);

        // before

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql1);

        // check information_schema.locality_info

        // check show topology
        List<String> dnListOfDb = getDnListOfDb(databaseName2, true);
        List<String> actualDn = getDnListOfTable(databaseName2, tableName1);
        Assert.assertEquals(new HashSet<String>(dnListOfDb), new HashSet<String>(actualDn));

        // drop and check again
        final String dn = chooseDatanode(tddlConnection, false);
        final String drainNodeSql = String.format("rebalance database drain_node = '%s'", dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, drainNodeSql);

        final String querySql =
            String.format("select count(1) from metadb.ddl_engine where schema_name = '%s' and state != 'SUCCESS'",
                databaseName2);

        // wait for rebalance complete
        boolean waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance database drain_node for " + databaseName2);
        }

        List<String> afterDrainNodeDbList = getDnListOfDb(databaseName2, true);
        List<String> afterDrainNodeTableDnList = getDnListOfTable(databaseName2, tableName1);
        Assert.assertEquals("db and table dn list not equal after drain node",
            new HashSet<String>(afterDrainNodeDbList), new HashSet<String>(afterDrainNodeTableDnList));
        List<String> expectedDrainNodeDnList = new ArrayList<>(dnListOfDb);
        expectedDrainNodeDnList.remove(dn);
        Assert.assertEquals("expected dn after drain node not meet", new HashSet<String>(afterDrainNodeTableDnList),
            new HashSet<String>(expectedDrainNodeDnList));

        final String rebalanceSql = String.format("rebalance database");
        JdbcUtil.executeUpdateSuccess(tddlConnection, rebalanceSql);

        // wait for rebalance complete
        waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance database for " + databaseName2);
        }

        List<String> afterRebalanceDbList = getDnListOfDb(databaseName2, true);
        List<String> afterRebalanceTableList = getDnListOfTable(databaseName2, tableName1);
        Assert.assertEquals("db and table dn list not equal after rebalance",
            new HashSet<String>(afterRebalanceTableList), new HashSet<String>(afterRebalanceTableList));
        Assert.assertEquals("expected dn after rebalance not meet", new HashSet<String>(afterRebalanceDbList),
            new HashSet<String>(dnListOfDb));
    }
}
