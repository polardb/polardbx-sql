package com.alibaba.polardbx.qatest.dql.auto.agg;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class PartialAggTest extends PartitionTestBase {
    static final String DATABASE_1 = "test_partial_agg_db";

    static final String TABLE_NAME = "test_partial_agg";

    @Before
    public void prepare() {
        dropDatabase();
        createDatabase();
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format("use %s", DATABASE_1));
        dropTableIfExists(TABLE_NAME);
        createTable();
        insertData();
    }

    public void testPartialAgg(boolean enableLocalBuffer) {
        String sql =
            "/*+TDDL:cmd_extra(PREFER_PARTIAL_AGG=true MPP_TASK_LOCAL_BUFFER_ENABLED = %s enable_master_mpp=false PARALLELISM=5 workload_type=ap enable_push_sort=false enable_push_agg=false enable_cbo_push_agg=false)*/ "
                + " select count(*), c1 from %s group by c1";
        JdbcUtil.executeSuccess(tddlConnection, "use " + DATABASE_1);
        checkPlanUsePartialAgg(String.format(sql, enableLocalBuffer, TABLE_NAME));
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sql, enableLocalBuffer, TABLE_NAME));
    }

    @Test
    @Ignore("wait for PREFER_PARTIAL_AGG work under row mode")
    public void testDisableLocalBuffer() {
        testPartialAgg(false);
    }

    @Test
    @Ignore("wait for PREFER_PARTIAL_AGG work under row mode")
    public void testEnableLocalBuffer() {
        testPartialAgg(true);
    }

    private void checkPlanUsePartialAgg(String sql) {
        String explainResult =
            getExplainResult(tddlConnection, String.format(sql, DATABASE_1, TABLE_NAME)).toLowerCase();
        Assert.assertTrue(explainResult.contains("partialhashagg"),
            "plan should be partial agg");
    }

    @After
    public void dropDatabase() {
        String dropDatabase = "drop database if exists %s";
        JdbcUtil.executeSuccess(tddlConnection, String.format(dropDatabase, DATABASE_1));
    }

    private void createDatabase() {
        String createDatabase = "create database %s mode = 'auto'";
        JdbcUtil.executeSuccess(tddlConnection, String.format(createDatabase, DATABASE_1));
    }

    private void createTable() {
        String createTable = "CREATE PARTITION TABLE `%s` (\n"
            + "\t`c1` int(11) NOT NULL,\n"
            + "\t`c2` int(11) DEFAULT NULL,\n"
            + "\t`c3` int(11) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`c1`),\n"
            + "\tLOCAL KEY `k_c1` (`c1`),\n"
            + "\tLOCAL KEY `k_c2` (`c2`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c1`)\n"
            + "PARTITIONS 3";
        JdbcUtil.executeSuccess(tddlConnection, String.format(createTable, TABLE_NAME));
    }

    private void insertData() {
        String insertData =
            "insert into %s values (%s,%s,%s);";
        // insert 1000 rows
        for (int i = 0; i < 1000; ++i) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(insertData, TABLE_NAME, i, i, i));
        }
    }
}
