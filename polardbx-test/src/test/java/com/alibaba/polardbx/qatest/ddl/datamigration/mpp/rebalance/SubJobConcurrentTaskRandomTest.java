package com.alibaba.polardbx.qatest.ddl.datamigration.mpp.rebalance;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.ddl.datamigration.mpp.pkrange.PkTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.ddl.datamigration.mpp.rebalance.SubJobConcurrentTaskTest.concurrentTaskSubJobTest;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkTableStatus;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.generateRandomMatchCreateTableAndGsiSql;

@NotThreadSafe
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class SubJobConcurrentTaskRandomTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PkTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public SubJobConcurrentTaskRandomTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("empty_table", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    @Ignore
    public void testSubJobConcurrentTaskRandom4() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_random_test4";

        int tableNums = 256;
        int maxGsiNum = 6;
        // random tablegroup match
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        if (!checkTableStatus(tddlConnection, -1L, "t0")) {
            List<String> ddls = generateRandomMatchCreateTableAndGsiSql(tableNums, maxGsiNum);
            logger.info(String.format(" create %d tables, %d gsi", tableNums, ddls.size() - tableNums));
            logger.info(String.format(" the ddl is  %s", JSON.toJSONString(ddls)));
            for (int i = 0; i < ddls.size(); i++) {
                String ddl = ddls.get(i);
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    ddl);
                if (i % 10 == 0) {
                    logger.info(String.format(" created %d tables or gsi", i));
                }
            }
        }
        logger.info(String.format(" create table and gsi finished "));
        int waitTime = 500 * (tableNums + maxGsiNum * tableNums);// 100s
        int minConcurrent = 12;
        int maxConcurrent = 16;
        String rebalanceDdl =
            String.format(
                "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database shuffle_data_dist=1;",
                maxConcurrent);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    @Ignore
    public void testSubJobConcurrentTaskRandom5() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_random_test5";

        int tableNums = 512;
        int maxGsiNum = 8;
        // random tablegroup match
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        if (!checkTableStatus(tddlConnection, -1L, "t0")) {
            List<String> ddls = generateRandomMatchCreateTableAndGsiSql(tableNums, maxGsiNum);
            logger.info(String.format(" create %d tables, %d gsi", tableNums, ddls.size() - tableNums));
            logger.info(String.format(" the ddl is  %s", JSON.toJSONString(ddls)));
            for (int i = 0; i < ddls.size(); i++) {
                String ddl = ddls.get(i);
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    ddl);
                if (i % 10 == 0) {
                    logger.info(String.format(" created %d tables or gsi", i));
                }
            }
        }
        logger.info(String.format(" create table and gsi finished "));
        int waitTime = 500 * (tableNums + maxGsiNum * tableNums);// 100s
        int minConcurrent = 24;
        int maxConcurrent = 32;
        String rebalanceDdl =
            String.format(
                "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database shuffle_data_dist=1;",
                maxConcurrent);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    @Ignore
    public void testSubJobConcurrentTaskRandom6() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_random_test6";

        int tableNums = 1024;
        int maxGsiNum = 16;
        // random tablegroup match
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        if (!checkTableStatus(tddlConnection, -1L, "t0")) {
            List<String> ddls = generateRandomMatchCreateTableAndGsiSql(tableNums, maxGsiNum);
            logger.info(String.format(" create %d tables, %d gsi", tableNums, ddls.size() - tableNums));
            logger.info(String.format(" the ddl is  %s", JSON.toJSONString(ddls)));
            for (int i = 0; i < ddls.size(); i++) {
                String ddl = ddls.get(i);
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    ddl);
                if (i % 10 == 0) {
                    logger.info(String.format(" created %d tables or gsi", i));
                }
            }
        }
        logger.info(String.format(" create table and gsi finished "));
        int waitTime = 500 * (tableNums + maxGsiNum * tableNums);// 100s
        int minConcurrent = 32;
        int maxConcurrent = 64;
        String rebalanceDdl =
            String.format(
                "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database shuffle_data_dist=1;",
                maxConcurrent);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    @Ignore
    public void testSubJobConcurrentTaskRandom1() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_random_test1";

        int tableNums = 32;
        int maxGsiNum = 3;
        // random tablegroup match
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        if (!checkTableStatus(tddlConnection, -1L, "t0")) {
            List<String> ddls = generateRandomMatchCreateTableAndGsiSql(tableNums, maxGsiNum);
            logger.info(String.format(" create %d tables, %d gsi", tableNums, ddls.size() - tableNums));
            logger.info(String.format(" the ddl is  %s", JSON.toJSONString(ddls)));
            for (int i = 0; i < ddls.size(); i++) {
                String ddl = ddls.get(i);
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    ddl);
                if (i % 10 == 0) {
                    logger.info(String.format(" created %d tables or gsi", i));
                }
            }
        }
        logger.info(String.format(" create table and gsi finished "));
        int waitTime = 500 * (tableNums + maxGsiNum * tableNums);// 100s
        int minConcurrent = 5;
        int maxConcurrent = 6;
        String rebalanceDdl =
            String.format(
                "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database shuffle_data_dist=1;",
                maxConcurrent);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    @Ignore
    public void testSubJobConcurrentTaskRandom2() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_random_test2";

        int tableNums = 128;
        int maxGsiNum = 3;
        // random tablegroup match
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        if (!checkTableStatus(tddlConnection, -1L, "t0")) {
            List<String> ddls = generateRandomMatchCreateTableAndGsiSql(tableNums, maxGsiNum);
            logger.info(String.format(" create %d tables, %d gsi", tableNums, ddls.size() - tableNums));
            logger.info(String.format(" the ddl is  %s", JSON.toJSONString(ddls)));
            for (int i = 0; i < ddls.size(); i++) {
                String ddl = ddls.get(i);
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    ddl);
                if (i % 10 == 0) {
                    logger.info(String.format(" created %d tables or gsi", i));
                }
            }
        }
        logger.info(String.format(" create table and gsi finished "));
        int waitTime = 500 * (tableNums + maxGsiNum * tableNums);// 100s
        int minConcurrent = 10;
        int maxConcurrent = 12;
        String rebalanceDdl =
            String.format(
                "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database shuffle_data_dist=1;",
                maxConcurrent);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    @Ignore
    public void testSubJobConcurrentTaskRandom3DrainNode() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_random_test3";

        int tableNums = 128;
        int maxGsiNum = 3;
        // random tablegroup match
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        if (!checkTableStatus(tddlConnection, -1L, "t0")) {
            List<String> ddls = generateRandomMatchCreateTableAndGsiSql(tableNums, maxGsiNum);
            logger.info(String.format(" create %d tables, %d gsi", tableNums, ddls.size() - tableNums));
            logger.info(String.format(" the ddl is  %s", JSON.toJSONString(ddls)));
            for (int i = 0; i < ddls.size(); i++) {
                String ddl = ddls.get(i);
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    ddl);
                if (i % 10 == 0) {
                    logger.info(String.format(" created %d tables or gsi", i));
                }
            }
        }
        logger.info(String.format(" create table and gsi finished "));
        int waitTime = 500 * (tableNums + maxGsiNum * tableNums);// 100s
        int minConcurrent = 10;
        int maxConcurrent = 12;
        String undeletableDn = LocalityTestBase.chooseDatanode(tddlConnection, true);
        List<String> dns = LocalityTestBase.getDatanodes(tddlConnection);
        dns.remove(undeletableDn);
        String dn = dns.get(0);
        String rebalanceDdl = String.format(
            "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database drain_node = '%s'",
            maxConcurrent, dn);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

}