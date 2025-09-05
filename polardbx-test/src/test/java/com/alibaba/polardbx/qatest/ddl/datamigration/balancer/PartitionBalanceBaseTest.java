package com.alibaba.polardbx.qatest.ddl.datamigration.balancer;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.ddl.auto.dal.CheckTableTest;
import com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.DataLoader;
import com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.ManualHotSpotDataGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author moyi
 * @since 2021/04
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@NotThreadSafe
public class PartitionBalanceBaseTest extends BalancerTestBase {

    private int tableNum = 2;

    private List<String> tableNames = new ArrayList<>();
    private int tableRows = 500000;

    public static int maxWaitTime = 5000 * 200;

    public static int waitTime = 2000;

    @Before
    public void before() throws FileNotFoundException, SQLException, InterruptedException {
        CreateTableBeans createTableBeans;
        try {
            createTableBeans = loadCreateTableBeans("rebalance_database_partition_balance.test.yml");
        } catch (Exception e) {
            throw e;
        }
        List<String> createTableSqls = createTableBeans.sqlTemplates;
        int templateNum = createTableSqls.size();
        for (int i = 0; i < tableNum; i++) {
            String tableName = String.format("t%d", i + 1);
            String createTableSqlFormat = createTableSqls.get(i % templateNum);
            String createTable = String.format(createTableSqlFormat, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
            tableNames.add(tableName);
            LOG.info("create table " + tableName);
        }
        Long start = new Date().getTime();
        List<Thread> threads = new ArrayList<>();
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = tableNames.get(i);
            Connection tddlConnection = getPolardbxConnection(logicalDBName);
            connections.add(tddlConnection);
            DataLoader dataLoader = DataLoader.create(tddlConnection, tableName, new ManualHotSpotDataGenerator());
            Thread thread = new Thread(() -> {
                dataLoader.batchInsert(tableRows, false);
            });
            threads.add(thread);
            thread.start();
//        int splitPartitionSize = (int) (initialTableDetail.dataLength / 8);
        }
        for (int i = 0; i < tableNames.size(); i++) {
            threads.get(i).join();
        }
        for (Connection connection : connections) {
            connection.close();
        }
        Long end = new Date().getTime();
        LOG.info("data loader cost " + (end - start) + " ms");
    }

    @After
    public void after() {

        String dropTableSqlFormat = "drop table %s";
        for (int i = 0; i < tableNum; i++) {
            String tableName = tableNames.get(i);
            String dropTable = String.format(dropTableSqlFormat, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
        }
    }

    protected String genPartitionBalanceSql(String table, String tableGroup) {
        final String partitionBalance = "rebalance tablegroup %s policy='partition_balance'";
        return String.format(partitionBalance, tableGroup);
    }

    protected String genAutoSplitForPartitionBalanceSql(String table, String tableGroup) {
        final String partitionBalance = "rebalance tablegroup %s policy='auto_split_for_partition_balance'";
        return String.format(partitionBalance, tableGroup);
    }

    protected CreateTableBeans loadCreateTableBeans(String fileName) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        String resourceDir = "partition/env/RebalanceTest/" + fileName;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        InputStream inputStream = new FileInputStream(fileDir);
        return yaml.loadAs(inputStream, CreateTableBeans.class);
    }

    protected void waitTillTriggerOn(String sql, Connection connection) {
        Boolean triggerOn = false;
        int totalWaitTime = 0;
        while (!triggerOn) {
            try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
                if (!JdbcUtil.getAllResult(rs).isEmpty()) {
                    Thread.sleep(waitTime);
                    totalWaitTime += waitTime;
                } else {
                    triggerOn = true;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (totalWaitTime >= maxWaitTime) {
                throw new RuntimeException("wait trigger on timeout!");
            }
        }
    }

    /**
     * Create a table then verify the information_schema.table_detail
     */

    protected void checkPartitionDetails(TableDetails tableDetails) {
        long sumRows = tableDetails.tableRows;
        long sumPartNum = tableDetails.partitions.size();
        Map<String, List<PartitionDetail>> partGroupByStorage = tableDetails.partitions.stream().collect(
            Collectors.groupingBy(o -> o.storageInstId, Collectors.toList()));
        int dnNum = partGroupByStorage.keySet().size();
        long avgRows = Math.floorDiv(sumRows, dnNum);
        long minRows = Double.valueOf(avgRows * 0.85).longValue();
        long maxRows = Double.valueOf(avgRows * 1.15).longValue();
        long minPartNum = Math.floorDiv(sumPartNum, dnNum);
        long maxPartNum = -Math.floorDiv(-sumPartNum, dnNum);
        for (String storage : partGroupByStorage.keySet()) {
            int partNum = partGroupByStorage.get(storage).size();
            long rows = partGroupByStorage.get(storage).stream().map(o -> o.rows).reduce(0L, Long::sum);
            Assert.assertTrue("part num not balanced", (minPartNum <= partNum) && (partNum <= maxPartNum));
            Assert.assertTrue("part size not balanced", (minRows <= rows) && (rows <= maxRows));
        }
    }

    @Test
    public void test1_PartitionBalance() throws SQLException {
        String balanceDbSql = "rebalance database policy='partition_balance'";
        try (ResultSet rs = JdbcUtil.executeQuery(balanceDbSql, tddlConnection);) {
            LOG.info("execute " + balanceDbSql);
            ResultSetMetaData rsmd = rs.getMetaData();
            if (rsmd.getColumnCount() == 5) {
                //logical backfill
                Assert.assertEquals(Arrays.asList("job_id", "schema", "name", "action", "backfill_rows"),
                    JdbcUtil.getColumnNameListToLowerCase(rs));
            } else {
                //physical backfill
                Assert.assertEquals(
                    Arrays.asList("job_id", "schema", "name", "action", "backfill_rows", "backfill_data_size",
                        "backfill_estimated_time"),
                    JdbcUtil.getColumnNameListToLowerCase(rs));
            }
//            Boolean emptyResult = JdbcUtil.getAllResult(rs).stream()
//                .noneMatch(o -> o.get(2).toString().equalsIgnoreCase("MovePartition"));
//            Assert.assertTrue("result should be empty", emptyResult);
        }

        LOG.info("wait for result " + balanceDbSql);
        waitTillTriggerOn("show ddl", tddlConnection);

//        for (int i = 0; i < tableNames.size(); i++) {
//            String tableName = tableNames.get(i);
//            TableDetails initialTableDetail = queryTableDetails(logicalDBName, tableName);
//            String balanceSql = genPartitionBalanceSql(tableName, initialTableDetail.tableGroup);
//            String explainBalanceSQL = balanceSql + " explain=true";
//            try (ResultSet rs = JdbcUtil.executeQuery(explainBalanceSQL, tddlConnection)) {
//                Assert.assertEquals(Arrays.asList("job_id", "schema", "name", "action", "backfill_rows"),
//                    JdbcUtil.getColumnNameListToLowerCase(rs));
//                Boolean emptyResult = JdbcUtil.getAllResult(rs).stream()
//                    .noneMatch(o -> o.get(2).toString().equalsIgnoreCase("MovePartition"));
//                Assert.assertTrue("result should be empty", emptyResult);
//            }
//        }
//
//        for (int i = 0; i < tableNames.size(); i++) {
//            String tableName = tableNames.get(i);
//            String analyzeTableSql = String.format("analyze table %s", tableName);
//            JdbcUtil.executeUpdate(tddlConnection, analyzeTableSql);
//            LOG.info("execute  " + analyzeTableSql);
//            TableDetails initialTableDetail = queryTableDetails(logicalDBName, tableName);
//            String balanceSql = genPartitionBalanceSql(tableName, initialTableDetail.tableGroup);
//            String explainBalanceSQL = balanceSql + " explain=true";
//            try (ResultSet rs = JdbcUtil.executeQuery(explainBalanceSQL, tddlConnection)) {
//                Assert.assertEquals(Arrays.asList("job_id", "schema", "name", "action", "backfill_rows"),
//                    JdbcUtil.getColumnNameListToLowerCase(rs));
//                Boolean emptyResult = JdbcUtil.getAllResult(rs).stream()
//                    .noneMatch(o -> o.get(2).toString().equalsIgnoreCase("MovePartition"));
//                Assert.assertTrue("result should be empty", emptyResult);
//            }
//        }

        String rebalanceAutoSplitDbSql =
            "rebalance database policy='auto_split_for_partition_balance' solve_level = 'hot_split'";
        try (ResultSet rs = JdbcUtil.executeQuery(rebalanceAutoSplitDbSql, tddlConnection);) {
            LOG.info("execute  " + rebalanceAutoSplitDbSql);
            ResultSetMetaData rsmd = rs.getMetaData();
            if (rsmd.getColumnCount() == 5) {
                Assert.assertEquals(Arrays.asList("job_id", "schema", "name", "action", "backfill_rows"),
                    JdbcUtil.getColumnNameListToLowerCase(rs));
            } else {
                Assert.assertEquals(
                    Arrays.asList("job_id", "schema", "name", "action", "backfill_rows", "backfill_data_size",
                        "backfill_estimated_time"),
                    JdbcUtil.getColumnNameListToLowerCase(rs));
            }
            Boolean emptyResult = JdbcUtil.getAllResult(rs).stream()
                .noneMatch(o -> o.get(2).toString().equalsIgnoreCase("SplitPartition"));
            Assert.assertFalse("result should not be empty", emptyResult);
        }
        LOG.info("wait for result  " + rebalanceAutoSplitDbSql);
        waitTillTriggerOn("show ddl", tddlConnection);

        try (ResultSet rs = JdbcUtil.executeQuery(balanceDbSql, tddlConnection);) {
            LOG.info("execute  " + balanceDbSql);
            ResultSetMetaData rsmd = rs.getMetaData();
            if (rsmd.getColumnCount() == 5) {
                Assert.assertEquals(Arrays.asList("job_id", "schema", "name", "action", "backfill_rows"),
                    JdbcUtil.getColumnNameListToLowerCase(rs));
            } else {
                Assert.assertEquals(
                    Arrays.asList("job_id", "schema", "name", "action", "backfill_rows", "backfill_data_size",
                        "backfill_estimated_time"),
                    JdbcUtil.getColumnNameListToLowerCase(rs));
            }
//            Boolean emptyResult = JdbcUtil.getAllResult(rs).stream().noneMatch(o->o.get(2).toString().equalsIgnoreCase("MovePartition"));
//            Assert.assertFalse("result should not be empty", emptyResult);
        }

        LOG.info("wait for result  " + balanceDbSql);
        waitTillTriggerOn("show ddl", tddlConnection);

//        for (int i = 0; i < tableNames.size(); i++) {
//            String tableName = tableNames.get(i);
//            TableDetails tableDetail = queryTableDetails(logicalDBName, tableName);
//            checkPartitionDetails(tableDetail);
//            LOG.info("check partition  " + tableName);
//            String balanceSql = genPartitionBalanceSql(tableName, tableDetail.tableGroup);
//            String explainBalanceSQL = balanceSql + " explain=true";
//            try (ResultSet rs = JdbcUtil.executeQuery(explainBalanceSQL, tddlConnection)) {
//                Assert.assertEquals(Arrays.asList("job_id", "schema", "name", "action", "backfill_rows"),
//                    JdbcUtil.getColumnNameListToLowerCase(rs));
//                Boolean emptyResult = JdbcUtil.getAllResult(rs).stream()
//                    .noneMatch(o -> o.get(2).toString().equalsIgnoreCase("MovePartition"));
//                Assert.assertTrue("result should be empty", emptyResult);
//            }
//        }

//        JdbcUtil.executeUpdate(tddlConnection, "analyze table " + tableName1);
    }

    @Test
    public void test2_ShowHotKey() throws SQLException {

        // ingest data

        String showHotKey = "show hotkey from %s partition(%s)";
        String tableName = tableNames.get(1);
        String showHotKeySql = String.format(showHotKey, tableName, "p17");
        Pair<Integer, String> phyDbNameTableName =
            CheckTableTest.getFullObjectName(tddlConnection, tableName, tableName, 16);
        String analyzeSql = String.format("analyze table %s", phyDbNameTableName.getValue());
//        String groupHint = String.format("/*+TDDL:node(%d)*/", phyDbNameTableName.getKey());
//        String sql = groupHint + analyzeSql;
        Connection connection = getMysqlConnection();
        JdbcUtil.executeUpdateSuccess(connection, "use balancertestbase_p00000");
        for (int i = 0; i < 16; i++) {
            JdbcUtil.executeUpdateSuccess(connection, analyzeSql);
        }
        String version =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery("select @@version", connection)).get(0).get(0).toString();
        Boolean is80Version = version.startsWith("8.0");
//        JdbcUtil.executeUpdateSuccess(tddlConnection, analyzeSql);
        try (ResultSet rs = JdbcUtil.executeQuery(showHotKeySql, tddlConnection);) {
            Assert.assertEquals(Arrays.asList("schema_name", "table_name", "part_name", "hotvalue", "estimate_rows",
                    "estimate_percentage"),
                JdbcUtil.getColumnNameListToLowerCase(rs));
            Assert.assertFalse("result should not be empty", JdbcUtil.getAllResult(rs).isEmpty() && !is80Version);
        }
    }

}
