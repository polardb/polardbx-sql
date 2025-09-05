package com.alibaba.polardbx.qatest.ddl.datamigration.mpp.checker;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.mpp.pkrange.PkTest;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;

@NotThreadSafe
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LargeTableCheckTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PkTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public LargeTableCheckTest(boolean crossSchema) {
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

    public void prepareTableIfNotExists(Connection tddlConnection, String schemaName, String tableName, int rows)
        throws Exception {
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        prepareTableIfNotExists(tddlConnection, schemaName, tableName, createTableStmt, rows);

    }

    public void prepareTableIfNotExists(Connection tddlConnection, String schemaName, String tableName,
                                        String createTableStmt, int rows)
        throws Exception {
        JdbcUtil.executeUpdate(tddlConnection, "create database if not exists " + schemaName + " mode = auto");

        int partNum = 3;
        int eachPartRows = rows / partNum;
        JdbcUtil.executeUpdate(tddlConnection, "use " + schemaName);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, "show tables"));
        Boolean tableExists = results.stream().anyMatch(o -> o.get(0).toString().equalsIgnoreCase(tableName));
//        JdbcUtil.executeUpdate(tddlConnection, "drop table " + tableName);
        if (!tableExists) {
            prepareData(tddlConnection, schemaName, tableName, eachPartRows, createTableStmt,
                partNum, DataManipulateUtil.TABLE_TYPE.PARTITION_TABLE);
            String analyzeTableSql = String.format("analyze table %s", tableName);
            JdbcUtil.executeUpdate(tddlConnection, analyzeTableSql);
        }
    }

    static String  convertTimeStamp(LocalDateTime dateTime){
//        LocalDateTime dateTime = Instant.ofEpochMilli(timestamp)
//            .atZone(ZoneId.systemDefault())
//            .toLocalDateTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String dateTimeString = dateTime.format(formatter);
        return dateTimeString;
    }
    static List<String> buildPkStringList(Connection connection, String sql) {
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(connection, sql));
        List<String> resultStr = new ArrayList<>();
        for (List<Object> result : results) {
            List<Object> convertedResult = new ArrayList<>();
            for (Object o : result) {
                if (o instanceof JdbcUtil.MyNumber) {
                    convertedResult.add(((JdbcUtil.MyNumber) o).getNumber());
                } else if (o instanceof JdbcUtil.MyDate) {
                    convertedResult.add(convertTimeStamp(((Timestamp) ((JdbcUtil.MyDate) o).getDate()).toLocalDateTime()));
                } else {
                    convertedResult.add(o.toString());
                }
            }
            resultStr.add(JSON.toJSONString(convertedResult));
        }
        return resultStr;
    }

    static void recheckSelectFullSql(Connection connection, String sql) {
        List<String> results = buildPkStringList(connection, sql);
        logger.warn(" recheck Select full sql " + sql + " results ### " + JSON.toJSONString(results));
    }

    @Test
    public void test00MovePartitionErrorReportForLargeTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "large_table_check_auto";
        // prepare data
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 8000_000);
        Map<String, List<String>> topology = DdlStateCheckUtil.getTableTopology(tddlConnection, originalTableName);
        List<String> dns = DdlStateCheckUtil.getStorageList(tddlConnection);
        String setBatcherRecheckerNumSqlStmt = "set global FASTCHECKER_MAX_RECHECK_BATCH = %d";
        String setBatcherRecheckerNumSql = String.format(setBatcherRecheckerNumSqlStmt, 8);
        JdbcUtil.executeUpdate(tddlConnection, setBatcherRecheckerNumSql);

        String dn1 = topology.get("p1").get(0);
        String dn2 = topology.get("p2").get(0);
        String p1Group = topology.get("p1").get(1);
        String p2Group = topology.get("p2").get(1);
        String p1PhyTableName = topology.get("p1").get(2);
        String p2PhyTableName = topology.get("p2").get(2);
        String movePartitionSql =
            String.format("alter table %s move partitions (p1) to '%s', (p2) to '%s' async=true", originalTableName,
                dn2, dn1);
        JdbcUtil.executeUpdate(tddlConnection, movePartitionSql);

        Long jobId = DdlStateCheckUtil.getRootDdlJobIdFromPattern(tddlConnection, movePartitionSql);
        String updateSqlStmt = "/*+TDDL:node(%s)*/ update %s set c = 'error value' where a >= %d and a <= %d;";
        String selectSqlStmt = "/*+TDDL:node(%s)*/ select a from %s where a >= %d and a <= %d;";
        DdlStateCheckUtil.waitTillImportTableSpaceDone(tddlConnection, jobId, null);
        DdlStateCheckUtil.waitTillLogicalBackfillDone(tddlConnection, jobId, null);
        DdlStateCheckUtil.pauseDdl(tddlConnection, jobId);
        List<List<String>> pkErrors = new ArrayList<>();

        /* p1  200_0000~200_0020 */
        int left = 2_000_000;
        int right = left + 20;
        String updateSql = String.format(updateSqlStmt, p2Group, p1PhyTableName, left, right);
        String selectSql = String.format(selectSqlStmt, p2Group, p1PhyTableName, left, right);
        List<String> pkList = buildPkStringList(tddlConnection, selectSql);
//        pkErrors.add(Lists.newArrayList("2000002", "2000012", "2000018", "2000019", "2000020"));
        pkErrors.add(pkList);
        JdbcUtil.executeUpdate(tddlConnection, updateSql);

        pkList = new ArrayList<>();
        /* p2  0~20 */
        left = 0;
        right = left + 20;
        updateSql = String.format(updateSqlStmt, p1Group, p2PhyTableName, left, right);
        selectSql = String.format(selectSqlStmt, p1Group, p2PhyTableName, left, right);
        pkList = buildPkStringList(tddlConnection, selectSql);
        JdbcUtil.executeUpdate(tddlConnection, updateSql);

        /* p2  700_0080~800_0000 */
        right = 8_000_000;
        left = right - 20;
        updateSql = String.format(updateSqlStmt, p1Group, p2PhyTableName, left, right);
        selectSql = String.format(selectSqlStmt, p1Group, p2PhyTableName, left, right);
        JdbcUtil.executeUpdate(tddlConnection, updateSql);
        pkList = buildPkStringList(tddlConnection, selectSql);
        pkErrors.add(pkList);

        DdlStateCheckUtil.continueDdlAsync(tddlConnection, jobId);
        DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, null);
        DdlStateCheckUtil.checkIfReportErrorByChecker(tddlConnection, jobId, Lists.newArrayList(p1Group, p2Group),
            Lists.newArrayList(p1PhyTableName, p2PhyTableName), pkErrors, pkErrors.size());
    }

    @Test
    public void test01MovePartitionErrorReportForLargeTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "large_table_check_auto1";
        String createTableStmt =
            "create table if not exists "
                + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), d varchar(32), e datetime, PRIMARY KEY(e, d, a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        // prepare data
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, createTableStmt, 8000_000);
        Map<String, List<String>> topology = DdlStateCheckUtil.getTableTopology(tddlConnection, originalTableName);
        List<String> dns = DdlStateCheckUtil.getStorageList(tddlConnection);
        String setBatcherRecheckerNumSqlStmt = "set global FASTCHECKER_MAX_RECHECK_BATCH = %d";
        String setBatcherRecheckerNumSql = String.format(setBatcherRecheckerNumSqlStmt, 8);
        JdbcUtil.executeUpdate(tddlConnection, setBatcherRecheckerNumSql);

        String dn1 = topology.get("p1").get(0);
        String dn2 = topology.get("p2").get(0);
        String dn3 = topology.get("p3").get(0);
        String p1Group = topology.get("p1").get(1);
        String p2Group = topology.get("p2").get(1);
        String p3Group = topology.get("p3").get(1);
        String p1PhyTableName = topology.get("p1").get(2);
        String p2PhyTableName = topology.get("p2").get(2);
        String p3PhyTableName = topology.get("p3").get(2);
        String movePartitionSql =
            String.format("alter table %s move partitions (p1, p3) to '%s', (p2) to '%s' async=true", originalTableName,
                dn2, dn1);
        int maxReportNum = 2;
        setBatcherRecheckerNumSql = String.format(setBatcherRecheckerNumSqlStmt, maxReportNum);
        JdbcUtil.executeUpdate(tddlConnection, setBatcherRecheckerNumSql);
        JdbcUtil.executeUpdate(tddlConnection, movePartitionSql);

        Long jobId = DdlStateCheckUtil.getRootDdlJobIdFromPattern(tddlConnection, movePartitionSql);
        String updateSqlStmt = "/*+TDDL:node(%s)*/ update %s set c = 'error value' where a >= %d and a <= %d;";
        String selectSqlStmt = "/*+TDDL:node(%s)*/ select e, d, a from %s where a >= %d and a <= %d;";
        String selectFullSqlStmt = "/*+TDDL:node(%s)*/ select * from %s where a >= %d and a <= %d;";
        DdlStateCheckUtil.waitTillImportTableSpaceDone(tddlConnection, jobId, null);
        DdlStateCheckUtil.waitTillLogicalBackfillDone(tddlConnection, jobId, null);
        DdlStateCheckUtil.pauseDdl(tddlConnection, jobId);
        List<List<String>> pkErrors = new ArrayList<>();

        /* p1  200_0000~200_0020 */
        int left = 2_000_000;
        int right = left + 20;
        String updateSql = String.format(updateSqlStmt, p2Group, p1PhyTableName, left, right);
        String selectSql = String.format(selectSqlStmt, p2Group, p1PhyTableName, left, right);
        String selectFullSql = String.format(selectFullSqlStmt, p2Group, p1PhyTableName, left, right);
        List<String> pkList = buildPkStringList(tddlConnection, selectSql);
        JdbcUtil.executeUpdate(tddlConnection, updateSql);
        recheckSelectFullSql(tddlConnection, selectFullSql);
//        pkErrors.add(Lists.newArrayList("2000002", "2000012", "2000018", "2000019", "2000020"));
        pkErrors.add(pkList);

        /* p2  0~20 */
        left = 0;
        right = left + 20;
        updateSql = String.format(updateSqlStmt, p1Group, p2PhyTableName, left, right);
        selectSql = String.format(selectSqlStmt, p1Group, p2PhyTableName, left, right);
        selectFullSql = String.format(selectFullSqlStmt, p1Group, p2PhyTableName, left, right);
        pkList = buildPkStringList(tddlConnection, selectSql);
        pkErrors.add(pkList);
        JdbcUtil.executeUpdate(tddlConnection, updateSql);
        recheckSelectFullSql(tddlConnection, selectFullSql);

        /* p3  700_0080~800_0000 */
        right = 8_000_000;
        left = right - 2000;
        updateSql = String.format(updateSqlStmt, p2Group, p3PhyTableName, left, right);
        selectSql = String.format(selectSqlStmt, p2Group, p3PhyTableName, left, right);
        selectFullSql = String.format(selectFullSqlStmt, p2Group, p3PhyTableName, left, right);
        pkList = buildPkStringList(tddlConnection, selectSql);
        JdbcUtil.executeUpdate(tddlConnection, updateSql);
        recheckSelectFullSql(tddlConnection, selectFullSql);
        pkErrors.add(pkList);

        DdlStateCheckUtil.continueDdlAsync(tddlConnection, jobId);
        DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, null);

        // recover
        setBatcherRecheckerNumSql = String.format(setBatcherRecheckerNumSqlStmt, 8);
        JdbcUtil.executeUpdate(tddlConnection, setBatcherRecheckerNumSql);

        DdlStateCheckUtil.checkIfReportErrorByChecker(tddlConnection, jobId,
            Lists.newArrayList(p1Group, p2Group, p3Group),
            Lists.newArrayList(p1PhyTableName, p2PhyTableName, p3PhyTableName), pkErrors, maxReportNum);
    }

    @Test
    public void test10MovePartitionCheckConcurrentForLargeTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "large_table_check_auto";
        // prepare data
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 8000_000);
        Map<String, List<String>> topology = DdlStateCheckUtil.getTableTopology(tddlConnection, originalTableName);
        List<String> dns = DdlStateCheckUtil.getStorageList(tddlConnection);
        String originalDn = topology.get("p1").get(0);
        dns.remove(originalDn);
        String dn = dns.get(0);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global FASTCHECKER_THREAD_POOL_SIZE=3");
        String movePartitionSql =
            String.format(
                "/*+TDDL:cmd_extra(FASTCHECKER_BATCH_SIZE=400000,FASTCHECKER_BATCH_PARALLEL=true,FASTCHECKER_THREAD_POOL_SIZE=3)*/alter table %s move partitions p1 to '%s' async=true",
                originalTableName, dn);
        JdbcUtil.executeUpdate(tddlConnection, movePartitionSql);

        Long jobId = DdlStateCheckUtil.getRootDdlJobIdFromPattern(tddlConnection, movePartitionSql);
//        DdlStateCheckUtil.waitTillImportTableSpaceDone(tddlConnection, jobId, null);
//        DdlStateCheckUtil.waitTillLogicalBackfillDone(tddlConnection, jobId, null);
        // pause
//        DdlStateCheckUtil.pauseDdl(tddlConnection, jobId);
//        // continue
//        DdlStateCheckUtil.continueDdlAsync(tddlConnection, jobId);
        // check concurrent checker valid
        DdlStateCheckUtil.waitTillDdlDone(log, tddlConnection, jobId, tableName, 3, "hashcheck", false);
        DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId);
    }

    @Test
    public void test20RepartitionConcurrentForLargeTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "large_table_check_auto";
        // prepare data
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 8000_000);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global FASTCHECKER_THREAD_POOL_SIZE=2");
        String movePartitionSql =
            String.format(
                "/*+TDDL:cmd_extra(FASTCHECKER_BATCH_SIZE=400000,FASTCHECKER_BATCH_PARALLEL=true,FASTCHECKER_THREAD_POOL_SIZE=2)*/alter table %s partition by hash(a) partitions 4 async=true",
                originalTableName);
        JdbcUtil.executeUpdate(tddlConnection, movePartitionSql);

        Long jobId = DdlStateCheckUtil.getRootDdlJobIdFromPattern(tddlConnection, movePartitionSql);
//        DdlStateCheckUtil.waitTillImportTableSpaceDone(tddlConnection, jobId, null);
//        DdlStateCheckUtil.waitTillLogicalBackfillDone(tddlConnection, jobId, null);
//        // pause
//        DdlStateCheckUtil.pauseDdl(tddlConnection, jobId);
//        // continue
//        DdlStateCheckUtil.continueDdlAsync(tddlConnection, jobId);
        // check concurrent checker valid
        DdlStateCheckUtil.waitTillDdlDone(log, tddlConnection, jobId, tableName, 2, "hashcheck", false);
        DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId);
    }

    @Test
    public void test30EmptyTableMovePartition() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_check_auto";
        // prepare data
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 0);
        Map<String, List<String>> topology = DdlStateCheckUtil.getTableTopology(tddlConnection, originalTableName);
        List<String> dns = DdlStateCheckUtil.getStorageList(tddlConnection);
        String originalDn = topology.get("p1").get(0);
        dns.remove(originalDn);
        String dn = dns.get(0);
        String movePartitionSql =
            String.format(
                "/*+TDDL:cmd_extra(FASTCHECKER_BATCH_SIZE=400000,FASTCHECKER_BATCH_PARALLEL=true,FASTCHECKER_THREAD_POOL_SIZE=3)*/alter table %s move partitions p1 to '%s' async=true",
                originalTableName, dn);
        JdbcUtil.executeUpdate(tddlConnection, movePartitionSql);

        Long jobId = DdlStateCheckUtil.getRootDdlJobIdFromPattern(tddlConnection, movePartitionSql);
        // pause
        DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, tableName);
        DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId);
    }

    @Test
    public void test40EmptyTableRepartition() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_check_auto";
        // prepare data
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 0);
        String movePartitionSql =
            String.format(
                "/*+TDDL:cmd_extra(FASTCHECKER_BATCH_SIZE=400000,FASTCHECKER_BATCH_PARALLEL=true,FASTCHECKER_THREAD_POOL_SIZE=3)*/alter table %s partition by hash(a) partitions 5 async=true",
                originalTableName);
        JdbcUtil.executeUpdate(tddlConnection, movePartitionSql);

        Long jobId = DdlStateCheckUtil.getRootDdlJobIdFromPattern(tddlConnection, movePartitionSql);
        // check concurrent checker valid
        DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, tableName);
        DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId);
    }
}