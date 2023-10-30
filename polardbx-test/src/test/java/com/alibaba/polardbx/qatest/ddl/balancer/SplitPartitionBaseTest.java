package com.alibaba.polardbx.qatest.ddl.balancer;

import com.alibaba.polardbx.qatest.ddl.balancer.dataingest.DataIngestForThreeColumns;
import com.alibaba.polardbx.qatest.ddl.balancer.dataingest.DataIngest;
import com.alibaba.polardbx.qatest.ddl.balancer.dataingest.DataIngestForBigInt;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author moyi
 * @since 2021/04
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@NotThreadSafe
public class SplitPartitionBaseTest extends BalancerTestBase {

    private String tableName1;
    private String tableName2;

    private int tableRows = 2000;

    @Before
    public void before() {

        this.tableName1 = generateTableName();
        this.tableName2 = generateTableName();
        String createTableSqlFormat = "create table %s (id bigint, k int)  " +
            " partition by hash(id) partitions 1 " +
            " AUTO_SPLIT='ON'";
        String createTable1 = String.format(createTableSqlFormat, tableName1);
        String createTable2 = String.format(createTableSqlFormat, tableName2);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable2);
        LOG.info("create table " + tableName1);
        LOG.info("create table " + tableName2);
    }

    @After
    public void after() {

        String dropTableSqlFormat = "drop table %s";
        String dropTable1 = String.format(dropTableSqlFormat, tableName1);
        String dropTable2 = String.format(dropTableSqlFormat, tableName2);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable2);
    }

    @Test
    public void test1CreateTableAutoSplit() {

        String sql = "show create table " + tableName1;
        String createTableSql = JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 2);
        Assert.assertTrue("should contains AUTO_SPLIT flag", createTableSql.contains("AUTO_SPLIT=ON"));
    }

    /**
     * Create a table then verify the information_schema.table_detail
     */
    @Test
    public void test2TableDetail() throws SQLException {

        TableDetails detail1 = queryTableDetails(logicalDBName, tableName1);
        Assert.assertEquals(tableName1, detail1.tableName);
        Assert.assertNotNull(detail1.tableGroup);

        TableDetails detail2 = queryTableDetails(logicalDBName, tableName2);
        Assert.assertEquals(tableName2, detail2.tableName);
        Assert.assertNotNull(detail2.tableGroup);

        Assert.assertEquals(detail1.tableGroup, detail2.tableGroup);
    }

    @Test
    public void test2_ExplainRebalance() throws SQLException {

        // ingest data
        DataIngest ingest1 = new DataIngestForBigInt(tableName1, tddlConnection);
        ingest1.ingest(tableRows);
        JdbcUtil.executeUpdate(tddlConnection, "analyze table " + tableName1);

        // explain split
        TableDetails initialTableDetail = queryTableDetails(logicalDBName, tableName1);
        int splitPartitionSize = (int) (initialTableDetail.dataLength / 8);
        String explainSplitSql = genSplitPartitionSql(tableName1, splitPartitionSize, 10);
        explainSplitSql += " explain=true";
        try (ResultSet rs = JdbcUtil.executeQuery(explainSplitSql, tddlConnection)) {
            Assert.assertEquals(Arrays.asList("job_id", "schema", "name", "action", "backfill_rows"),
                JdbcUtil.getColumnNameListToLowerCase(rs));
            Assert.assertFalse("result should not be empty", JdbcUtil.getAllResult(rs).isEmpty());
        }
    }

    /**
     * Split partition-group instead of a single partition
     */
    @Test
    public void test3SplitPartitionGroup() throws SQLException {

        String analyzeTable1 = "analyze table " + tableName1;
        String analyzeTable2 = "analyze table " + tableName2;

        // ingest data
        DataIngest ingest1 = new DataIngestForBigInt(tableName1, tddlConnection);
        DataIngest ingest2 = new DataIngestForBigInt(tableName2, tddlConnection);
        ingest1.ingest(tableRows);
        ingest2.ingest(tableRows);
        JdbcUtil.executeUpdate(tddlConnection, analyzeTable1);
        JdbcUtil.executeUpdate(tddlConnection, analyzeTable2);

        TableDetails initialTableDetail = queryTableDetails(logicalDBName, tableName1);
        int splitPartitionSize = (int) (initialTableDetail.dataLength / 8);
        int mergePartitionSize = (int) initialTableDetail.dataLength * 10;

        // Split the first table
        String splitSql = genSplitPartitionSql(tableName1, splitPartitionSize, 10);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        // Verify the other table
        TableDetails afterSplitDetails = queryTableDetails(logicalDBName, tableName2);
        LOG.info("split partition-group into " + afterSplitDetails);
        Assert.assertTrue(afterSplitDetails.partitions.size() > initialTableDetail.partitions.size());
        JdbcUtil.executeUpdate(tddlConnection, analyzeTable1);
        JdbcUtil.executeUpdate(tddlConnection, analyzeTable2);

        // Merge partition-groups
        String mergeSql = genMergePartitionSql(tableName2, mergePartitionSize);
        JdbcUtil.executeUpdateSuccess(tddlConnection, mergeSql);

        // Verify merge
        TableDetails afterMerge = queryTableDetails(logicalDBName, tableName1);
        LOG.info("merge partitions into " + afterMerge);
        Assert.assertTrue(String.format("after merge has %d partitions", afterMerge.partitions.size()),
            afterMerge.partitions.size() < afterSplitDetails.partitions.size());
    }

    /**
     * Split key-partition, calculate multiple hashcode for each column
     */
    @Test
    public void test4SplitKeyPartition() throws SQLException {

        String tableName = "key_table";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);

        String createTable = "create table key_table(id1 int, id2 int, k bigint) partition by key(id1, id2) "
            + "AUTO_SPLIT='ON'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        DataIngest ingest1 = new DataIngestForThreeColumns(tableName, tddlConnection);
        ingest1.ingest(tableRows);
        String analyzeTable1 = "analyze table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, analyzeTable1);

        TableDetails initialTableDetail = queryTableDetails(logicalDBName, tableName, tddlConnection);
        int splitPartitionSize = (int) (initialTableDetail.dataLength / 8);
        String splitSql = genSplitPartitionSql(tableName, splitPartitionSize, 10);
        splitSql += " explain=true";

        try (ResultSet rs = JdbcUtil.executeQuery(splitSql, tddlConnection)) {
            List<List<Object>> result = JdbcUtil.getAllResult(rs);
            LOG.info("split: " + result);
            Assert.assertFalse(result.isEmpty());
        }

    }

}
