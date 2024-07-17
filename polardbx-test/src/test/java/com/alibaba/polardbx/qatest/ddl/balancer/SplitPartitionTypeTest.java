package com.alibaba.polardbx.qatest.ddl.balancer;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.ddl.balancer.dataingest.DataIngest;
import com.alibaba.polardbx.qatest.ddl.balancer.dataingest.DataIngestForBigInt;
import com.alibaba.polardbx.qatest.ddl.balancer.dataingest.DataIngestForDateTime;
import com.alibaba.polardbx.qatest.ddl.balancer.dataingest.DataIngestForString;
import com.alibaba.polardbx.qatest.ddl.balancer.dataingest.DataIngestForThreeColumns;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author moyi
 * @since 2021/04
 */

@NotThreadSafe
public class SplitPartitionTypeTest extends BalancerTestBase {

    private static Logger LOG = LoggerFactory.getLogger(SplitPartitionTypeTest.class);

    @Parameterized.Parameter
    public TestParameter parameter;

    private String currentTableName;

    @Before
    public void setUp() {

        this.currentTableName = generateTableName();
        this.parameter.dataIngest.setTable(this.currentTableName);
        this.parameter.dataIngest.setConnection(tddlConnection);
    }

    @After
    public void tearDown() {

        this.parameter.dataIngest.checkConsistency();

        final String sql = "drop table " + this.currentTableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Parameterized.Parameters(name = "{index}: rebalance table {0}")
    public static List<TestParameter> parameters() {
        // partition strategy: range/list/hash/range column/list column/key
        // data type: numeric/string/time
        return Arrays.asList(
            /* BIGINT */
            new TestParameter(
                "(id bigint, k int)",
                " partition by hash(id) partitions 1",
                new DataIngestForBigInt()
            ),
            new TestParameter(
                "(id bigint, k int)",
                " partition by range(id) (partition p0 values less than (1000000)) ",
                new DataIngestForBigInt()
            ),
            new TestParameter(
                "(id bigint, k int)",
                " partition by key(id) partitions 1",
                new DataIngestForBigInt()
            ),
            new TestParameter(
                "(id1 bigint, id2 bigint, k int)",
                " partition by key(id1, id2) partitions 1",
                new DataIngestForThreeColumns()
            ),
            new TestParameter(
                "(id bigint, k int)",
                " partition by range columns(id) (partition p0 values less than(10000000)) ",
                new DataIngestForBigInt()
            ),
            new TestParameter(
                "(id1 bigint, id2 bigint, k int)",
                " partition by range columns(id1, id2) (partition p0 values less than (500000, 1000000) )",
                new DataIngestForThreeColumns()
            ),

            /* VARCHAR */
            new TestParameter(
                "(id varchar(24), k int)",
                " partition by key(id) partitions 1 ",
                new DataIngestForString(24)
            ),
            new TestParameter(
                "(id varchar(24), k int)",
                " partition by key(id, k) partitions 1",
                new DataIngestForString(24)
            ),
            new TestParameter(
                "(id varchar(24), k int)",
                " partition by range columns(id, k) (partition p0 values less than ('z', 10000000) )",
                new DataIngestForString(24)
            ),

            /* TIME */
            /* DATE */
            /* TIMESTAMP */

            /* DATETIME */
            new TestParameter(
                "(id datetime, k int) ",
                " partition by hash(year(id)) partitions 1",
                new DataIngestForDateTime()
            ),
            new TestParameter(
                "(id datetime, k int) ",
                " partition by range(year(id)) (partition p0 values less than (2046) )",
                new DataIngestForDateTime()
            ),
            new TestParameter(
                "(id datetime, k int) ",
                " partition by range columns(id) (partition p0 values less than ('2046-01-01 00:00:00') )",
                new DataIngestForDateTime()
            ),
            new TestParameter(
                "(id datetime, k int) ",
                " partition by key(id) partitions 1 ",
                new DataIngestForDateTime()
            )

        );
    }

    /**
     * split and merge a partitioned-table
     */
    @Test
    public void rebalanceTable_SplitPartition() throws SQLException {

        final String partitionBy = this.parameter.partitionBy;
        final String columns = this.parameter.columns;
        final String createTableSql = "create table " + currentTableName + columns + partitionBy;
        final String analyzeTableSql = "analyze table " + currentTableName;
        final int dataRows = 2000_00;

        // ingest bulk of data
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        this.parameter.dataIngest.ingest(dataRows);
        JdbcUtil.executeUpdateSuccess(tddlConnection, analyzeTableSql);

        // query table detail
        TableDetails initialTableDetail = queryTableDetails(logicalDBName, currentTableName);
        int splitPartitionSize = (int) (initialTableDetail.dataLength / 8);
        int mergePartitionSize = (int) initialTableDetail.dataLength * 10;

        LOG.info("initial table detail: " + initialTableDetail);

        // split partition
        String splitSql = genSplitPartitionSql(currentTableName, splitPartitionSize, 8);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, analyzeTableSql);

        // verify split
        TableDetails afterSplitDetails = queryTableDetails(logicalDBName, currentTableName);
        LOG.info("after split table detail: " + afterSplitDetails);
        Assert.assertTrue(afterSplitDetails.partitions.size() > initialTableDetail.partitions.size());

        // TODO: test merge. but not for now.
        // merge partitions into one partition
//        String mergeSql = genMergePartitionSql(currentTableName, mergePartitionSize);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, mergeSql);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, analyzeTableSql);
//
//        // verify merge
//        TableDetails afterMerge = queryTableDetails(logicalDBName, currentTableName);
//        Assert.assertTrue(String.format("after merge has %d partitions", afterMerge.partitions.size()),
//            afterMerge.partitions.size() < afterSplitDetails.partitions.size());
//
//        LOG.info("after merge table detail: " + afterMerge);
    }

    static class TestParameter {
        public String columns;
        public String partitionBy;
        public DataIngest dataIngest;

        public TestParameter(String columns, String partitionBy, DataIngest dataIngest) {
            this.columns = columns;
            this.partitionBy = partitionBy;
            this.dataIngest = dataIngest;
        }

        @Override
        public String toString() {
            return "TestCase " + this.columns + this.partitionBy;
        }
    }
}
