package com.alibaba.polardbx.qatest.ddl.balancer;

import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.DataLoader;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.ManualHotSpotDataGenerator;
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
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
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
public class PartitionBalanceTest extends BalancerTestBase {

    void runPartitionBalanceTest(String testCaseFile) throws IOException, SQLException, InterruptedException {
        BalanceCase balanceCase = new BalanceCase(testCaseFile);
        balanceCase.runCase();
    }

    @Test
    public void runTestCaseSample() throws IOException, SQLException, InterruptedException {
        String testCaseFile = "sample.test.yml";
        for (int i = 0; i < 2; i++) {
            runPartitionBalanceTest(testCaseFile);
        }
    }

    @Test
    public void runTestCaseSampleLongTair() throws IOException, SQLException, InterruptedException {
        String testCaseFile = "sample_long_tair.test.yml";
        runPartitionBalanceTest(testCaseFile);
    }

    @Test
    public void runTestCaseSplitMixedSampleError() throws IOException, SQLException, InterruptedException {
        String testCaseFile = "rebalance_table_split_error.test.yml";
        runPartitionBalanceTest(testCaseFile);
    }

    @Test
    public void runTestCaseSplitMixedSampleHotSplit() throws IOException, SQLException, InterruptedException {
        String testCaseFile = "rebalance_table_mixed.test.yml";
        runPartitionBalanceTest(testCaseFile);

    }

//    @Test
//    public void runTestCaseSplitSampleLongTair() throws IOException, SQLException, InterruptedException {
//        String testCaseFile = "rebalance_table_split_long_tair.test.yml";
//        runPartitionBalanceTest(testCaseFile);
//    }

    @Test
    public void runTestCaseRebalanceDatabaseSampleLongTair() throws IOException, SQLException, InterruptedException {
        String testCaseFile = "rebalance_database_split_long_tair.test.yml";
        runPartitionBalanceTest(testCaseFile);

    }

    @Test
    public void runTestCaseRebalanceDatabaseMassiveTableGroup() throws IOException, SQLException, InterruptedException {
        String testCaseFile = "rebalance_massive_tablegroup.test.yml";
        runPartitionBalanceTest(testCaseFile);
    }

    @Test
    public void runTestCaseAutoBalanceSingleTable() throws IOException, SQLException, InterruptedException {
        String testCaseFile = "rebalance_auto_balance_single_table.test.yml";
        runPartitionBalanceTest(testCaseFile);

    }

    @Test
    public void runTestCaseAutoBalanceSingleTableAndPartitionTable()
        throws IOException, SQLException, InterruptedException {
        String testCaseFile = "rebalance_auto_balance_single_table_and_partition_table.test.yml";
        runPartitionBalanceTest(testCaseFile);

    }

    @Test
    public void runTestCaseSplitKeyPartitionTable() throws IOException, SQLException, InterruptedException {
        String testCaseFile = "rebalance_split_key_partition_table.test.yml";
        runPartitionBalanceTest(testCaseFile);

    }

}
