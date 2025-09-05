package com.alibaba.polardbx.qatest.storagepool;

import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.storagepool.LegacyStoragePoolTestCase.StoragePoolTestCaseTask;
import com.alibaba.polardbx.qatest.storagepool.importDatabase.AlterPartitionTest;
import com.alibaba.polardbx.qatest.storagepool.importDatabase.CommonDdlTest;
import com.alibaba.polardbx.qatest.storagepool.importDatabase.HintTest;
import com.alibaba.polardbx.qatest.storagepool.importDatabase.ImportDatabaseRebalanceTest;
import com.alibaba.polardbx.qatest.storagepool.importDatabase.ImportTableTest;
import com.alibaba.polardbx.qatest.storagepool.importDatabase.RepartitionTest;
import net.jcip.annotations.NotThreadSafe;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileNotFoundException;

@NotThreadSafe
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoragePoolTestCase extends LocalityTestBase {
    public void runTestCase(String resourceFile) throws FileNotFoundException, InterruptedException {
        String resourceDir = "partition/env/StoragePoolTest/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        StoragePoolTestCaseTask storagePoolTestCaseTask = new StoragePoolTestCaseTask(fileDir);
        storagePoolTestCaseTask.execute(tddlConnection);
    }

    /**
     * import database 的case，需要多个DN
     * 因此测试case放在存储池目录下
     */
    @Test
    public void testCase001ImportTable() {
        ImportTableTest testcase = new ImportTableTest();
        testcase.runTestCases();
    }

    @Test
    public void testCase002ImportDatabaseRebalance() {
        ImportDatabaseRebalanceTest testcase = new ImportDatabaseRebalanceTest();
        testcase.runTestCases();
    }

    @Test
    public void testCase003ImportDatabaseRepartition() {
        RepartitionTest testcase = new RepartitionTest();
        testcase.runTestCases();
    }

    @Test
    public void testCase004ImportDatabaseAlterPartition() {
        AlterPartitionTest testcase = new AlterPartitionTest();
        testcase.runTestCases();
    }

    @Test
    public void testCase005ImportDatabaseCommonDdl() {
        CommonDdlTest testcase = new CommonDdlTest();
        testcase.runTestCases();
    }

    @Test
    public void testCase006ImportDatabaseHintTest() {
        HintTest testcase = new HintTest();
        testcase.runTestCases();
    }


    /*
     * for create partition table.
     * (hash_partition, range_partition, list_partition)
     * (full_part_spec, non_full_part_spec)
     * (int_partition_key, string_partition_key)
     * (with_gsi, without_gsi)
     * (table_level_locality, partition_level_localiy, table_and_partition_level_locality, no_locality)
     *
     * for create other table
     * (broadcast_table, single_table)
     * (with_gsi, without_gsi)
     *
     * for repartition
     * (broad->single, broad->partition, partition->single, partiton->broadcast, single->broad, single->partition)
     *
     * for modify partition
     * (move, add, split, merge, split_by_hot_value, extract)
     *
     * for set locality
     *
     * for rebalance
     */

    @Test
    public void testCase01StoragePoolDemo() throws FileNotFoundException, InterruptedException {
        runTestCase("storage_pool_init.test.yml");
    }

    @Test
    public void testCase02ListPartitionTableOperation() throws FileNotFoundException, InterruptedException {
        runTestCase("list_partition_table_storage_pool.test.yml");
    }

    @Test
    public void testCase03HashPartitionTableOperation() throws FileNotFoundException, InterruptedException {
        runTestCase("hash_partition_table_storage_pool.test.yml");
    }

    @Test
    public void testCase04SingleTableOperation() throws FileNotFoundException, InterruptedException {
        runTestCase("single_table_storage_pool.test.yml");
    }

    @Test
    public void testCase05ControlAdapter() throws FileNotFoundException, InterruptedException {
        runTestCase("control_adapter.test.yml");
    }
}
