/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.locality;

import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalityTestCaseTask;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileNotFoundException;

@NotThreadSafe
public class LocalityTestCase extends LocalityTestBase {
    public void runTestCase(String resourceFile) throws FileNotFoundException {
        String resourceDir = "partition/env/LocalityTest/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        LocalityTestCaseTask localityTestCaseTask = new LocalityTestCaseTask(fileDir);
        localityTestCaseTask.execute(tddlConnection);
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
    public void testCreateSimpleTable() throws FileNotFoundException {
        runTestCase("create_simple_table.test.yml");
    }

    @Test
    public void testListPartitionTableOperation() throws FileNotFoundException {
        runTestCase("list_partition_table_operation.test.yml");
    }

    @Test
    public void testSingleTableOperation() throws FileNotFoundException {
        runTestCase("single_table_operation.test.yml");
    }

    @Test
    public void testCreateTableGroupSimple() throws FileNotFoundException {
        runTestCase("create_tablegroup_simple.test.yml");
    }

    @Test
    public void testAlterTableGroupFirstSimple() throws FileNotFoundException {
        runTestCase("alter_tablegroup_first_simple.test.yml");
    }

    @Test
    @Ignore("ignore force dn")
    public void testAlterTableGroupSecondSimple() throws FileNotFoundException {
        runTestCase("alter_tablegroup_second_simple.test.yml");
    }

    @Test
    public void testRepartitionSimple() throws FileNotFoundException {
        runTestCase("repartition_simple.test.yml");
    }

    @Test
    public void testAlterLocalitySimple() throws FileNotFoundException {
        runTestCase("alter_locality_simple.test.yml");
    }

    @Test
    public void testRebalanceSimple() throws FileNotFoundException {
        runTestCase("rebalance_simple.test.yml");
    }

    @Test
    public void testCreateOverrideTable() throws FileNotFoundException {
        runTestCase("create_override_table.test.yml");
    }

    @Test
    public void testAlterLocalityOverride() throws FileNotFoundException {
        runTestCase("alter_locality_override.test.yml");
    }

    @Test
    public void testCreateSingleTableBalance() throws FileNotFoundException {
        runTestCase("create_single_table_balance.test.yml");
    }

    @Test
    public void testTruncate() throws FileNotFoundException {
        runTestCase("truncate.test.yml");
    }
}
