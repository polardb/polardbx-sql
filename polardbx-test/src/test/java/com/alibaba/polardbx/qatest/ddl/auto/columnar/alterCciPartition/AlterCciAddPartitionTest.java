/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar.alterCciPartition;

import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
@NotThreadSafe
public class AlterCciAddPartitionTest extends AlterCciPartitionBaseTest {
    static List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>(Arrays
        .asList(
            new PartitionRuleInfo(PartitionStrategy.RANGE,
                1,
                PARTITION_BY_BIGINT_RANGE,
                "alter table " + tableName + "." + cciName + " add partition (partition p9 values less than(100400))"),
            new PartitionRuleInfo(PartitionStrategy.RANGE_COLUMNS,
                2,
                PARTITION_BY_INT_BIGINT_RANGE_COL,
                "alter table " + tableName + "." + cciName
                    + " add partition (partition p9 values less than(400, 100400))"),
            new PartitionRuleInfo(PartitionStrategy.LIST,
                1,
                PARTITION_BY_BIGINT_LIST,
                "alter table " + tableName + "." + cciName
                    + " add partition (partition p9 values in (100300,100301,100302,100303,100304,100305))"),
            new PartitionRuleInfo(PartitionStrategy.LIST_COLUMNS,
                4,
                PARTITION_BY_INT_BIGINT_LIST, "alter table " + tableName + "." + cciName
                + " add partition (partition p9 values in ((1,100300),(1,100301),(1,100302),(1,100303),(1,100304),(1,100305)))"))
    );

    private static PartitionRuleInfo partitionRuleInfo;
    static boolean firstIn = true;
    final static String logicalDatabase = "AlterCciAddPartition";

    public AlterCciAddPartitionTest(PartitionRuleInfo partitionRuleInfo) {
        super(logicalDatabase);
        this.partitionRuleInfo = partitionRuleInfo;
        firstIn = true;
    }

    @Test
    public void testDDLOnly() {

    }

    @Parameterized.Parameters(name = "{index}:partitionRuleInfo={0}")
    public static List<PartitionRuleInfo[]> prepareData() {
        List<PartitionRuleInfo[]> status = new ArrayList<>();
        partitionRuleInfos.stream().forEach(o -> {
            PartitionRuleInfo pi =
                new PartitionRuleInfo(o.strategy, o.initDataType, o.partitionRule,
                    o.alterCommand);
            status.add(new PartitionRuleInfo[] {pi});
        });
        return status;
    }

    @Before
    public void setUpTables() {
        if (firstIn) {
            setUp(true, partitionRuleInfo, false);
            firstIn = false;
        }
        partitionRuleInfo.connection = getTddlConnection1();
        String sql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }
}
