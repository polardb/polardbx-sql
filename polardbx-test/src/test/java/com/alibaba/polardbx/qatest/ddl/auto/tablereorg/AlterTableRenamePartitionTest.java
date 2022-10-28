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

package com.alibaba.polardbx.qatest.ddl.auto.tablereorg;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@RunWith(Parameterized.class)
@NotThreadSafe
public class AlterTableRenamePartitionTest extends AlterTableReorgBaseTest {

    private static List<ComplexTaskMetaManager.ComplexTaskStatus> tableStatus =
        Stream.of(
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC).collect(Collectors.toList());

    static List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>(Arrays
        .asList(
            new PartitionRuleInfo(PartitionStrategy.KEY,
                1,
                PARTITION_BY_BIGINT_KEY,
                ""),
            new PartitionRuleInfo(PartitionStrategy.KEY,
                2,
                PARTITION_BY_INT_KEY, ""),
            new PartitionRuleInfo(PartitionStrategy.KEY,
                2,
                PARTITION_BY_INT_BIGINT_KEY, ""),
            new PartitionRuleInfo(PartitionStrategy.HASH,
                2,
                PARTITION_BY_INT_BIGINT_HASH, ""),
            new PartitionRuleInfo(PartitionStrategy.HASH,
                3,
                PARTITION_BY_MONTH_HASH, ""),
            new PartitionRuleInfo(PartitionStrategy.RANGE,
                1,
                PARTITION_BY_BIGINT_RANGE, ""),
            new PartitionRuleInfo(PartitionStrategy.RANGE_COLUMNS,
                2,
                PARTITION_BY_INT_BIGINT_RANGE_COL, ""),
            new PartitionRuleInfo(PartitionStrategy.LIST,
                1,
                PARTITION_BY_BIGINT_LIST, ""),
            new PartitionRuleInfo(PartitionStrategy.LIST_COLUMNS,
                4,
                PARTITION_BY_INT_BIGINT_LIST, ""))
    );

    private static PartitionRuleInfo partitionRuleInfo;
    static boolean firstIn = true;
    final static String logicalDatabase = "AlterTableRenamePartition";
    final static List<String> partitionNames = Lists.newArrayList("p1", "p2", "p3");

    public AlterTableRenamePartitionTest(PartitionRuleInfo partitionRuleInfo) {
        super(logicalDatabase, ImmutableList.of(partitionRuleInfo.getTableStatus().name()));
        this.partitionRuleInfo = partitionRuleInfo;
        firstIn = true;
    }

    @Test
    public void testRenameAndSetTg() {
        int time = 10;
        int numPart = partitionNames.size();
        do {
            Random r = new Random();
            int changeParts = Math.abs(r.nextInt()) % numPart + 1;
            String sql = generateRenameSql(changeParts, true);
            executeDml(sql, tddlConnection);
            executeDml("select * from " + tableName, tddlConnection);
            time--;
        } while (time > 0);

        executeDml("alter table " + tableName + " rename partition p2 to p200", tddlConnection);
        executeDml("alter table " + tableName + " set tablegroup=" + tableGroupName + " force", tddlConnection);

        time = 5;
        do {
            Random r = new Random();
            int changeParts = Math.abs(r.nextInt()) % numPart + 1;
            String sql = generateRenameSql(changeParts, false);
            executeDml(sql, tddlConnection);
            executeDml("select * from " + tableName, tddlConnection);
            time--;
        } while (time > 0);
    }

    private String generateRenameSql(int changeParts, boolean alterTable) {
        StringBuilder sb = new StringBuilder();
        sb.append("alter");
        if (alterTable) {
            sb.append(" table ");
            sb.append(tableName);
        } else {
            sb.append(" tablegroup ");
            sb.append(tableGroupName);
        }
        sb.append(" rename partition ");
        int i = 0;
        do {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(partitionNames.get(i));
            sb.append(" to ");
            i++;
            if (i < changeParts) {
                sb.append(partitionNames.get(i % partitionNames.size()));
            } else {
                sb.append(partitionNames.get(0));
            }
        } while (i < changeParts);
        return sb.toString();
    }

    @Parameterized.Parameters(name = "{index}:partitionRuleInfo={0}")
    public static List<PartitionRuleInfo[]> prepareData() {
        List<PartitionRuleInfo[]> status = new ArrayList<>();
        tableStatus.stream().forEach(c -> {
            partitionRuleInfos.stream().forEach(o -> {
                PartitionRuleInfo pi =
                    new PartitionRuleInfo(o.strategy, o.initDataType, o.partitionRule, o.alterCommand);
                pi.setTableStatus(c);
                status.add(new PartitionRuleInfo[] {pi});
            });
        });
        return status;
    }

    @Before
    public void setUpTables() {
        if (firstIn) {
            setUpForRename(true, partitionRuleInfo, false);
            firstIn = false;
        }
        partitionRuleInfo.connection = getTddlConnection1();
        String sql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }
}
