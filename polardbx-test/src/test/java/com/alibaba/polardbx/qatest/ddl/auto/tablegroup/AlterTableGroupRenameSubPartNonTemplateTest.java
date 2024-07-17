package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class AlterTableGroupRenameSubPartNonTemplateTest extends AlterTableGroupSubPartitionTest {

    public AlterTableGroupRenameSubPartNonTemplateTest(PartitionRuleInfo partitionRuleInfo) {
        super(partitionRuleInfo);
    }

    @Parameterized.Parameters(name = "{index}:partitionRuleInfo={0}")
    public static List<PartitionRuleInfo[]> prepareData() {
        List<PartitionRuleInfo[]> status = new ArrayList<>();
        tableStatus.stream().forEach(c -> {
            getPartitionRuleInfos().stream().forEach(o -> {
                PartitionRuleInfo pi = new PartitionRuleInfo(o.strategy, o.initDataType, o.ignoreInit, o.partitionRule,
                    o.alterTableGroupCommand, o.targetPart);
                pi.setTableStatus(c);
                status.add(new PartitionRuleInfo[] {pi});
            });
        });
        return status;
    }

    @Test
    public void testDDLOnly() {
        String sql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeQuery("select * from t2", tddlConnection);
    }

    protected static List<PartitionRuleInfo> getPartitionRuleInfos() {
        Map<String, String> tPartitionRules = getNonTemplatedCombinations();

        List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>();

        int initDataType = 5;
        boolean ignoreInit = false;

        for (int i = 1; i < partitions.length; i++) {
            for (int j = 0; j < subPartitions.length; j++) {
                initDataType++;
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    alterTableGroup + " rename partition p2 to pxx2x",
                    "p2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    alterTable + " rename partition p2 to pxx2x",
                    "p2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    alterTableGroup + " rename subpartition p2sp2 to p2sp2xx",
                    "p2sp2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    alterTable + " rename subpartition p2sp2 to p2sp2xx",
                    "p2sp2"
                ));
            }
        }

        return partitionRuleInfos;
    }

}
