package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class AlterTableGroupSplitSubPartNonTemplateTest extends AlterTableGroupSubPartitionTest {

    public AlterTableGroupSplitSubPartNonTemplateTest(PartitionRuleInfo partitionRuleInfo) {
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
        Map<String, String> splitPartitionCommands = getNonTemplatedSplitPartCombinations();
        Map<String, String> splitSubPartitionCommands = getNonTemplatedSplitSubPartCombinations();

        List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>();

        int initDataType = 5;
        boolean ignoreInit = false;

        for (int i = 1; i < partitions.length; i++) {
            for (int j = 0; j < subPartitions.length; j++) {
                initDataType++;
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    alterTableGroup + " " + splitPartitionCommands.get(partitions[i] + subPartitions[j]),
                    "p2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    alterTable + " " + splitPartitionCommands.get(partitions[i] + subPartitions[j]),
                    "p2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    alterTableGroup + " " + splitSubPartitionCommands.get(partitions[i] + subPartitions[j]),
                    "p2sp2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    alterTable + " " + splitSubPartitionCommands.get(partitions[i] + subPartitions[j]),
                    "p2sp2"
                ));
            }
        }

        return partitionRuleInfos;
    }

}
