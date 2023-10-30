package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class AlterTableGroupDropSubPartNonTemplateTest extends AlterTableGroupSubPartitionTest {

    public AlterTableGroupDropSubPartNonTemplateTest(PartitionRuleInfo partitionRuleInfo) {
        super(partitionRuleInfo);
    }

    @Parameterized.Parameters(name = "{index}:partitionRuleInfo={0}")
    public static List<PartitionRuleInfo[]> prepareData() {
        List<PartitionRuleInfo[]> status = new ArrayList<>();
        tableStatus.stream().forEach(c -> {
            getPartitionRuleInfos().stream().forEach(o -> {
                PartitionRuleInfo pi = new PartitionRuleInfo(o.strategy, o.initDataType, o.ignoreInit, o.partitionRule,
                    o.alterTableGroupCommand);
                pi.setTableStatus(c);
                status.add(new PartitionRuleInfo[] {pi});
            });
        });
        return status;
    }

    protected static List<PartitionRuleInfo> getPartitionRuleInfos() {
        List<PartitionRuleInfo> partitionRuleInfos = getPartitionRuleInfos(alterTableGroup);
        partitionRuleInfos.addAll(getPartitionRuleInfos(alterTable));
        return partitionRuleInfos;
    }

    protected static List<PartitionRuleInfo> getPartitionRuleInfos(String alterPrefix) {
        Map<String, String> tPartitionRules = getNonTemplatedCombinations();

        String dropPartition = alterPrefix + " drop partition p2";
        String dropSubPartition = alterPrefix + " modify partition p3 drop subpartition p3sp2";

        List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>();

        int initDataType = 5;
        boolean ignoreInit = true;

        for (int i = 1; i < partitions.length; i++) {
            for (int j = 0; j < subPartitions.length; j++) {
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    dropPartition
                ));
                if (j != 0) {
                    partitionRuleInfos.add(new PartitionRuleInfo(
                        partStrategies[i], initDataType, ignoreInit,
                        tPartitionRules.get(partitions[i] + subPartitions[j]),
                        dropSubPartition
                    ));
                }
            }
        }

        return partitionRuleInfos;
    }

    @Test
    public void testDDLOnly() {
        String sql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeQuery("select * from t2", tddlConnection);
    }

}
