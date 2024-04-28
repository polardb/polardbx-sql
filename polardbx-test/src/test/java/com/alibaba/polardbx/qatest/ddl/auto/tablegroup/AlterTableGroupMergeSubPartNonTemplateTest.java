package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class AlterTableGroupMergeSubPartNonTemplateTest extends AlterTableGroupSubPartitionTest {

    private static final String MERGE_LOGICAL_PARTITION_GROUP = alterTableGroup + " merge partitions p2,p1 to p2";
    private static final String MERGE_LOGICAL_PARTITION = alterTable + " merge partitions p2,p1 to p2";
    private static final String MERGE_PHY_PARTITION_GROUP =
        alterTableGroup + " merge subpartitions p2sp2,p2sp1 to p2sp2";
    private static final String MERGE_PHY_PARTITION = alterTable + " merge subpartitions p2sp2,p2sp1 to p2sp2";

    public AlterTableGroupMergeSubPartNonTemplateTest(PartitionRuleInfo partitionRuleInfo) {
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
                    MERGE_LOGICAL_PARTITION_GROUP,
                    "p2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    MERGE_LOGICAL_PARTITION,
                    "p2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    MERGE_PHY_PARTITION_GROUP,
                    "p2sp2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    MERGE_PHY_PARTITION,
                    "p2sp2"
                ));
            }
        }

        return partitionRuleInfos;
    }

}
