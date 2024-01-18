package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class AlterTableGroupAddSubPartNonTemplateTest extends AlterTableGroupSubPartitionTest {

    public AlterTableGroupAddSubPartNonTemplateTest(PartitionRuleInfo partitionRuleInfo) {
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

        String addDefaultPartForKey = alterPrefix + " add partition (partition p4 values %s (%s) subpartitions 4)";
        String addDefaultPartForOthers = alterPrefix + " add partition (partition p4 values %s (%s))";

        String addPartition1 = alterPrefix + " add partition (partition p4 values %s (%s) (";
        String addPartition2 = "subpartition p4sp1 values %s (%s), subpartition p4sp2 values %s (%s)))";

        String addSubPartition =
            alterPrefix + " modify partition p3 add subpartition (subpartition sp4 values %s (%s))";

        List<String> addPartitionsForKey = new ArrayList<>();
        addPartitionsForKey.add(String.format(addDefaultPartForKey, "less than", "2034"));
        addPartitionsForKey.add(String.format(addDefaultPartForKey, "less than", "'2034-01-01 00:00:00',41"));
        addPartitionsForKey.add(String.format(addDefaultPartForKey, "in", "4"));
        addPartitionsForKey.add(String.format(addDefaultPartForKey, "in", "(4,'abc4')"));

        List<String> addPartitionsForOthers = new ArrayList<>();
        addPartitionsForOthers.add(String.format(addDefaultPartForOthers, "less than", "2034"));
        addPartitionsForOthers.add(String.format(addDefaultPartForOthers, "less than", "'2034-01-01 00:00:00',41"));
        addPartitionsForOthers.add(String.format(addDefaultPartForOthers, "in", "4"));
        addPartitionsForOthers.add(String.format(addDefaultPartForOthers, "in", "(4,'abc4')"));

        List<String> addPartitions1 = new ArrayList<>();
        addPartitions1.add(String.format(addPartition1, "less than", "2034"));
        addPartitions1.add(String.format(addPartition1, "less than", "'2034-01-01 00:00:00',41"));
        addPartitions1.add(String.format(addPartition1, "in", "4"));
        addPartitions1.add(String.format(addPartition1, "in", "(4,'abc4')"));

        List<String> addPartitions2 = new ArrayList<>();
        addPartitions2.add(String.format(addPartition2, "less than", "2031", "less than", "2032"));
        addPartitions2.add(String.format(addPartition2, "less than", "1,'2011-01-01 00:00:00'", "less than",
            "2,'2012-01-01 00:00:00'"));
        addPartitions2.add(String.format(addPartition2, "in", "11", "in", "21"));
        addPartitions2.add(String.format(addPartition2, "in", "(11,'def1')", "in", "(21,'def2')"));

        List<String> addSubPartitions = new ArrayList<>();
        addSubPartitions.add(String.format(addSubPartition, "less than", "2034"));
        addSubPartitions.add(String.format(addSubPartition, "less than", "40,'2034-01-01 00:00:00'"));
        addSubPartitions.add(String.format(addSubPartition, "in", "41"));
        addSubPartitions.add(String.format(addSubPartition, "in", "(41,'def4')"));

        List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>();

        int initDataType = 5;
        boolean ignoreInit = true;

        for (int i = 1; i < partitions.length; i++) {
            for (int j = 0; j < subPartitions.length; j++) {
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    j == 0 ? addPartitionsForKey.get(i - 1) : addPartitionsForOthers.get(i - 1)
                ));
                if (j != 0) {
                    partitionRuleInfos.add(new PartitionRuleInfo(
                        partStrategies[i], initDataType, ignoreInit,
                        tPartitionRules.get(partitions[i] + subPartitions[j]),
                        addPartitions1.get(i - 1) + addPartitions2.get(j - 1)
                    ));
                    partitionRuleInfos.add(new PartitionRuleInfo(
                        partStrategies[i], initDataType, ignoreInit,
                        tPartitionRules.get(partitions[i] + subPartitions[j]),
                        addSubPartitions.get(j - 1)
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
