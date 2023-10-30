package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class AlterTableGroupAddSubPartTemplateTest extends AlterTableGroupSubPartitionTest {

    public AlterTableGroupAddSubPartTemplateTest(PartitionRuleInfo partitionRuleInfo) {
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
        Map<String, String> tPartitionRules = getTemplatedCombinations();

        String addPartition = alterPrefix + " add partition (partition p4 values %s (%s))";
        String addSubPartition = alterPrefix + " add subpartition (subpartition sp4 values %s (%s))";

        List<String> addPartitions = new ArrayList<>();
        addPartitions.add(String.format(addPartition, "less than", "2034"));
        addPartitions.add(String.format(addPartition, "less than", "'2034-01-01 00:00:00',41"));
        addPartitions.add(String.format(addPartition, "in", "4"));
        addPartitions.add(String.format(addPartition, "in", "(4,'abc4')"));

        List<String> addSubPartitions = new ArrayList<>();
        addSubPartitions.add(String.format(addSubPartition, "less than", "2034"));
        addSubPartitions.add(String.format(addSubPartition, "less than", "40,'2034-01-01 00:00:00'"));
        addSubPartitions.add(String.format(addSubPartition, "in", "41"));
        addSubPartitions.add(String.format(addSubPartition, "in", "(41,'def4')"));

        List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>();

        int initDataType = 5;
        boolean ignoreInit = true;

        for (int i = 0; i < partitions.length; i++) {
            for (int j = 1; j < subPartitions.length; j++) {
                if (i != 0) {
                    partitionRuleInfos.add(new PartitionRuleInfo(
                        partStrategies[i], initDataType, ignoreInit,
                        tPartitionRules.get(partitions[i] + subPartitions[j]),
                        addPartitions.get(i - 1)
                    ));
                }
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    addSubPartitions.get(j - 1)
                ));
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
