package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class AlterTableGroupMoveSubPartNonTemplateTest extends AlterTableGroupSubPartitionTest {

    public AlterTableGroupMoveSubPartNonTemplateTest(PartitionRuleInfo partitionRuleInfo) {
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

    protected static List<PartitionRuleInfo> getPartitionRuleInfos() {
        List<PartitionRuleInfo> partitionRuleInfos = getPartitionRuleInfos(alterTableGroup);
        partitionRuleInfos.addAll(getPartitionRuleInfos(alterTable));
        return partitionRuleInfos;
    }

    protected static List<PartitionRuleInfo> getPartitionRuleInfos(String alterPrefix) {
        Map<String, String> tPartitionRules = getNonTemplatedCombinations();

        String movePartition = alterPrefix + " move partitions p2 to '%s'";
        String moveSubPartition = alterPrefix + " move subpartitions p3sp1,p3sp2,p3sp3 to '%s'";

        List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>();

        int initDataType = 5;
        boolean ignoreInit = true;

        for (int i = 1; i < partitions.length; i++) {
            for (int j = 1; j < subPartitions.length; j++) {
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    movePartition,
                    "p2"
                ));
                partitionRuleInfos.add(new PartitionRuleInfo(
                    partStrategies[i], initDataType, ignoreInit,
                    tPartitionRules.get(partitions[i] + subPartitions[j]),
                    moveSubPartition,
                    "p3sp2"
                ));
            }
        }

        return partitionRuleInfos;
    }

    @Before
    @Override
    public void setUpTables() {
        if (firstIn) {
            setUp(true, partitionRuleInfo, false, true, true);
            firstIn = false;
        }
        partitionRuleInfo.connection = getTddlConnection1();
        String sql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testDDLOnly() {
        String sql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeQuery("select * from t2", tddlConnection);
    }
}
