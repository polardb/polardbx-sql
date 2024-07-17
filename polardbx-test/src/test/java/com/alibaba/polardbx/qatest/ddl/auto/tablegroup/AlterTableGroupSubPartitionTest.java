package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NotThreadSafe
public class AlterTableGroupSubPartitionTest extends AlterTableGroupBaseTestExt {

    protected static final String logicalDatabase = "AlterTableGroupSubPartitionTest";

    protected static List<ComplexTaskMetaManager.ComplexTaskStatus> tableStatus = Stream.of(
        ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
        ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
        ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
        ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC
    ).collect(Collectors.toList());

    protected static PartitionStrategy[] partStrategies = new PartitionStrategy[] {
        PartitionStrategy.KEY, PartitionStrategy.RANGE, PartitionStrategy.RANGE_COLUMNS, PartitionStrategy.LIST,
        PartitionStrategy.LIST_COLUMNS
    };

    protected static String[] partitions = new String[] {
        PART_BY_KEY, PART_BY_RANGE, PART_BY_RANGE_COLUMNS, PART_BY_LIST, PART_BY_LIST_COLUMNS
    };

    protected static String[] subPartitions = new String[] {
        SUBPART_BY_KEY, SUBPART_BY_RANGE, SUBPART_BY_RANGE_COLUMNS, SUBPART_BY_LIST, SUBPART_BY_LIST_COLUMNS
    };

    protected static final String alterTableGroup = "alter tablegroup " + tableGroupName;
    protected static final String alterTable = "alter table t2";

    protected final PartitionRuleInfo partitionRuleInfo;
    protected boolean firstIn = true;

    public AlterTableGroupSubPartitionTest(PartitionRuleInfo partitionRuleInfo) {
        super(logicalDatabase, ImmutableList.of(partitionRuleInfo.getTableStatus().name()));
        this.partitionRuleInfo = partitionRuleInfo;
    }

    @Before
    public void setUpTables() {
        if (firstIn) {
            setUp(true, partitionRuleInfo, false, true, false, true);
            firstIn = false;
        }
        partitionRuleInfo.connection = getTddlConnection1();
        String sql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

}
