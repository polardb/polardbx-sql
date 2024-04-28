package com.alibaba.polardbx.qatest.ddl.cdc.repartition;

import org.junit.Test;

import java.sql.SQLException;
import java.util.Set;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-14 16:18
 **/
public class CdcRePartitionSub_FirstListColumnTest extends CdcRePartitionSub_FirstBaseTest {

    public CdcRePartitionSub_FirstListColumnTest() {
        dbName = "cdc_sub_partition_first_list_c";
    }

    @Test
    public void testHashPartition() throws SQLException {
        testRePartitionDdl();
    }

    @Override
    protected Set<SubPartitionType> getSubPartitionTypes() {
        return ListColumnsPartitionSet;
    }
}
