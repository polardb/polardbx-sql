package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class CheckColumnarPartitionTest extends AutoReadBaseTestCase {
    final String SCHEMA = "drds_polarx1_part_qatest_app";
    final String TABLE = "select_base_diff_shard_key_one_db_one_tb";

    @Test
    public void testCheckColumnarPartition() {
        String sql = String.format("check columnar partition %s.%s ", SCHEMA, TABLE);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}
