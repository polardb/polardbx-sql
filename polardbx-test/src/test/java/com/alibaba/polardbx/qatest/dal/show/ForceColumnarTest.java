package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Test;

public class ForceColumnarTest extends AutoReadBaseTestCase {
    final String SCHEMA = "drds_polarx1_part_qatest_app";
    final String TABLE = "select_base_diff_shard_key_one_db_one_tb";

    @Test
    public void testCheckColumnarPartition() {
        if (!PropertiesUtil.columnarMode()) {
            return;
        }
        String sql = String.format("/*+TDDL:cmd_extra(workload_type=ap)*/ select * from %s.%s ", SCHEMA, TABLE);
        String explain = getExplainResult(tddlConnection, sql);
        Assert.assertTrue(explain.toLowerCase().contains("osstablescan"),
            "columnar plan should contains oss table scan, but was " + explain);
    }
}
