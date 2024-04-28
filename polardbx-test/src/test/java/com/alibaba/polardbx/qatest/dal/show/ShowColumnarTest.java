package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Ignore;
import org.junit.Test;

public class ShowColumnarTest extends AutoReadBaseTestCase {
    @Test
    public void testShowColumnarStatus() {
        String sql = "show columnar status;";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show columnar status where tso = 1;";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show full columnar status;";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show full columnar status where tso = 783276;";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        JdbcUtil.useDb(tddlConnection, "polardbx");
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Test
    @Ignore("Maybe no cdc")
    public void testShowColumnarOffset() {
        String sql = "show columnar offset;";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}
