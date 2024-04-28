package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class ShowCommandTest extends ReadBaseTestCase {
    @Test
    public void testShowConnection() {
        String sql = "show connection;";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show full connection;";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}
