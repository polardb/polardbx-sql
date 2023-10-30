package com.alibaba.polardbx.qatest.dal.alter;

import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class AlterSystemLeaderTest extends DirectConnectionBaseTestCase {

    private void executeUpdateSql(String sql, List<Object> param, Connection tddlConnection) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        try {
            tddlPs.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(tddlPs);
        }
    }

    @Test
    public void testAlterSystemLeader1() {
        String sql = "alter system leader '127.0.0.1:3326';";

        executeUpdateSql(sql, null, tddlConnection);
    }

    @Test(expected = RuntimeException.class)
    public void testAlterSystemLeader2() {
        String sql = "alter system leader 127.0.0.1:3326;";

        executeUpdateSql(sql, null, tddlConnection);
    }

}
