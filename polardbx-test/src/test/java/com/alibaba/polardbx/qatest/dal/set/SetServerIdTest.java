package com.alibaba.polardbx.qatest.dal.set;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * created by ziyang.lb
 * Test Server Id
 **/
public class SetServerIdTest extends DirectConnectionBaseTestCase {

    @Test
    public void testSetServerId() throws SQLException, InterruptedException {
        long originServerId = getServerId(false, tddlConnection);
        long originGmsServerId = getServerId(false, getMetaConnection());
        long newServerId = RandomUtils.getLongBetween(originServerId + 1, originServerId + 1000);

        JdbcUtil.executeQueryFaied(tddlConnection, "set server_id = 5678",
            "Variable 'server_id' is a GLOBAL variable and should be set with SET GLOBAL");
        JdbcUtil.executeQueryFaied(tddlConnection, "set global server_id = abc",
            "Incorrect argument type to variable 'server_id'");
        JdbcUtil.executeSuccess(tddlConnection, "set global server_id = " + newServerId);

        Assert.assertEquals(newServerId, getServerId(false, tddlConnection));
        Assert.assertEquals(newServerId, getServerId(true, tddlConnection));
        Assert.assertEquals(newServerId, getServerIdWithoutLike(false, tddlConnection));
        Assert.assertEquals(newServerId, getServerIdWithoutLike(true, tddlConnection));
        Assert.assertEquals(newServerId, selectServerIdGlobal(tddlConnection));
        Assert.assertEquals(originGmsServerId, getServerId(false, getMetaConnection()));
        Assert.assertEquals(originGmsServerId, getServerId(true, getMetaConnection()));
        Assert.assertNotEquals(newServerId, originGmsServerId);
    }

    private long getServerId(boolean withGlobal, Connection connection) throws SQLException {
        String getServerIdSql;
        if (withGlobal) {
            getServerIdSql = "show global variables like 'server_id'";
        } else {
            getServerIdSql = "show variables like 'server_id'";
        }

        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuery(getServerIdSql, connection);
            if (rs.next()) {
                Assert.assertEquals("server_id", rs.getString(1));
                return rs.getLong(2);
            } else {
                throw new RuntimeException("server_id can`t be null");
            }
        } finally {
            JdbcUtil.close(rs);
        }
    }

    private long getServerIdWithoutLike(boolean withGlobal, Connection connection) throws SQLException {
        String getServerIdSql;
        if (withGlobal) {
            getServerIdSql = "show global variables";
        } else {
            getServerIdSql = "show variables";
        }

        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuery(getServerIdSql, connection);
            while (rs.next()) {
                String name = rs.getString(1);
                if (StringUtils.equals(name, "server_id")) {
                    return rs.getLong(2);
                }
            }
            throw new RuntimeException("server_id can`t be null");

        } finally {
            JdbcUtil.close(rs);
        }
    }

    private long selectServerIdGlobal(Connection connection) throws SQLException, InterruptedException {
        Thread.sleep(10000);
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuery("select @@GLOBAL.server_id", connection);
            while (rs.next()) {
                return rs.getLong(1);
            }
            throw new RuntimeException("server_id can`t be null");

        } finally {
            JdbcUtil.close(rs);
        }
    }
}
