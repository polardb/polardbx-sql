package com.alibaba.polardbx.qatest.dal;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * <a href="https://dev.mysql.com/doc/refman/8.0/en/information-functions.html">InformationFunction</a>
 */
public class InformationFunctionTest extends ReadBaseTestCase {

    @Test
    public void testSelectCurrentUser() throws SQLException {
        String sql = "select current_user();";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String currentUser = rs.getString(1);
        Assert.assertTrue(StringUtils.startsWith(currentUser, ConnectionManager.getInstance().getPolardbxUser()));
    }

    @Test
    public void databaseTest() throws SQLException {
        String sql = "select database();";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String database = rs.getString(1);
        Assert.assertEquals(PropertiesUtil.polardbXDBName1(usingNewPartDb()), database);
    }
}
