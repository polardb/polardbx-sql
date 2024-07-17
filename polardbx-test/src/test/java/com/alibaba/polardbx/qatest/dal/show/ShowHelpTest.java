package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ShowHelpTest extends ReadBaseTestCase {
    @Test
    public void testShowHelp() throws SQLException {
        String sql = "select * from information_schema.show_help;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertTrue(rs.next());
    }
}
