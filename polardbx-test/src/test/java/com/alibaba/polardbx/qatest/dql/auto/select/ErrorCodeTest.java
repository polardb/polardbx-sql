package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author fangwu
 */
public class ErrorCodeTest extends BaseTestCase {
    private static final String tblName = "tbl_not_exists";

    @Test
    public void testTableNotExists() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop table if exists " + tblName);
            c.createStatement().executeQuery("select * from " + tblName);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = e.getMessage();
            System.out.println(msg);
            Assert.assertTrue(
                msg.contains("ERR-CODE: [PXC-4006][ERR_TABLE_NOT_EXIST] Table 'tbl_not_exists' doesn't exist"));
            Assert.assertTrue(msg.indexOf("ERR-CODE") == msg.lastIndexOf("ERR-CODE"));
        }
    }

    @Test
    public void testCantChangeIsolation() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("begin");
            c.createStatement().executeQuery("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("PXC-25001"));
        }
    }

    @Test
    public void testSqlParserError() {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("begin");
            c.createStatement().executeQuery("select * from t1 xxx xxx");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("[PXC-4500][ERR_PARSER]"));
        }
    }
}
