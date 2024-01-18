package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_KEY_NAME;

/**
 * @author fangwu
 */
public class SqlShowFullColumnsTest extends BaseTestCase {

    @Test
    public void testShowFullColumnsWithoutImplictKey() throws SQLException {
        String tblSql = " CREATE TABLE if not exists `showfullcolumnstest` (\n"
            + "\t`id` int(11) NOT NULL\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 0 DEFAULT CHARSET = utf8mb4";

        String testSql = "show full columns from showfullcolumnstest like '%'";
        try (Connection conn = getPolardbxConnection()) {
            // create table with IMPLICIT_KEY
            conn.createStatement().execute(tblSql);

            ResultSet rs = conn.createStatement().executeQuery(testSql);
            boolean hasOutput = false;
            while (rs.next()) {
                hasOutput = true;
                // test if any implicit colmuns output
                Assert.assertTrue(!rs.getString("field").equalsIgnoreCase(IMPLICIT_KEY_NAME));
                Assert.assertTrue(!rs.getString("field").equalsIgnoreCase(IMPLICIT_COL_NAME));
            }
            // test if no output
            Assert.assertTrue(hasOutput);
            conn.createStatement().execute("drop table if exists showfullcolumnstest");
        } catch (SQLException e) {
            getPolardbxConnection().createStatement().execute("drop table if exists showfullcolumnstest");
            throw e;
        }
    }
}
