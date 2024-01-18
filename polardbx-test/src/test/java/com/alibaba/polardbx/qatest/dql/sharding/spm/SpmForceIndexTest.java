package com.alibaba.polardbx.qatest.dql.sharding.spm;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/**
 * This test is not a test of the effectiveness of Force Index in DN,
 * but a test of whether the behavior of Force/Ignore/Use Index is fixed in the execution plan cached in SPM.
 *
 * @author fangwu
 */
public class SpmForceIndexTest extends BaseTestCase {
    private static final String db = "spm_force_index";
    private static final String createTbl = "CREATE TABLE `SpmForceIndexTest` (\n"
        + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT BY GROUP,\n"
        + "\t`order_id` bigint(20) NOT NULL DEFAULT '0',\n"
        + "\t`agency_id` int(11) NOT NULL DEFAULT '0',\n"
        + "\t`order_date` int(11) DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`id`),\n"
        + "\tKEY `idx_order_id` (`order_id`),\n"
        + "\tKEY `i_agency` (`agency_id`),\n"
        + "\tKEY `i_order_date` (`order_date`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by UNI_HASH(`order_id`) tbpartition by UNI_HASH(`order_id`) tbpartitions 4";
    private static final String testSql1 = "select order_id from `SpmForceIndexTest` force index (idx_order_id) "
        + "where order_id > 0 AND agency_id = 2 ORDER BY id ASC";

    private static final String testSql2 =
        "select order_id from `SpmForceIndexTest` ignore index (idx_order_id, i_agency, primary) "
            + "where order_id > 0 AND agency_id = 2 ORDER BY id ASC";

    private static final String testSql3 =
        "select order_id from `SpmForceIndexTest` use index (idx_order_id, i_agency, primary) "
            + "where order_id > 0 AND agency_id = 2 ORDER BY id ASC";

    public SpmForceIndexTest() {
    }

    @Before
    public void prepareTable() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            Statement stmt = c.createStatement();
            stmt.execute("drop database if exists " + db);
            stmt.execute("create database if not exists " + db);
            stmt.execute("use " + db);
            stmt.execute(createTbl);
        }

    }

    @After
    public void clean() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            Statement stmt = c.createStatement();
            stmt.execute("drop database if exists " + db);
        }
    }

    @Test
    public void testForceIndex() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("use " + db);
            c.createStatement().execute("baseline add sql /*TDDL:a()*/ " + testSql1);
            StringBuilder sb = new StringBuilder();
            ResultSet rs = c.createStatement().executeQuery("explain " + testSql1);
            while (rs.next()) {
                sb.append(rs.getString(1));
            }
            String explain = sb.toString().toUpperCase(Locale.ROOT);
            Assert.assertTrue(explain.contains("SOURCE:SPM"), explain);
            Assert.assertTrue(explain.contains("FORCE INDEX(IDX_ORDER_ID)"), explain);
            c.createStatement().executeQuery(testSql1);
        }
    }

    @Test
    public void testIgnoreIndex() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("use " + db);
            c.createStatement().execute("baseline add sql /*TDDL:a()*/ " + testSql2);
            StringBuilder sb = new StringBuilder();
            ResultSet rs = c.createStatement().executeQuery("explain " + testSql2);
            while (rs.next()) {
                sb.append(rs.getString(1));
            }
            String explain = sb.toString().toUpperCase(Locale.ROOT);
            Assert.assertTrue(explain.contains("SOURCE:SPM"), explain);
            Assert.assertTrue(explain.contains("IGNORE INDEX(I_AGENCY, IDX_ORDER_ID, PRIMARY)"), explain);
            c.createStatement().executeQuery(testSql2);
        }
    }

    @Test
    public void testUseIndex() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("use " + db);
            c.createStatement().execute("baseline add sql /*TDDL:a()*/ " + testSql3);
            StringBuilder sb = new StringBuilder();
            ResultSet rs = c.createStatement().executeQuery("explain " + testSql3);
            while (rs.next()) {
                sb.append(rs.getString(1));
            }
            String explain = sb.toString().toUpperCase(Locale.ROOT);
            Assert.assertTrue(explain.contains("SOURCE:SPM"), explain);
            Assert.assertTrue(explain.contains("USE INDEX(I_AGENCY, IDX_ORDER_ID, PRIMARY)"), explain);
            c.createStatement().executeQuery(testSql3);
        }
    }
}
