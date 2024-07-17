package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Objects;

public class SessionHintTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog("ROOT");
    private static final String TBL_NAME = "session_hint_test";
    private static final String CREATE_TBL = "CREATE TABLE if not exists `session_hint_test` (\n"
        + "    `pk` bigint(11) NOT NULL,\n"
        + "    `integer_test` int(11) DEFAULT NULL,\n"
        + "    PRIMARY KEY (`pk`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";

    private void checkHintWork(Connection conn, String tblName) throws SQLException {
        // prepare table
        conn.createStatement().execute(CREATE_TBL);

        // get group name and partition name
        ResultSet rs = conn.createStatement().executeQuery("show topology from  " + tblName);

        String groupName = null;
        String partitionName = null;
        while (rs.next()) {
            groupName = rs.getString("GROUP_NAME");
            partitionName = rs.getString("PARTITION_NAME");
            break;
        }
        rs.close();
        Assert.assertTrue(groupName != null && partitionName != null);

        // test partition hint working
        conn.createStatement().execute("set partition_hint=" + partitionName);
        conn.createStatement().execute("trace select * from " + tblName);
        rs = conn.createStatement().executeQuery("show trace");

        while (rs.next()) {
            String groupNameTmp = rs.getString("GROUP_NAME");
            Assert.assertTrue(groupNameTmp.equalsIgnoreCase(groupName));
        }
        rs.close();
        conn.createStatement().execute("set partition_hint=''");
    }

    @Test
    public void testSessionHintWithFlashBack() throws SQLException, InterruptedException {
        Connection connWithHint = null;
        try {
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use drds_polarx1_part_qatest_app");

            connWithHint.createStatement().execute("set partition_hint=p3");
            connWithHint.createStatement().execute("set global ENABLE_FORBID_PUSH_DML_WITH_HINT=false");
            Thread.sleep(2000);
            // test partition hint working
            checkHintWork(connWithHint, TBL_NAME);
            ResultSet rs;

            // clear data
            connWithHint.createStatement().execute("truncate table update_delete_base_autonic_multi_db_multi_tb");

            // Ensure the as of timestamp is valid.
            Thread.sleep(1000);

            // test insert into partition table
            Calendar current = Calendar.getInstance();
            current.setTimeInMillis(current.getTimeInMillis() - 5000);
            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            connWithHint.setAutoCommit(false);
            connWithHint.createStatement()
                .execute(
                    "insert into update_delete_base_autonic_multi_db_multi_tb(varchar_test, timestamp_test) values('session hint test value', now())");
            connWithHint.commit();
            String sql = "select * from update_delete_base_autonic_multi_db_multi_tb as of timestamp '" + f.format(
                current.getTime()) + "' where varchar_test='session hint test value'";
            System.out.println(sql);
            rs = connWithHint.createStatement().executeQuery(sql);
            while (rs.next()) {
                Assert.fail("should not query any value by flashback");
            }
            rs.close();
        } catch (Throwable t) {
            if (isMySQL80() && t.getMessage().contains(
                "The definition of the table required by the flashback query has changed")) {
                return;
            }
            throw t;
        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintForbiddenByConfig() throws SQLException, InterruptedException {
        Connection connWithHint = null;
        try {
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use drds_polarx1_part_qatest_app");

            connWithHint.createStatement().execute("set partition_hint=p3");
            connWithHint.createStatement().execute("set global ENABLE_FORBID_PUSH_DML_WITH_HINT=true");
            Thread.sleep(2000);
            connWithHint.setAutoCommit(false);

            // test partition hint working
            checkHintWork(connWithHint, TBL_NAME);
            connWithHint.createStatement().execute("set partition_hint=p3");
            try {
                connWithHint.createStatement()
                    .execute(
                        "insert into update_delete_base_autonic_multi_db_multi_tb(varchar_test, timestamp_test) values('session hint test value', now())");
                Assert.fail(" dml should be forbidden by ENABLE_FORBID_PUSH_DML_WITH_HINT=true");
            } catch (SQLException e) {
                if (!e.getMessage().contains("Unsupported to push physical dml by hint ")) {
                    throw e;
                }
            }

            try {
                connWithHint.createStatement()
                    .execute(
                        "update update_delete_base_autonic_multi_db_multi_tb set varchar_test='session hint test value'");
                Assert.fail(" dml should be forbidden by ENABLE_FORBID_PUSH_DML_WITH_HINT=true");
            } catch (SQLException e) {
                if (!e.getMessage().contains("Unsupported to push physical dml by hint ")) {
                    throw e;
                }
            }

            try {
                connWithHint.createStatement()
                    .execute(
                        "insert into update_delete_base_autonic_multi_db_multi_tb select * from update_delete_base_autonic_string_multi_db_multi_tb");
                Assert.fail(" dml should be forbidden by ENABLE_FORBID_PUSH_DML_WITH_HINT=true");
            } catch (SQLException e) {
                if (!e.getMessage().contains("Unsupported to push physical dml by hint ")) {
                    throw e;
                }
            }
        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintWithDML() throws SQLException, InterruptedException {
        Connection connWithHint = null;
        try {
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use drds_polarx1_part_qatest_app");

            connWithHint.createStatement().execute("set partition_hint=p3");
            connWithHint.createStatement().execute("set global ENABLE_FORBID_PUSH_DML_WITH_HINT=false");
            Thread.sleep(2000);

            // test partition hint working
            checkHintWork(connWithHint, TBL_NAME);
            connWithHint.createStatement().execute("set partition_hint=p3");

            connWithHint.createStatement()
                .execute(
                    "insert into update_delete_base_autonic_multi_db_multi_tb(varchar_test, timestamp_test) values('session hint test value', now())");

            connWithHint.createStatement()
                .execute(
                    "update update_delete_base_autonic_multi_db_multi_tb set varchar_test='session hint test value111'");
            connWithHint.createStatement()
                .execute("truncate table update_delete_base_autonic_string_multi_db_multi_tb");
            connWithHint.createStatement()
                .execute(
                    "insert into update_delete_base_autonic_string_multi_db_multi_tb(varchar_test, timestamp_test)  "
                        + "select varchar_test, timestamp_test from update_delete_base_autonic_multi_db_multi_tb");
        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            log.info("session hint test end");
        }
    }

}
