package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Partition Hint test cases
 *
 * @author fangwu
 */
public class SessionHintTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog("ROOT");
    private static final String TBL_NAME = "session_hint_test";
    private static final String CREATE_TBL = "CREATE TABLE if not exists `session_hint_test` (\n"
        + "    `pk` bigint(11) NOT NULL,\n"
        + "    `integer_test` int(11) DEFAULT NULL,\n"
        + "    PRIMARY KEY (`pk`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";

    /**
     * test if push hint working
     * set partition_hint=xxx;
     * trace select * from xxx;
     * show trace && check if partition_hint work
     */
    @Test
    public void testSessionHintWithShardingTable() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_qatest_app");
            c.createStatement().execute("set partition_hint=DRDS_POLARX1_QATEST_APP_000001_GROUP");
            c.createStatement().execute("trace select * from select_base_three_multi_db_one_tb");
            ResultSet rs = c.createStatement().executeQuery("show trace");

            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupName.equalsIgnoreCase("DRDS_POLARX1_QATEST_APP_000001_GROUP"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    /**
     * test if push hint working
     * set partition_hint=xxx;
     * trace select * from xxx;
     * show trace && check if partition_hint work
     */
    @Test
    public void testSessionHintWithAutoTable() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_part_qatest_app");

            String tableName = "select_base_three_multi_db_multi_tb";
            ResultSet topologyRs =
                c.createStatement().executeQuery("show topology from " + tableName);
            Map<String, String> partGroupMap = Maps.newHashMap();
            while (topologyRs.next()) {
                String groupName = topologyRs.getString("GROUP_NAME");
                String partName = topologyRs.getString("partition_name");
                partGroupMap.put(partName, groupName);
            }

            topologyRs.close();
            for (Map.Entry<String, String> entry : partGroupMap.entrySet()) {
                String partName = entry.getKey();
                String checkGroupName = entry.getValue();
                checkHintWork(c, TBL_NAME);
                c.createStatement().execute("set partition_hint=" + partName);
                ResultSet rs;
                String joinSql =
                    "trace select * from " + tableName + " a join select_base_two_multi_db_multi_tb b on a.pk=b.pk";
                c.createStatement().executeQuery(joinSql);
                rs = c.createStatement().executeQuery("show trace");

                while (rs.next()) {
                    String groupName = rs.getString("GROUP_NAME");
                    Assert.assertTrue(groupName.equalsIgnoreCase(checkGroupName));
                }
            }

            // broadcast table
            try {
                String sqlWithBroadcast =
                    "trace select * from select_base_two_multi_db_one_tb a join select_base_four_broadcast b on a.pk=b.pk;";
                c.createStatement().execute("set partition_hint=p3");
                c.createStatement().executeQuery(sqlWithBroadcast);
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                log.info(sqlException.getMessage());
                Assert.fail(
                    "partition hint should ignore broadcast table partition and replace its physical table name");
            }

            // partition hint won't infect broadcast table
            try {
                String sqlWithBroadcast = "trace select * from select_base_four_broadcast b";
                c.createStatement().execute("set partition_hint=p3333");
                c.createStatement().executeQuery(sqlWithBroadcast);
                ResultSet rs = c.createStatement().executeQuery("show trace");
                String groupName;
                while (rs.next()) {
                    groupName = rs.getString("GROUP_NAME");
                    Assert.assertTrue(groupName.equalsIgnoreCase("DRDS_POLARX1_PART_QATEST_APP_P00000_GROUP"));
                }
                rs.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                log.info(sqlException.getMessage());
                Assert.fail(
                    "partition hint should ignore broadcast table partition and replace its physical table name");
            }

            try {
                String errorSql =
                    "trace select * from select_base_two_multi_db_one_tb a";
                c.createStatement().execute("set partition_hint=p1111");
                c.createStatement().executeQuery(errorSql);
                Assert.fail(" error sql should product error msg");
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                log.info(sqlException.getMessage());
                Assert.assertTrue(
                    sqlException.getMessage().contains("Unsupported to use direct HINT "));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintWithTrans() throws SQLException {
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        ResultSet rs;
        try {
            c1 = getPolardbxConnection();
            c2 = getPolardbxConnection();
            c3 = getPolardbxConnection();
            c1.createStatement().execute("use drds_polarx1_part_qatest_app");
            c2.createStatement().execute("use drds_polarx1_part_qatest_app");
            c3.createStatement().execute("use drds_polarx1_part_qatest_app");

            String tableName = "update_delete_base_date_one_multi_db_one_tb";
            // c1 using hint, truncate data
            c1.createStatement().execute("truncate table " + tableName);

            // c2 begin and check no data
            c2.setAutoCommit(false);
            rs = c2.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

            // c3 check no data
            rs = c3.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

            // c1 begin and insert
            c1.setAutoCommit(false);
            c1.createStatement().execute("insert into " + tableName
                + " values(1,1,'a','a',null,1,1,1,1,1,1,1,1,1,now(), now(),now(),now(),now(),'a')");

            // c2 check no data
            rs = c2.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

            // c3 check no data
            rs = c3.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

            // c1 commit
            c1.commit();

            // c3 check data
            rs = c3.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(rs.next());
            rs.close();

            // c2 check no data
            rs = c2.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

        } finally {
            Objects.requireNonNull(c1).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c1).close();
            Objects.requireNonNull(c2).close();
            Objects.requireNonNull(c3).close();
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintWithDdl() throws SQLException {
        Connection connWithHint = null;
        Connection connWithoutHint = null;
        Random r = new Random();
        String tblName = "session_hint_ddl_test_tb_" + r.nextInt(10000);
        try {
            connWithoutHint = getPolardbxConnection();
            connWithoutHint.createStatement().execute("use drds_polarx1_part_qatest_app");
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use drds_polarx1_part_qatest_app");

            connWithHint.createStatement().execute("set partition_hint=p3");

            // test hint working
            checkHintWork(connWithHint, "select_base_three_multi_db_one_tb");

            // test create logical table
            connWithHint.createStatement().execute("CREATE TABLE " + tblName + "  (\n"
                + "\t`id` bigint(20) NOT NULL,\n"
                + "\t`ubigint_id` bigint(20) UNSIGNED DEFAULT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8");

            // get partition name and group name
            ResultSet rs = connWithHint.createStatement().executeQuery("show topology from " + tblName);
            rs.next();
            String groupName = rs.getString("GROUP_NAME");
            String partitionName = rs.getString("PARTITION_NAME");

            Assert.assertTrue(StringUtils.isNotEmpty(partitionName) && StringUtils.isNotEmpty(groupName));
            rs.close();

            // check session hint working for new table
            connWithHint.createStatement().execute("set partition_hint= " + partitionName);
            connWithHint.createStatement().execute("trace select * from " + tblName);
            rs = connWithHint.createStatement().executeQuery("show trace");
            while (rs.next()) {
                String groupNameTmp = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupNameTmp.equalsIgnoreCase(groupName));
            }
            rs.close();
        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("drop table if exists " + tblName);
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            Objects.requireNonNull(connWithoutHint).close();
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintWithInformationSchema() throws SQLException {
        Connection connWithHint = null;
        Connection connWithoutHint = null;
        try {
            connWithoutHint = getPolardbxConnection();
            connWithoutHint.createStatement().execute("use drds_polarx1_part_qatest_app");
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use drds_polarx1_part_qatest_app");

            connWithHint.createStatement().execute("set partition_hint=p3");

            checkHintWork(connWithHint, TBL_NAME);

            // test normal hint won't interrupt partition hint
            DataValidator.selectContentSameAssert("select * from information_schema.views", null, connWithHint,
                connWithoutHint, true);
            DataValidator.selectContentSameAssert("select * from information_schema.tables", null, connWithHint,
                connWithoutHint);
            DataValidator.selectContentSameAssert(
                "select MODULE_NAME, host, schedule_jobs, views from information_schema.module", null, connWithHint,
                connWithoutHint);
            DataValidator.selectContentSameAssert("select * from information_schema.column_statistics", null,
                connWithHint, connWithoutHint);

        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            Objects.requireNonNull(connWithoutHint).close();
            log.info("session hint test end");
        }
    }

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
    public void testSessionHintWithShowCommandAndDal() throws SQLException {
        Connection connWithHint = null;
        Connection connWithoutHint = null;
        try {
            connWithoutHint = getPolardbxConnection();
            connWithoutHint.createStatement().execute("use drds_polarx1_part_qatest_app");
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use drds_polarx1_part_qatest_app");

            connWithHint.createStatement().execute("set partition_hint=p3");

            // test hint working
            checkHintWork(connWithHint, TBL_NAME);

            // test show command
            DataValidator.selectContentSameAssert("show tables", null, connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show full tables", null, connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show ddl", null, connWithHint, connWithoutHint, true);
            DataValidator.selectContentSameAssert("show full ddl", null, connWithHint, connWithoutHint, true);
            DataValidator.selectContentSameAssert("show ddl status", null, connWithHint, connWithoutHint, true);
            DataValidator.selectContentSameAssert("show ddl result", null, connWithHint, connWithoutHint, true);
            DataValidator.selectContentSameAssert("show rule", null, connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show rule from select_base_three_multi_db_multi_tb", null,
                connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show full rule from select_base_three_multi_db_multi_tb", null,
                connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show topology from select_base_three_multi_db_multi_tb", null,
                connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show partitions from select_base_three_multi_db_multi_tb", null,
                connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show broadcasts", null, connWithHint, connWithoutHint);
            // show datasources cannot make sure POOLING_COUNT same
            DataValidator.selectConutAssert("show datasources", null, connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show node", null, connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show slow", null, connWithHint, connWithoutHint, true);
            DataValidator.selectConutAssert("show physical_slow", null, connWithHint, connWithoutHint);
            DataOperator.executeOnMysqlAndTddl(connWithHint, connWithoutHint, "clear slow", null);
            DataValidator.selectContentSameAssert(
                "explain optimizer select * from select_base_three_multi_db_multi_tb order by pk", null, connWithHint,
                connWithoutHint);

            // explain advisor/json_plan might cause different result in different cn node
            connWithHint.createStatement().executeQuery(
                "explain advisor select * from select_base_three_multi_db_multi_tb where varchar_test='12' order by pk");
            connWithoutHint.createStatement().executeQuery(
                "explain advisor select * from select_base_three_multi_db_multi_tb where varchar_test='12' order by pk");

            connWithHint.createStatement().execute("analyze table select_base_three_multi_db_multi_tb");
            DataValidator.selectContentSameAssert(
                "explain STATISTICS select * from select_base_three_multi_db_multi_tb where varchar_test='12' order by pk",
                null, connWithHint, connWithoutHint);

            connWithHint.createStatement().executeQuery(
                "explain JSON_PLAN select * from select_base_three_multi_db_multi_tb where varchar_test='12' order by pk");
            connWithoutHint.createStatement().executeQuery(
                "explain JSON_PLAN select * from select_base_three_multi_db_multi_tb where varchar_test='12' order by pk");

            connWithHint.createStatement().executeQuery(
                "explain EXECUTE select * from select_base_three_multi_db_multi_tb where varchar_test='12' order by pk");
            connWithoutHint.createStatement().executeQuery(
                "explain EXECUTE select * from select_base_three_multi_db_multi_tb where varchar_test='12' order by pk");

            DataValidator.selectContentSameAssert(
                "explain SHARDING select * from select_base_three_multi_db_multi_tb where varchar_test='12' order by pk",
                null, connWithHint, connWithoutHint);
            DataValidator.selectContentSameAssert("show sequences", null, connWithHint, connWithoutHint);
            connWithHint.createStatement().execute("create sequence session_hint_test_seq start with 100");
            connWithHint.createStatement().execute("alter sequence session_hint_test_seq start with 99");
            connWithHint.createStatement().execute("drop sequence session_hint_test_seq");

            connWithHint.createStatement().execute("clear ccl_rules");
            connWithHint.createStatement().execute("clear ccl_triggers");

            // thread running might be different
            connWithHint.createStatement().execute("show db status");
            connWithHint.createStatement().execute("show full db status");

            DataValidator.selectContentSameAssert("show ds", null, connWithHint, connWithoutHint);
            connWithHint.createStatement().execute("show connection");
            DataValidator.selectContentSameAssert("show trans", null, connWithHint, connWithoutHint, true);
            DataValidator.selectContentSameAssert("show full trans", null, connWithHint, connWithoutHint, true);
            DataValidator.selectContentSameAssert("show ccl_rules", null, connWithHint, connWithoutHint, true);

            // test check table
            DataValidator.selectContentSameAssert("check table select_base_four_multi_db_multi_tb;", null, connWithHint,
                connWithoutHint, true);
        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            Objects.requireNonNull(connWithoutHint).close();
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintWithDirectHint() throws SQLException {
        Connection connWithHint = null;
        try {
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use drds_polarx1_part_qatest_app");

            connWithHint.createStatement().execute("set partition_hint=p3");

            // test partition hint working
            checkHintWork(connWithHint, TBL_NAME);
            ResultSet rs;

            // test normal hint won't interrupt partition hint
            // get group name and partition name
            rs = connWithHint.createStatement().executeQuery("show topology from  " + TBL_NAME);

            String groupName = null;
            String partitionName = null;
            String tableName = null;
            while (rs.next()) {
                groupName = rs.getString("GROUP_NAME");
                partitionName = rs.getString("PARTITION_NAME");
                tableName = rs.getString("TABLE_NAME");
                break;
            }
            rs.close();
            Assert.assertTrue(groupName != null && partitionName != null);

            // test partition hint working
            connWithHint.createStatement().execute("set partition_hint=" + partitionName);
            // test partition hint with non-direct hint
            connWithHint.createStatement().execute("trace /*TDDL:slave()*/ select * from " + TBL_NAME);
            connWithHint.createStatement().execute("trace /*TDDL:a()*/ select * from " + TBL_NAME);
            connWithHint.createStatement()
                .execute("trace /*TDDL:test_hint=true*/ select * from select_base_three_multi_db_one_tb");
            rs = connWithHint.createStatement().executeQuery("show trace");

            while (rs.next()) {
                String groupNameTmp = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupNameTmp.equalsIgnoreCase(groupName));
            }
            rs.close();

            // test node hint would interrupt partition hint
            Assert.assertTrue(groupName != null);
            Assert.assertTrue(tableName != null);
            connWithHint.createStatement()
                .execute("trace /*TDDL:node=" + groupName + "*/ select * from " + tableName);

        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            log.info("session hint test end");
        }
    }

    @Ignore
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

            // test insert into partition table
            Calendar current = Calendar.getInstance();
            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            connWithHint.setAutoCommit(false);
            connWithHint.createStatement()
                .execute(
                    "insert into update_delete_base_autonic_multi_db_multi_tb(varchar_test, timestamp_test) values('session hint test value', now())");
            connWithHint.commit();
            rs = connWithHint.createStatement().executeQuery(
                "select * from update_delete_base_autonic_multi_db_multi_tb as of timestamp '" + f.format(
                    current.getTime()) + "' where varchar_test='session hint test value'");
            System.out.println(
                "select * from update_delete_base_autonic_multi_db_multi_tb as of timestamp '" + f.format(
                    current.getTime()) + "' where varchar_test='session hint test value'");
            while (rs.next()) {
                Assert.fail("should not query any value by flashback");
            }
            rs.close();
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
                    "insert into update_delete_base_autonic_string_multi_db_multi_tb  select * from update_delete_base_autonic_multi_db_multi_tb");
        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            log.info("session hint test end");
        }
    }

    /**
     * test if push hint working on multi table
     * set partition_hint=xxx;
     * trace select * from xxx;
     * show trace && check if partition_hint work
     */
    @Test
    public void testSessionHintWithShardingMultiTable() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_qatest_app");
            c.createStatement().execute("set partition_hint='DRDS_POLARX1_QATEST_APP_000001_GROUP:00'");
            c.createStatement().execute("trace select * from select_base_three_multi_db_multi_tb");
            ResultSet rs = c.createStatement().executeQuery("show trace");

            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupName.equalsIgnoreCase("DRDS_POLARX1_QATEST_APP_000001_GROUP"));
            }

            c.createStatement().execute("set partition_hint='DRDS_POLARX1_QATEST_APP_000001_GROUP:1'");
            c.createStatement().execute("trace select * from select_base_three_multi_db_multi_tb");
            rs = c.createStatement().executeQuery("show trace");

            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupName.equalsIgnoreCase("DRDS_POLARX1_QATEST_APP_000001_GROUP"));
            }

            c.createStatement().execute("set partition_hint='DRDS_POLARX1_QATEST_APP_000001_GROUP:4'");
            try {
                c.createStatement().execute("trace select * from select_base_three_multi_db_multi_tb");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage()
                    .contains("table index overflow in target group from direct HINT for sharding table"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintWithJoin() throws SQLException {
        // drds table with different sharding table num, report error
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_qatest_app");
            c.createStatement().execute("set partition_hint='DRDS_POLARX1_QATEST_APP_000001_GROUP:00'");

            try {
                c.createStatement().execute(
                    "select * from select_base_three_multi_db_multi_tb a join select_base_three_multi_db_one_tb b on a.pk=b.pk");
                Assert.fail("should report error");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage()
                    .contains("different sharding table num in partition hint mode"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }

        // auto table in different table group, report error
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_part_qatest_app");
            c.createStatement().execute("set partition_hint='p1'");

            // create table group
            c.createStatement().execute("create tablegroup if not exists partition_hint_test_tg1");
            c.createStatement().execute("create tablegroup if not exists partition_hint_test_tg2");

            // create table into table group partition_test_tg1
            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_tbl1` (\n"
                + "        `a` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "TABLEGROUP=partition_hint_test_tg1\n"
                + "PARTITION BY RANGE(YEAR(a))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_tbl2` (\n"
                + "        `a` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "TABLEGROUP=partition_hint_test_tg2\n"
                + "PARTITION BY RANGE(YEAR(a))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            // test different table group error report
            try {
                c.createStatement()
                    .execute("select * from partition_hint_test_tbl1 a join partition_hint_test_tbl2 b on a.a=b.a");
                Assert.fail("should report error");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage()
                    .contains("different table group in partition hint mode"));
            }

            // change table group to same one:partition_hint_test_tbl1
            c.createStatement().execute("ALTER TABLE partition_hint_test_tbl2 set TABLEGROUP=partition_hint_test_tg1;");

            // test if same table group work
            c.createStatement()
                .execute("select * from partition_hint_test_tbl1 a join partition_hint_test_tbl2 b on a.a=b.a");
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).createStatement().execute("drop table if exists partition_hint_test_tbl1");
            Objects.requireNonNull(c).createStatement().execute("drop table if exists partition_hint_test_tbl2");
            Objects.requireNonNull(c).createStatement().execute("drop tablegroup if exists partition_hint_test_tg1");
            Objects.requireNonNull(c).createStatement().execute("drop tablegroup if exists partition_hint_test_tg2");
            log.info("session hint test end");
        }

        // test auto table join with broadcast table
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_part_qatest_app");
            c.createStatement().execute("set partition_hint='p1'");

            // create table group
            c.createStatement().execute("create tablegroup if not exists partition_hint_test_tg1");

            // create table into table group partition_test_tg1
            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_tbl1` (\n"
                + "        `a` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "TABLEGROUP=partition_hint_test_tg1\n"
                + "PARTITION BY RANGE(YEAR(a))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_tbl2` (\n"
                + "        `a` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "BROADCAST");

            // test if table with broadcast table work
            c.createStatement()
                .execute("select * from partition_hint_test_tbl1 a join partition_hint_test_tbl2 b on a.a=b.a");
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).createStatement().execute("drop table if exists partition_hint_test_tbl1");
            Objects.requireNonNull(c).createStatement().execute("drop table if exists partition_hint_test_tbl2");
            Objects.requireNonNull(c).createStatement().execute("drop tablegroup if exists partition_hint_test_tg1");
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintCrossSchema() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_qatest_app");
            c.createStatement().execute("set partition_hint='DRDS_POLARX1_QATEST_APP_000001_GROUP:00'");

            c.createStatement().execute("select * from drds_polarx1_qatest_app.select_base_three_multi_db_multi_tb");

            try {
                c.createStatement()
                    .execute("select * from drds_polarx1_part_qatest_app.select_base_three_multi_db_multi_tb");
                Assert.fail("should report error");
            } catch (SQLException e) {
                log.info(e.getMessage());
                Assert.assertTrue(
                    e.getMessage().contains("partition hint visit table crossing schema"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    /**
     * partition hint with physical table name should cause table not exists error
     * this condition should use direct node
     */
    @Test
    public void testSessionHintWithPhysicalTableName() throws SQLException {
        Connection c = null;

        // drds table
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_qatest_app");
            c.createStatement().execute("set partition_hint='DRDS_POLARX1_QATEST_APP_000001_GROUP:00'");

            /*
             * expected table topology
             * |    0 | DRDS_POLARX1_QATEST_APP_000000_GROUP | select_base_three_multi_db_multi_tb_i9JV_00 | -              |
             * |    1 | DRDS_POLARX1_QATEST_APP_000000_GROUP | select_base_three_multi_db_multi_tb_i9JV_01 | -              |
             * |    2 | DRDS_POLARX1_QATEST_APP_000000_GROUP | select_base_three_multi_db_multi_tb_i9JV_02 | -              |
             * |    3 | DRDS_POLARX1_QATEST_APP_000000_GROUP | select_base_three_multi_db_multi_tb_i9JV_03 | -              |
             * |    4 | DRDS_POLARX1_QATEST_APP_000001_GROUP | select_base_three_multi_db_multi_tb_i9JV_04 | -              |
             * |    5 | DRDS_POLARX1_QATEST_APP_000001_GROUP | select_base_three_multi_db_multi_tb_i9JV_05 | -              |
             * |    6 | DRDS_POLARX1_QATEST_APP_000001_GROUP | select_base_three_multi_db_multi_tb_i9JV_06 | -              |
             * |    7 | DRDS_POLARX1_QATEST_APP_000001_GROUP | select_base_three_multi_db_multi_tb_i9JV_07 | -              |
             * ... ...
             */
            c.createStatement().execute("select * from drds_polarx1_qatest_app.select_base_three_multi_db_multi_tb");

            try {
                c.createStatement()
                    .execute(
                        "select * from select_base_three_multi_db_multi_tb_i9JV_02 a join select_base_three_multi_db_one_tb b on a.pk=b.pk;");
                Assert.fail("should report error");
            } catch (SQLException e) {
                log.info(e.getMessage());
                Assert.assertTrue(e.getMessage().contains("[ERR_TABLE_NOT_EXIST]"));
            }
            try {
                c.createStatement()
                    .execute(
                        "select * from select_base_three_multi_db_multi_tb_i9JV_06 a join select_base_three_multi_db_one_tb b on a.pk=b.pk;");
            } catch (SQLException e) {
                log.info(e.getMessage());
                Assert.assertTrue(e.getMessage().contains("[ERR_TABLE_NOT_EXIST]"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }

        // auto table
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_part_qatest_app");
            c.createStatement().execute("set partition_hint=p2");

            /*
             * expected topology
             * |    0 | DRDS_POLARX1_PART_QATEST_APP_P00000_GROUP | select_base_three_multi_db_multi_tb_MvY5_00001 | p2
             * |    1 | DRDS_POLARX1_PART_QATEST_APP_P00001_GROUP | select_base_three_multi_db_multi_tb_MvY5_00000 | p1
             * |    2 | DRDS_POLARX1_PART_QATEST_APP_P00001_GROUP | select_base_three_multi_db_multi_tb_MvY5_00002 | p3
             * ... ...
             */
            c.createStatement()
                .execute("select * from drds_polarx1_part_qatest_app.select_base_three_multi_db_multi_tb");

            try {
                c.createStatement()
                    .execute(
                        "select * from select_base_three_multi_db_multi_tb_MvY5_00002 a join select_base_three_multi_db_one_tb b on a.pk=b.pk;");
                Assert.fail("should report error");
            } catch (SQLException e) {
                log.info(e.getMessage());
                Assert.assertTrue(e.getMessage().contains("[ERR_TABLE_NOT_EXIST]"));
            }
            try {
                c.createStatement()
                    .execute(
                        "select * from select_base_three_multi_db_multi_tb_MvY5_00001 a join select_base_three_multi_db_one_tb b on a.pk=b.pk;");

            } catch (SQLException e) {
                log.info(e.getMessage());
                Assert.assertTrue(e.getMessage().contains("[ERR_TABLE_NOT_EXIST]"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    /**
     * show full connection would output partition hint info.
     */
    @Test
    public void testShowFullConnection() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_qatest_app");
            c.createStatement().execute("set partition_hint='DRDS_POLARX1_QATEST_APP_000001_GROUP:00'");

            ResultSet rs = c.createStatement().executeQuery("show full connection");

            boolean found = false;
            while (rs.next()) {
                String hint = rs.getString("PARTITION_HINT");
                if ("DRDS_POLARX1_QATEST_APP_000001_GROUP:00".equalsIgnoreCase(hint)) {
                    found = true;
                }
            }
            Assert.assertTrue(found);
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    /**
     * partition hint with dml on table with gsi should report error.
     */
    @Test
    public void testDmlWithAutoGsi() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("drop database if exists partition_hint_test");
            c.createStatement().execute("create database if not exists partition_hint_test mode=auto");
            c.createStatement().execute("use partition_hint_test");
            Random r = new Random();
            String tblName = "session_hint_dml_test_tb_order_" + r.nextInt(10000);
            c.createStatement().execute(
                "CREATE PARTITION TABLE " + tblName + "(\n"
                    + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                    + "    `bid` int(11) DEFAULT NULL,\n"
                    + "    `name` varchar(30) DEFAULT NULL,\n"
                    + "    PRIMARY KEY (`id`),\n"
                    + "    GLOBAL INDEX /* idx_name_$a870 */ `idx_name` (`name`) PARTITION BY KEY (`name`, `id`) PARTITIONS 16,\n"
                    + "    LOCAL KEY `_local_idx_name` (`name`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
                    + "PARTITION BY KEY(`id`)\n"
                    + "PARTITIONS 16");
            c.createStatement()
                .execute("insert into " + tblName + "(id, bid, name) values(1, 11, 'a')");
            c.createStatement().execute("set partition_hint='P1'");

            c.createStatement().execute("insert into " + tblName + "(id, bid, name) values(2, 12, 'a')");

            Assert.fail("should report error");
        } catch (SQLException e) {
            Assert.assertTrue(
                e.getMessage().contains("[ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY]"));
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    /**
     * partition hint with dml on table with gsi should report error.
     */
    @Test
    public void testDmlWithShardingGsi() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("drop database if exists partition_hint_test");
            c.createStatement().execute("create database if not exists partition_hint_test mode=drds");
            c.createStatement().execute("use partition_hint_test");
            Random r = new Random();
            String tblName = "session_hint_dml_test_tb_order_" + r.nextInt(10000);
            c.createStatement().execute(
                "CREATE TABLE " + tblName + "(\n"
                    + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                    + "    `bid` int(11) DEFAULT NULL,\n"
                    + "    `name` varchar(30) DEFAULT NULL,\n"
                    + "    PRIMARY KEY (`id`),\n"
                    + "    GLOBAL INDEX /* idx_name_$a870 */ `idx_name` (`name`) dbpartition by hash(`name`),\n"
                    + "    LOCAL KEY `_local_idx_name` (`name`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
                    + "dbpartition by hash(name)");
            c.createStatement()
                .execute("insert into " + tblName + "(id, bid, name) values(1, 11, 'a')");
            c.createStatement().execute("set partition_hint='PARTITION_HINT_TEST_000000_GROUP'");

            c.createStatement().execute("insert into " + tblName + "(id, bid, name) values(2, 12, 'a')");

            Assert.fail("should report error");
        } catch (SQLException e) {
            Assert.assertTrue(
                e.getMessage().contains("[ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY]"));
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    /**
     * partition hint with dml on table with gsi should report error.
     */
    @Test
    public void testDmlWithShardingBroadcast() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("drop database if exists partition_hint_test");
            c.createStatement().execute("create database if not exists partition_hint_test mode=drds");
            c.createStatement().execute("use partition_hint_test");
            Random r = new Random();
            String tblName = "session_hint_dml_test_tb_order_" + r.nextInt(10000);
            c.createStatement().execute(
                "CREATE TABLE " + tblName + "(\n"
                    + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                    + "    `bid` int(11) DEFAULT NULL,\n"
                    + "    `name` varchar(30) DEFAULT NULL,\n"
                    + "    PRIMARY KEY (`id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
                    + "broadcast");
            c.createStatement()
                .execute("insert into " + tblName + "(id, bid, name) values(1, 11, 'a')");
            c.createStatement().execute("set partition_hint='PARTITION_HINT_TEST_SINGLE_GROUP'");

            c.createStatement().execute("insert into " + tblName + "(id, bid, name) values(2, 12, 'a')");

            Assert.fail("should report error");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.assertTrue(
                e.getMessage().contains("[ERR_MODIFY_BROADCAST_TABLE_BY_HINT_NOT_ALLOWED]"));
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    /**
     * test prepare path with partition hint
     */
    @Test
    public void testPrepareDml() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("drop database if exists partition_hint_test");
            c.createStatement().execute("create database if not exists partition_hint_test mode=auto");
            c.createStatement().execute("use partition_hint_test");
            // create table into table group partition_test_tg1
            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_tbl1` (\n"
                + "        `id` bigint NOT NULL,\n"
                + "        `name` varchar(25) NOT NULL,\n"
                + "        `c_time` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "PARTITION BY RANGE(YEAR(c_time))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_tbl2` (\n"
                + "        `id` bigint NOT NULL,\n"
                + "        `name` varchar(25) NOT NULL,\n"
                + "        `c_time` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "PARTITION BY RANGE(YEAR(c_time))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            c.createStatement().executeQuery("set partition_hint='p1'");
            // test insert
            PreparedStatement ps =
                c.prepareStatement("insert into partition_hint_test_tbl1(id, name, c_time) values(?, ?, ?)");

            ps.setInt(1, 5);
            ps.setString(2, "a");
            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));

            int affectRows = ps.executeUpdate();

            Assert.assertTrue(affectRows == 1);
            ps.close();

            // test insert batch
            c.createStatement().executeQuery("set partition_hint='p2'");
            ps = c.prepareStatement("insert into partition_hint_test_tbl1(id, name, c_time) values(?, ?, ?)");

            ps.setInt(1, 5);
            ps.setString(2, "a");
            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));

            ps.addBatch();

            ps.setInt(1, 6);
            ps.setString(2, "b");
            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));

            ps.addBatch();

            ps.executeBatch();
            ps.close();

            ResultSet rs = c.createStatement().executeQuery("select count(1) from partition_hint_test_tbl1");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(2 == rs.getInt(1));

            // test insert multi
            c.createStatement().executeQuery("set partition_hint='p3'");
            ps = c.prepareStatement("insert into partition_hint_test_tbl1(id, name, c_time) values(?, ?, ?),(?, ?, ?)");

            ps.setInt(1, 5);
            ps.setString(2, "a");
            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            ps.setInt(4, 6);
            ps.setString(5, "b");
            ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));

            affectRows = ps.executeUpdate();

            Assert.assertTrue(affectRows == 2);
            ps.close();

            c.createStatement().executeQuery("set partition_hint=''");
            rs = c.createStatement().executeQuery("select count(1) from partition_hint_test_tbl1");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(5 == rs.getInt(1));
            // test insert select
            c.createStatement().executeQuery("set partition_hint='p3'");
            c.createStatement().execute("insert into partition_hint_test_tbl2 select * from partition_hint_test_tbl1");

            rs = c.createStatement().executeQuery("select count(1) from partition_hint_test_tbl2");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(2 == rs.getInt(1));

            c.createStatement().executeQuery("set partition_hint='p2'");
            rs = c.createStatement().executeQuery("select count(1) from partition_hint_test_tbl2");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(0 == rs.getInt(1));
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).createStatement().execute("drop database if exists partition_hint_test");
            log.info("session hint test end");
        }
    }
}
