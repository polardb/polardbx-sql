package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Partition Hint test cases
 *
 * @author fangwu
 */
@CdcIgnore(
    ignoreReason = "binlog 和 replica 未支持 partition hint")
public class PartitionNameHintTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog("ROOT");
    private static final String SHARD_DB_NAME = "partition_name_hint_test_shard";
    private static final String AUTO_DB_NAME = "partition_name_hint_test_auto";
    private static final String AUTO_TBL_NAME1 = "partition_name_hint_test1";
    private static final String AUTO_TBL_NAME2 = "partition_name_hint_test2";
    private static final String AUTO_TBL_NAME3 = "partition_name_hint_test3";
    private static final String AUTO_TBL_NAME4 = "partition_name_hint_test4";
    private static final String DRDS_TBL_NAME = "multi_db_multi_tbl";
    private static final String DRDS_TBL_NAME2 = "multi_db_multi_tbl2";

    /**
     * auto table
     */
    private static final String AUTO_CREATE_TBL = "CREATE TABLE %s(\n"
        + " id bigint not null auto_increment,\n"
        + " bid int,\n"
        + " name varchar(30),\n"
        + " birthday datetime not null,\n"
        + " primary key(id)\n"
        + ")\n"
        + "partition by hash(id)\n"
        + "partitions 8;";

    private static final String AUTO_CREATE_TBL2 = "CREATE TABLE %s(\n"
        + " id bigint not null auto_increment,\n"
        + " bid int,\n"
        + " name varchar(30),\n"
        + " birthday datetime not null,\n"
        + " primary key(id)\n"
        + ")\n"
        + "partition by hash(id)\n"
        + "partitions 5;";

    private static final String AUTO_CREATE_BROADCAST_TBL = "CREATE TABLE %s(\n"
        + " id bigint not null auto_increment, \n"
        + " bid int, \n"
        + " name varchar(30), \n"
        + " primary key(id)\n"
        + ") BROADCAST;";

    private static final String DRDS_CREATE_TBL = "CREATE TABLE multi_db_multi_tbl(\n"
        + " id bigint not null auto_increment, \n"
        + " bid int, \n"
        + " name varchar(30), \n"
        + " primary key(id)\n"
        + ") dbpartition by hash(id) tbpartition by hash(bid) tbpartitions 3";

    private static final String DRDS_CREATE_TBL2 = "CREATE TABLE multi_db_multi_tbl2(\n"
        + " id bigint not null auto_increment, \n"
        + " bid int, \n"
        + " name varchar(30), \n"
        + " primary key(id)\n"
        + ") dbpartition by hash(id) tbpartition by hash(bid) tbpartitions 5";

    @BeforeClass
    public static void setUp() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + SHARD_DB_NAME);
            c.createStatement().execute("create database if not exists " + SHARD_DB_NAME);
            c.createStatement().execute("drop database if exists " + AUTO_DB_NAME);
            c.createStatement().execute("create database if not exists " + AUTO_DB_NAME + " mode = auto");

            c.createStatement().execute("use " + AUTO_DB_NAME);
            c.createStatement().execute(String.format(AUTO_CREATE_TBL, AUTO_TBL_NAME1));
            c.createStatement().execute(String.format(AUTO_CREATE_TBL, AUTO_TBL_NAME2));
            c.createStatement().execute(String.format(AUTO_CREATE_BROADCAST_TBL, AUTO_TBL_NAME3));
            c.createStatement().execute(String.format(AUTO_CREATE_TBL2, AUTO_TBL_NAME4));
            c.createStatement().execute("use " + SHARD_DB_NAME);
            c.createStatement().execute(DRDS_CREATE_TBL);
            c.createStatement().execute(DRDS_CREATE_TBL2);
        }
    }

    /**
     * test if push hint working
     * set partition_hint=xxx;
     * trace select * from xxx;
     * show trace && check if partition_hint work
     */
    @Test
    public void testSessionHintWithShardingTable() throws SQLException {
        Connection c = getPolardbxConnection(SHARD_DB_NAME);
        try {
            c.createStatement().execute("set partition_hint=PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP");
            c.createStatement().execute("trace select * from " + DRDS_TBL_NAME);
            ResultSet rs = c.createStatement().executeQuery("show trace");

            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupName.equalsIgnoreCase("PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP"));
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
            c.createStatement().execute("use " + AUTO_DB_NAME);

            ResultSet topologyRs =
                c.createStatement().executeQuery("show topology from " + AUTO_TBL_NAME1);
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
                checkHintWork(c, AUTO_TBL_NAME1);
                c.createStatement().execute("set partition_hint=" + partName);
                ResultSet rs;
                String joinSql =
                    "trace select * from " + AUTO_TBL_NAME1 + " a join " + AUTO_TBL_NAME2 + " b on a.id=b.id";
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
                    "trace select * from " + AUTO_TBL_NAME1 + " a join " + AUTO_TBL_NAME3 + " b on a.id=b.bid;";
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
                String sqlWithBroadcast = "trace select * from " + AUTO_TBL_NAME3 + " b";
                c.createStatement().execute("set partition_hint=p3333");
                c.createStatement().executeQuery(sqlWithBroadcast);
                ResultSet rs = c.createStatement().executeQuery("show trace");
                String groupName;
                while (rs.next()) {
                    groupName = rs.getString("GROUP_NAME");
                    Assert.assertTrue(groupName.equalsIgnoreCase("PARTITION_NAME_HINT_TEST_AUTO_P00000_GROUP"));
                }
                rs.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                log.info(sqlException.getMessage());
                Assert.fail(
                    "partition hint should ignore broadcast table partition and replace its physical table name");
            }

            try {
                String errorSql = "trace select * from " + AUTO_TBL_NAME1 + " a";
                c.createStatement().execute("set partition_hint=p1111");
                c.createStatement().executeQuery(errorSql);
                Assert.fail(" error sql should product error msg");
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                log.info(sqlException.getMessage());
                Assert.assertTrue(
                    sqlException.getMessage().contains("Unsupported direct HINT for part table :p1111"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    /**
     * non-table statement like SELECT @@session.transaction_read_only
     */
    @Test
    public void testSessionHintWithNonTableStmt() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use " + AUTO_DB_NAME);
            c.createStatement().execute("set partition_hint=p3");
            c.createStatement().execute("SELECT @@session.transaction_read_only");
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).close();
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
            c1.createStatement().execute("use " + AUTO_DB_NAME);
            c2.createStatement().execute("use " + AUTO_DB_NAME);
            c3.createStatement().execute("use " + AUTO_DB_NAME);

            String tableName = AUTO_TBL_NAME2;
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
            c1.createStatement().execute("insert into " + tableName + " values(1,1,'a',now())");

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
    public void testSessionHintWithInformationSchema() throws SQLException {
        Connection connWithHint = null;
        Connection connWithoutHint = null;
        try {
            connWithoutHint = getPolardbxConnection();
            connWithoutHint.createStatement().execute("use " + AUTO_DB_NAME);
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use " + AUTO_DB_NAME);

            connWithHint.createStatement().execute("set partition_hint=p3");

            // test normal hint won't interrupt partition hint
            DataValidator.selectContentSameAssert("explain select * from information_schema.views", null, connWithHint,
                connWithoutHint, true);
            DataValidator.selectContentSameAssert("explain select * from information_schema.tables", null, connWithHint,
                connWithoutHint);
            DataValidator.selectContentSameAssert(
                "explain select MODULE_NAME, host, schedule_jobs, views from information_schema.module", null,
                connWithHint,
                connWithoutHint);
            DataValidator.selectContentSameAssert("explain select * from information_schema.column_statistics", null,
                connWithHint, connWithoutHint);

        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            Objects.requireNonNull(connWithoutHint).close();
            log.info("session hint test end");
        }
    }

    private void checkHintWork(Connection conn, String tblName) throws SQLException {
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
            connWithoutHint.createStatement().execute("use " + AUTO_DB_NAME);
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use " + AUTO_DB_NAME);

            connWithHint.createStatement().execute("set partition_hint=p3");

            // test show command
            connWithHint.createStatement().executeQuery("show tables");
            connWithHint.createStatement().executeQuery("show full tables");
            connWithHint.createStatement().executeQuery("show ddl");
            connWithHint.createStatement().executeQuery("show full ddl");
            connWithHint.createStatement().executeQuery("show ddl status");
            connWithHint.createStatement().executeQuery("show ddl result");
            connWithHint.createStatement().executeQuery("show rule from " + AUTO_TBL_NAME1);
            connWithHint.createStatement().executeQuery("show full rule from " + AUTO_TBL_NAME2);
            connWithHint.createStatement().executeQuery("show topology from " + AUTO_TBL_NAME3);
            connWithHint.createStatement().executeQuery("show partitions from " + AUTO_TBL_NAME1);
            // show datasources cannot make sure POOLING_COUNT same
            DataValidator.selectConutAssert("show datasources", null, connWithHint, connWithoutHint);
            DataOperator.executeOnMysqlAndTddl(connWithHint, connWithoutHint, "clear slow", null);

            // explain advisor/json_plan might cause different result in different cn node
            connWithHint.createStatement().executeQuery(
                "explain advisor select * from " + AUTO_TBL_NAME1 + " where name='12' order by id");
            connWithHint.createStatement().execute("analyze table " + AUTO_TBL_NAME1);
            connWithHint.createStatement().executeQuery(
                "explain STATISTICS select * from " + AUTO_TBL_NAME2 + " where name='12' order by id");
            connWithHint.createStatement().executeQuery(
                "explain JSON_PLAN select * from " + AUTO_TBL_NAME1 + " where name='12' order by id");
            connWithoutHint.createStatement()
                .executeQuery("explain JSON_PLAN select * from " + AUTO_TBL_NAME3 + " where name='12' order by id");

            connWithHint.createStatement().executeQuery(
                "explain EXECUTE select * from " + AUTO_TBL_NAME3 + " where name='12' order by id");
            connWithoutHint.createStatement().executeQuery(
                "explain EXECUTE select * from " + AUTO_TBL_NAME3 + " where name='12' order by id");

            connWithHint.createStatement().executeQuery(
                "explain SHARDING select * from " + AUTO_TBL_NAME3 + " where name='12' order by id");
            connWithHint.createStatement().execute("create sequence session_hint_test_seq start with 100");
            connWithHint.createStatement().execute("alter sequence session_hint_test_seq start with 99");
            connWithHint.createStatement().execute("drop sequence session_hint_test_seq");

            connWithHint.createStatement().execute("clear ccl_rules");
            connWithHint.createStatement().execute("clear ccl_triggers");

            // thread running might be different
            connWithHint.createStatement().executeQuery("show db status");
            connWithHint.createStatement().executeQuery("show full db status");

            connWithHint.createStatement().executeQuery("show ds");
            connWithHint.createStatement().executeQuery("show trans");
            connWithHint.createStatement().executeQuery("show node");
            connWithHint.createStatement().executeQuery("show slow");
            connWithHint.createStatement().executeQuery("show full trans");
            connWithHint.createStatement().executeQuery("show ccl_rules");
            connWithHint.createStatement().executeQuery("show physical_slow");
            connWithHint.createStatement().executeQuery("show broadcasts");
            connWithHint.createStatement().executeQuery("show connection");
            connWithHint.createStatement().executeQuery("show rule");
            connWithHint.createStatement().executeQuery("check table " + AUTO_TBL_NAME3);
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
            connWithHint.createStatement().execute("use " + AUTO_DB_NAME);

            connWithHint.createStatement().execute("set partition_hint=p3");

            ResultSet rs;

            // test normal hint won't interrupt partition hint
            // get group name and partition name
            rs = connWithHint.createStatement().executeQuery("show topology from  " + AUTO_TBL_NAME1);

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
            connWithHint.createStatement().execute("trace /*TDDL:slave()*/ select * from " + AUTO_TBL_NAME1);

            connWithHint.createStatement().execute("trace /*TDDL:a()*/ select * from " + AUTO_TBL_NAME2);

            connWithHint.createStatement()
                .execute("trace /*TDDL:test_hint=true*/ select * from " + AUTO_TBL_NAME1);
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
            c.createStatement().execute("use " + SHARD_DB_NAME);
            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP:00'");
            c.createStatement().execute("trace select * from " + DRDS_TBL_NAME);
            ResultSet rs = c.createStatement().executeQuery("show trace");

            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupName.equalsIgnoreCase("PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP"));
            }

            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP:1'");
            c.createStatement().execute("trace select * from " + DRDS_TBL_NAME);
            rs = c.createStatement().executeQuery("show trace");

            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupName.equalsIgnoreCase("PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP"));
            }

            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP:4'");
            try {
                c.createStatement().execute("trace select * from " + DRDS_TBL_NAME);
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage()
                    .contains("table index overflow in target group from direct HINT for sharding table"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).close();

            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintWithJoin() throws SQLException {
        // drds table with different sharding table num, report error
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use " + SHARD_DB_NAME);
            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP:00'");

            try {
                c.createStatement()
                    .execute("select * from " + DRDS_TBL_NAME + " a join " + DRDS_TBL_NAME2 + " b on a.id=b.id");
                Assert.fail("should report error");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
                Assert.assertTrue(e.getMessage()
                    .contains("different sharding table num in partition hint mode"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).close();
            log.info("session hint test end");
        }

        // auto table in different table group, report error
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use " + AUTO_DB_NAME);
            c.createStatement().execute("set partition_hint='p1'");

            // create table group
            c.createStatement().execute("create tablegroup if not exists partition_hint_test_tg1");
            c.createStatement().execute("create tablegroup if not exists partition_hint_test_tg2");

            // create table into table group partition_test_tg1
            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_group_tbl1` (\n"
                + "        `a` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "TABLEGROUP=partition_hint_test_tg1\n"
                + "PARTITION BY RANGE(YEAR(a))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_group_tbl2` (\n"
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
                    .execute(
                        "select * from partition_hint_test_group_tbl1 a join partition_hint_test_group_tbl2 b on a.a=b.a");
                Assert.fail("should report error");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage()
                    .contains("different table group in partition hint mode"));
            }

            // change table group to same one:partition_hint_test_tbl1
            c.createStatement()
                .execute("ALTER TABLE partition_hint_test_group_tbl2 set TABLEGROUP=partition_hint_test_tg1;");

            // test if same table group work
            c.createStatement()
                .execute(
                    "select * from partition_hint_test_group_tbl1 a join partition_hint_test_group_tbl2 b on a.a=b.a");
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).createStatement().execute("drop table if exists partition_hint_test_group_tbl1");
            Objects.requireNonNull(c).createStatement().execute("drop table if exists partition_hint_test_group_tbl2");
            Objects.requireNonNull(c).createStatement().execute("drop tablegroup if exists partition_hint_test_tg1");
            Objects.requireNonNull(c).createStatement().execute("drop tablegroup if exists partition_hint_test_tg2");
            Objects.requireNonNull(c).close();
            log.info("session hint test end");
        }

        // test auto table join with broadcast table
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use " + AUTO_DB_NAME);
            c.createStatement().execute("set partition_hint='p1'");

            // create table group
            c.createStatement().execute("create tablegroup if not exists partition_hint_test_tg1");

            // create table into table group partition_test_tg1
            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_group_tbl1` (\n"
                + "        `a` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "TABLEGROUP=partition_hint_test_tg1\n"
                + "PARTITION BY RANGE(YEAR(a))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_broadcast_tbl2` (\n"
                + "        `a` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "BROADCAST");

            // test if table with broadcast table work
            c.createStatement()
                .execute(
                    "select * from partition_hint_test_group_tbl1 a join partition_hint_test_broadcast_tbl2 b on a.a=b.a");
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).createStatement().execute("drop table if exists partition_hint_test_group_tbl1");
            Objects.requireNonNull(c).createStatement()
                .execute("drop table if exists partition_hint_test_broadcast_tbl2");
            Objects.requireNonNull(c).createStatement().execute("drop tablegroup if exists partition_hint_test_tg1");
            Objects.requireNonNull(c).close();

            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintCrossSchema() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use " + SHARD_DB_NAME);
            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP:00'");

            c.createStatement().execute("select * from " + SHARD_DB_NAME + "." + DRDS_TBL_NAME);

            try {
                c.createStatement().execute("select * from " + AUTO_DB_NAME + "." + AUTO_TBL_NAME4);
                Assert.fail("should report error");
            } catch (SQLException e) {
                log.info(e.getMessage());
                Assert.assertTrue(
                    e.getMessage().contains("partition hint visit table crossing schema"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).close();
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
            c.createStatement().execute("use " + SHARD_DB_NAME);
            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP:00'");
            c.createStatement().execute("select * from " + SHARD_DB_NAME + "." + DRDS_TBL_NAME);

            try {
                c.createStatement().execute(
                    "select * from select_base_three_multi_db_multi_tb_i9JV_02 a join " + DRDS_TBL_NAME
                        + " b on a.id=b.id");
                Assert.fail("should report error");
            } catch (SQLException e) {
                log.info(e.getMessage());
                Assert.assertTrue(e.getMessage().contains("[ERR_TABLE_NOT_EXIST]"));
            }
            try {
                c.createStatement()
                    .execute(
                        "select * from select_base_three_multi_db_multi_tb_i9JV_06 a join " + DRDS_TBL_NAME
                            + " b on a.id=b.id");
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
            c.createStatement().execute("use " + AUTO_DB_NAME);
            c.createStatement().execute("set partition_hint=p2");
            c.createStatement().execute("select * from " + AUTO_DB_NAME + "." + AUTO_TBL_NAME4);

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
            Objects.requireNonNull(c).close();
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
            c.createStatement().execute("use " + SHARD_DB_NAME);
            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP:00'");

            ResultSet rs = c.createStatement().executeQuery("show full connection");

            boolean found = false;
            while (rs.next()) {
                String hint = rs.getString("PARTITION_HINT");
                if ("PARTITION_NAME_HINT_TEST_SHARD_000001_GROUP:00".equalsIgnoreCase(hint)) {
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
            c.createStatement().execute("use " + AUTO_DB_NAME);
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
            Objects.requireNonNull(c).close();
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
            c.createStatement().execute("use " + SHARD_DB_NAME);
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
            c.createStatement().execute("insert into " + tblName + "(id, bid, name) values(1, 11, 'a')");
            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_000000_GROUP'");

            c.createStatement().execute("insert into " + tblName + "(id, bid, name) values(2, 12, 'a')");

            Assert.fail("should report error");
        } catch (SQLException e) {
            Assert.assertTrue(
                e.getMessage().contains("[ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY]"));
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).close();
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
            c.createStatement().execute("use " + SHARD_DB_NAME);
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
            c.createStatement().execute("insert into " + tblName + "(id, bid, name) values(1, 11, 'a')");
            c.createStatement().execute("set partition_hint='PARTITION_NAME_HINT_TEST_SHARD_SINGLE_GROUP'");

            c.createStatement().execute("insert into " + tblName + "(id, bid, name) values(2, 12, 'a')");

            Assert.fail("should report error");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.assertTrue(
                e.getMessage().contains("[ERR_MODIFY_BROADCAST_TABLE_BY_HINT_NOT_ALLOWED]"));
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(c).close();
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
            c.createStatement().execute("use " + AUTO_DB_NAME);
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

    @Test
    public void testOldDirectHint() throws SQLException {
        // drds db test
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("use " + SHARD_DB_NAME);
            c.createStatement().execute("CREATE TABLE if not exists `multi_db_multi_tbl` (\n"
                + "\t`id` bigint NOT NULL AUTO_INCREMENT BY GROUP,\n"
                + "\t`bid` int DEFAULT NULL,\n"
                + "\t`name` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`id`),\n"
                + "\tKEY `auto_shard_key_bid` USING BTREE (`bid`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci  dbpartition by hash(`id`) tbpartition by hash(`bid`) tbpartitions 3");

            c.createStatement().execute("CREATE TABLE if not exists `multi_db` (\n"
                + "\t`id` bigint NOT NULL AUTO_INCREMENT BY GROUP,\n"
                + "\t`bid` int DEFAULT NULL,\n"
                + "\t`name` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`id`),\n"
                + "\tKEY `auto_shard_key_bid` USING BTREE (`bid`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci  dbpartition by hash(`id`)");

            ResultSet rs = c.createStatement().executeQuery("show topology from multi_db");
            Map<String, Set<String>> topologyForMultiDb = new HashMap<>();
            while (rs.next()) {
                if (topologyForMultiDb.containsKey(rs.getString("GROUP_NAME"))) {
                    topologyForMultiDb.get(rs.getString("GROUP_NAME")).add(rs.getString("TABLE_NAME"));
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(rs.getString("TABLE_NAME"));
                    topologyForMultiDb.put(rs.getString("GROUP_NAME"), set);
                }
            }
            rs.close();

            Map<String, Set<String>> topologyForMultiDbMultiTb = new HashMap<>();
            rs = c.createStatement().executeQuery("show topology from multi_db_multi_tbl");
            while (rs.next()) {
                if (topologyForMultiDbMultiTb.containsKey(rs.getString("GROUP_NAME"))) {
                    topologyForMultiDbMultiTb.get(rs.getString("GROUP_NAME")).add(rs.getString("TABLE_NAME"));
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(rs.getString("TABLE_NAME"));
                    topologyForMultiDbMultiTb.put(rs.getString("GROUP_NAME"), set);
                }
            }

            String sql =
                "explain analyze /*+TDDL({'type':'direct','vtab':'%s','dbid':'%s','realtabs':[%s]})*/ select count(1) from %s";

            // test direct hint with single group&&single table
            String tableName = "multi_db";
            Set<String> multiGroups = new HashSet<>();
            String testSql;
            String tableNamePhysical = null;
            for (Map.Entry<String, Set<String>> entry : topologyForMultiDb.entrySet()) {
                testSql = String.format(sql, tableName, entry.getKey(), "'" + entry.getValue().iterator().next() + "'",
                    tableName);
                tableNamePhysical = entry.getValue().iterator().next();
                c.createStatement().executeQuery(testSql);
                if (multiGroups.size() < 2) {
                    multiGroups.add(entry.getKey());
                }
            }

            // test direct hint with multi group
            assert tableNamePhysical != null && multiGroups.size() == 2;
            testSql =
                String.format(sql, tableName, String.join(",", multiGroups), "'" + tableNamePhysical + "'",
                    tableName);
            c.createStatement().executeQuery(testSql);

            // test direct hint with single group&&multi table
            tableName = "multi_db_multi_tbl";
            for (Map.Entry<String, Set<String>> entry : topologyForMultiDbMultiTb.entrySet()) {
                testSql =
                    String.format(sql, tableName, entry.getKey(), "'" + String.join("','", entry.getValue()) + "'",
                        tableName);
                tableNamePhysical = entry.getValue().iterator().next();
                c.createStatement().executeQuery(testSql);
                if (multiGroups.size() < 2) {
                    multiGroups.add(entry.getKey());
                }
            }

            // test direct hint with multi group && single physical table
            assert tableNamePhysical != null && multiGroups.size() == 2;
            testSql =
                String.format(sql, tableName, String.join(",", multiGroups), "'" + tableNamePhysical + "'",
                    tableName);
            c.createStatement().executeQuery(testSql);
        }

        // test auto table
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("use " + AUTO_DB_NAME);
            c.createStatement().execute("CREATE TABLE if not exists key_tbl(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY KEY(name, id)\n"
                + "PARTITIONS 8;");

            c.createStatement().execute("CREATE TABLE if not exists hash_tbl(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "partition by hash(id)\n"
                + "partitions 8;");

            ResultSet rs = c.createStatement().executeQuery("show topology from key_tbl");
            Map<String, Set<String>> topologyForMultiDb = new HashMap<>();
            while (rs.next()) {
                if (topologyForMultiDb.containsKey(rs.getString("GROUP_NAME"))) {
                    topologyForMultiDb.get(rs.getString("GROUP_NAME")).add(rs.getString("TABLE_NAME"));
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(rs.getString("TABLE_NAME"));
                    topologyForMultiDb.put(rs.getString("GROUP_NAME"), set);
                }
            }
            rs.close();

            Map<String, Set<String>> topologyForMultiDbMultiTb = new HashMap<>();
            rs = c.createStatement().executeQuery("show topology from hash_tbl");
            while (rs.next()) {
                if (topologyForMultiDbMultiTb.containsKey(rs.getString("GROUP_NAME"))) {
                    topologyForMultiDbMultiTb.get(rs.getString("GROUP_NAME")).add(rs.getString("TABLE_NAME"));
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(rs.getString("TABLE_NAME"));
                    topologyForMultiDbMultiTb.put(rs.getString("GROUP_NAME"), set);
                }
            }

            String sql =
                "explain analyze /*+TDDL({'type':'direct','vtab':'%s','dbid':'%s','realtabs':[%s]})*/ select count(1) from %s";

            // test direct hint with single group&&single table
            String tableName = "key_tbl";
            Set<String> multiGroups = new HashSet<>();
            String testSql;
            for (Map.Entry<String, Set<String>> entry : topologyForMultiDb.entrySet()) {
                testSql = String.format(sql, tableName, entry.getKey(), "'" + entry.getValue().iterator().next() + "'",
                    tableName);
                c.createStatement().executeQuery(testSql);
                if (multiGroups.size() < 2) {
                    multiGroups.add(entry.getKey());
                }
            }

            // test direct hint with single group&&multi table
            tableName = "hash_tbl";
            for (Map.Entry<String, Set<String>> entry : topologyForMultiDbMultiTb.entrySet()) {
                testSql =
                    String.format(sql, tableName, entry.getKey(), "'" + String.join("','", entry.getValue()) + "'",
                        tableName);
                c.createStatement().executeQuery(testSql);
            }
        }
    }

    @Test
    public void testDirectHintForAutoDB() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use " + AUTO_DB_NAME);
            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_direct_tbl1` (\n"
                + "        `id` bigint NOT NULL,\n"
                + "        `name` varchar(25) NOT NULL,\n"
                + "        `c_time` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "PARTITION BY RANGE(YEAR(c_time))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            c.createStatement().execute("CREATE TABLE if not exists `partition_hint_test_direct_tbl2` (\n"
                + "        `id` bigint NOT NULL,\n"
                + "        `name` varchar(25) NOT NULL,\n"
                + "        `c_time` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "PARTITION BY RANGE(YEAR(c_time))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB);");

            // test insert batch
            ResultSet rs = c.createStatement().executeQuery(
                "explain /*+TDDL({'type':'direct','dbid':'p0'})*/ select * from partition_hint_test_direct_tbl1");
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1));
            }
            rs.close();

            Assert.assertTrue(sb.toString().contains("PhyQuery"));

            rs = c.createStatement().executeQuery(
                "explain /*+TDDL({'type':'direct','dbid':'p0, p3'})*/ select * from partition_hint_test_direct_tbl1");
            int count = 0;
            while (rs.next()) {
                if (rs.getString(1).contains("PhyQuery")) {
                    count++;
                }
            }
            rs.close();

            Assert.assertTrue(count == 2);

            rs = c.createStatement().executeQuery(
                "explain  select /*+TDDL({'type':'direct','dbid':'p0, p3'})*/ * from partition_hint_test_direct_tbl1");
            count = 0;
            while (rs.next()) {
                if (rs.getString(1).contains("PhyQuery")) {
                    count++;
                }
            }
            rs.close();

            Assert.assertTrue(count == 2);

            rs = c.createStatement().executeQuery(
                "explain  insert /*+TDDL({'type':'direct','dbid':'p0, p3'})*/ into partition_hint_test_direct_tbl1(id, name, c_time) values(1, 'a', now())");
            count = 0;
            while (rs.next()) {
                if (rs.getString(1).contains("PhyQuery")) {
                    count++;
                }
            }
            rs.close();

            Assert.assertTrue(count == 2);

            rs = c.createStatement()
                .executeQuery("explain /*TDDL:NODE='p2'*/ select count(1) from partition_hint_test_direct_tbl1");
            count = 0;
            while (rs.next()) {
                if (rs.getString(1).contains("PhyQuery")) {
                    count++;
                }
            }
            rs.close();

            Assert.assertTrue(count == 1);

            rs = c.createStatement()
                .executeQuery("explain /*TDDL:NODE(p3,p2)*/ select count(1) from partition_hint_test_direct_tbl2");
            count = 0;
            while (rs.next()) {
                if (rs.getString(1).contains("PhyQuery")) {
                    count++;
                }
            }
            rs.close();

            Assert.assertTrue(count == 2);

            // group hint
            rs = c.createStatement()
                .executeQuery(
                    "explain /*TDDL:node='p3, p2, p1'*/ select count(1) from partition_hint_test_direct_tbl2");
            count = 0;
            while (rs.next()) {
                if (rs.getString(1).contains("PhyQuery")) {
                    count++;
                }
            }
            rs.close();

            Assert.assertTrue(count == 3);

        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_hint=''");
            log.info("session hint test end");
        }
    }

    @Test
    public void testGroupHintSlaveHint() throws SQLException {
        // make sure slave nodes exists
        Set<String> slaveNodes = new HashSet<>();
        try (Connection connection = getPolardbxConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("show datasources");
            while (rs.next()) {
                String name = rs.getString("NAME");
                if (name.contains("slave")) {
                    slaveNodes.add(name);
                }
            }

            if (slaveNodes.size() == 0) {
                log.warn("no slave nodes, skip test");
                return;
            }
        }

        // test group hint for drds table
        try (Connection connection = getPolardbxConnection()) {
            connection.createStatement().execute("drop database if exists group_hint_drds_test");
            connection.createStatement().execute("create database if not exists group_hint_drds_test");
            connection.createStatement().execute("use group_hint_drds_test");

            // create table into table group partition_test_tg1
            connection.createStatement().execute("CREATE TABLE multi_db_multi_tbl(\n"
                + " id bigint not null auto_increment, \n"
                + " bid int, \n"
                + " name varchar(30), \n"
                + " primary key(id)\n"
                + ") dbpartition by hash(id) tbpartition by hash(bid) tbpartitions 3;");

            testGroupHint("/*+TDDL_GROUP({groupIndex:1})*/", connection, slaveNodes, true, "multi_db_multi_tbl");
            testGroupHint("/*+TDDL_GROUP({groupIndex:0})*/", connection, slaveNodes, false, "multi_db_multi_tbl");
            testGroupHint("/*+TDDL_GROUP({groupIndex:-2})*/", connection, slaveNodes, false, "multi_db_multi_tbl");
            testGroupHint("/*+TDDL:master()*/", connection, slaveNodes, false, "multi_db_multi_tbl");
            testGroupHint("/*+TDDL:slave()*/", connection, slaveNodes, true, "multi_db_multi_tbl");
        }

        // test group hint for auto table
        try (Connection connection = getPolardbxConnection()) {
            connection.createStatement().execute("drop database if exists group_hint_auto_test");
            connection.createStatement().execute("create database if not exists group_hint_auto_test mode=auto");
            connection.createStatement().execute("use group_hint_auto_test");

            // create table into table group partition_test_tg1
            connection.createStatement().execute("CREATE TABLE if not exists `group_hint_tbl1` (\n"
                + "        `id` bigint NOT NULL,\n"
                + "        `name` varchar(25) NOT NULL,\n"
                + "        `c_time` datetime NOT NULL\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
                + "PARTITION BY RANGE(YEAR(c_time))\n"
                + "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
                + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB)");

            testGroupHint("/*+TDDL_GROUP({groupIndex:1})*/", connection, slaveNodes, true, "group_hint_tbl1");
            testGroupHint("/*+TDDL_GROUP({groupIndex:0})*/", connection, slaveNodes, false, "group_hint_tbl1");
            testGroupHint("/*+TDDL_GROUP({groupIndex:-2})*/", connection, slaveNodes, false, "group_hint_tbl1");
            testGroupHint("/*+TDDL:master()*/", connection, slaveNodes, false, "group_hint_tbl1");
            testGroupHint("/*+TDDL:slave()*/", connection, slaveNodes, true, "group_hint_tbl1");
        }

        // test group hint + direct hint for auto table
        try (Connection connection = getPolardbxConnection()) {
            connection.createStatement().execute("use group_hint_auto_test");
            testGroupHint("/*+TDDL_GROUP({groupIndex:1})*//*+TDDL({\"dbid\":\"p2,p3\",\"type\":\"direct\"})*/",
                connection, slaveNodes, true, "group_hint_tbl1");
            ResultSet rs = connection.createStatement().executeQuery("show trace");
            int count = 0;
            while (rs.next()) {
                if (rs.getString(1).contains("PhyQuery")) {
                    count++;
                }
            }
            Assert.assertTrue(count == 2);
            testGroupHint(
                "/*+TDDL_GROUP({groupIndex:0})*//*+TDDL({\"dbindex\":true,\"dbid\":'p1, p0, p3',\"type\":\"direct\"})*/",
                connection, slaveNodes, false, "group_hint_tbl1");
            rs = connection.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                if (rs.getString(1).contains("PhyQuery")) {
                    count++;
                }
            }
            Assert.assertTrue(count == 3);
        }
    }

    @Test
    public void testSessionHintWithFlashBack() throws SQLException, InterruptedException {
        Connection connWithHint = null;
        try {
            connWithHint = getPolardbxConnection();
            connWithHint.createStatement().execute("use " + AUTO_DB_NAME);

            connWithHint.createStatement().execute("set partition_hint=p3");
            connWithHint.createStatement().execute("set global ENABLE_FORBID_PUSH_DML_WITH_HINT=false");
            Thread.sleep(2000);

            ResultSet rs;

            // clear data
            connWithHint.createStatement().execute("truncate table " + AUTO_TBL_NAME1);

            // Ensure the as of timestamp is valid.
            Thread.sleep(1000);

            // test insert into partition table
            Calendar current = Calendar.getInstance();
            current.setTimeInMillis(current.getTimeInMillis() - 5000);
            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            connWithHint.setAutoCommit(false);
            connWithHint.createStatement()
                .execute("insert into " + AUTO_TBL_NAME1 + "(bid, birthday) values(8, now())");
            connWithHint.commit();
            String sql = "select * from " + AUTO_TBL_NAME1 + " as of timestamp '" + f.format(current.getTime())
                + "' where bid=8";
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
            connWithHint.createStatement().execute("use " + AUTO_DB_NAME);

            connWithHint.createStatement().execute("set partition_hint=p3");
            connWithHint.createStatement().execute("set global ENABLE_FORBID_PUSH_DML_WITH_HINT=true");
            Thread.sleep(2000);
            connWithHint.setAutoCommit(false);

            connWithHint.createStatement().execute("set partition_hint=p3");
            try {
                connWithHint.createStatement()
                    .execute("insert into " + AUTO_TBL_NAME1 + "(bid, birthday) values(18, now())");
                Assert.fail(" dml should be forbidden by ENABLE_FORBID_PUSH_DML_WITH_HINT=true");
            } catch (SQLException e) {
                if (!e.getMessage().contains("Unsupported to push physical dml by hint ")) {
                    throw e;
                }
            }

            try {
                connWithHint.createStatement().execute("update " + AUTO_TBL_NAME1 + " set bid=18");
                Assert.fail(" dml should be forbidden by ENABLE_FORBID_PUSH_DML_WITH_HINT=true");
            } catch (SQLException e) {
                if (!e.getMessage().contains("Unsupported to push physical dml by hint ")) {
                    throw e;
                }
            }

            try {
                connWithHint.createStatement()
                    .execute("insert into " + AUTO_TBL_NAME1 + " select * from " + AUTO_TBL_NAME2);
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
            connWithHint.createStatement().execute("use " + AUTO_DB_NAME);

            connWithHint.createStatement().execute("set partition_hint=p3");
            connWithHint.createStatement().execute("set global ENABLE_FORBID_PUSH_DML_WITH_HINT=false");
            Thread.sleep(2000);

            // test partition hint working
            connWithHint.createStatement().execute("set partition_hint=p3");

            connWithHint.createStatement()
                .execute("insert into " + AUTO_TBL_NAME1 + "(bid, birthday) values(19, now())");

            connWithHint.createStatement().execute("update " + AUTO_TBL_NAME1 + " set bid=19");
            connWithHint.createStatement().execute("truncate table " + AUTO_TBL_NAME1);
            connWithHint.createStatement().execute(
                "insert into " + AUTO_TBL_NAME2 + "(bid, birthday)  " + "select bid, birthday from " + AUTO_TBL_NAME1);
        } finally {
            Objects.requireNonNull(connWithHint).createStatement().execute("set partition_hint=''");
            Objects.requireNonNull(connWithHint).close();
            log.info("session hint test end");
        }
    }

    @Test
    public void testTargetTableHint() throws SQLException {

        try (Connection connWithHint = getPolardbxConnection()) {
            connWithHint.createStatement().execute("use " + AUTO_DB_NAME);
            String groupName = null;
            String tableName = null;
            try (ResultSet rs = connWithHint.createStatement().executeQuery("show topology " + AUTO_TBL_NAME1)) {
                if (rs.next()) {
                    groupName = rs.getString("GROUP_NAME");
                    tableName = rs.getString("TABLE_NAME");
                }
            }
            String hint =
                String.format("/*+TDDL: SCAN(\"%s\", REAL_TABLE=(\"%s\"), NODE=\"%s\") */",
                    AUTO_TBL_NAME1, tableName, groupName);
            connWithHint.createStatement()
                .execute("trace " + hint + " select * from " + AUTO_TBL_NAME1 + " where id in (1, 2, 3)");
            int traceResultCount = 0;
            try (ResultSet rs = connWithHint.createStatement().executeQuery("show trace")) {
                while (rs.next()) {
                    traceResultCount++;
                    String traceGroupName = rs.getString("GROUP_NAME");
                    String params = rs.getString("PARAMS");
                    Assert.assertTrue(traceGroupName.equalsIgnoreCase(groupName), "wrong group name by HINT");
                    Assert.assertTrue(params.contains(tableName), "wrong table name by HINT");
                }
            }
            Assert.assertTrue(traceResultCount == 1, "should access exactly one partition");
        }
    }

    private void testGroupHint(String hint, Connection connection, Set<String> slaveNodes, boolean shouldContain,
                               String table)
        throws SQLException {
        connection.createStatement().executeQuery("trace " + hint + " select * from " + table);
        ResultSet rs = connection.createStatement().executeQuery("show trace");

        while (rs.next()) {
            String dbkey = rs.getString("DBKEY_NAME");
            String dnKey = dbkey.split("#")[1];
            if (shouldContain) {
                Assert.assertTrue(slaveNodes.contains(dnKey));
            } else {
                Assert.assertTrue(!slaveNodes.contains(dnKey));
            }
        }
        rs.close();
    }

}
