package com.alibaba.polardbx.qatest.dql.auto.spm;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.clearspring.analytics.util.Lists;
import org.glassfish.jersey.internal.guava.Sets;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.BASELINE_INFO;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.PLAN_INFO;

/**
 * @author fangwu
 */
@FixMethodOrder(MethodSorters.JVM)
public class SpmTest extends BaseTestCase {
    private static final String DB_NAME = "SPM_TEST_DB";
    private static final String TB_NAME = "SPM_TEST_TB";

    private static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `l_i_order` (`order_id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`order_id`) partitions 2";

    private static final String BASELINE_ADD =
        "baseline add sql /*TDDL:a()*/ select 1 from %s a join %s b on a.%s=b.%s";

    private static final String CREATE_BASELINE_INFO =
        "create table if not exists `" + GmsSystemTables.BASELINE_INFO + "` (\n"
            + "  `id` bigint not null,\n"
            + "  `schema_name` varchar(64) not null default '',\n"
            + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
            + "  `gmt_created` timestamp default current_timestamp,\n"
            + "  `sql` mediumtext not null,\n"
            + "  `table_set` text not null,\n"
            + "  `extend_field` longtext default null comment 'json string extend field',\n"
            + "  primary key `id_key` (`schema_name`, `id`)\n"
            + ") engine=innodb default charset=utf8";

    private static final String CREATE_PLAN_INFO =
        "create table if not exists `" + GmsSystemTables.PLAN_INFO + "` (\n"
            + "  `id` bigint(20) not null,\n"
            + "  `schema_name` varchar(64) not null,\n"
            + "  `baseline_id` bigint(20) not null,\n"
            + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
            + "  `gmt_created` timestamp default current_timestamp,\n"
            + "  `last_execute_time` timestamp null default null,\n"
            + "  `plan` longtext not null,\n"
            + "  `plan_type` varchar(255) null,\n"
            + "  `plan_error` longtext null,\n"
            + "  `choose_count` bigint(20) not null,\n"
            + "  `cost` double not null,\n"
            + "  `estimate_execution_time` double not null,\n"
            + "  `accepted` tinyint(4) not null,\n"
            + "  `fixed` tinyint(4) not null,\n"
            + "  `trace_id` varchar(255) not null,\n"
            + "  `origin` varchar(255) default null,\n"
            + "  `estimate_optimize_time` double default null,\n"
            + "  `cpu` double default null,\n"
            + "  `memory` double default null,\n"
            + "  `io` double default null,\n"
            + "  `net` double default null,\n"
            + "  `tables_hashcode` bigint not null default 0,\n"
            + "  `extend_field` longtext default null comment 'json string extend field',\n"
            + "  primary key `primary_key` (`schema_name`, `id`, `baseline_id`)\n,"
            + "  key `baseline_id_key` (`schema_name`, `baseline_id`)\n"
            + ") engine=innodb default charset=utf8";

    private static final String MOVE_SPM_BASELINE_TO_BASELINE_INFO =
        "REPLACE INTO BASELINE_INFO (`ID`, `SCHEMA_NAME`, `GMT_MODIFIED`, `GMT_CREATED`, `SQL`, `TABLE_SET`, `EXTEND_FIELD`) "
            + "SELECT  DISTINCT `ID`, `SCHEMA_NAME`, `GMT_MODIFIED`, `GMT_CREATED`, `SQL`, `TABLE_SET`, `EXTEND_FIELD`"
            + " FROM SPM_BASELINE WHERE SCHEMA_NAME='" + DB_NAME + "'";

    private static final String MOVE_SPM_PLAN_TO_PLAN_INFO =
        "REPLACE INTO PLAN_INFO (`ID`, `SCHEMA_NAME`, `BASELINE_ID`, `GMT_MODIFIED`, `GMT_CREATED`, `LAST_EXECUTE_TIME`, `PLAN`, `PLAN_TYPE`, "
            + "`PLAN_ERROR`, `CHOOSE_COUNT`, `COST`, `ESTIMATE_EXECUTION_TIME`, `ACCEPTED`, `FIXED`, `TRACE_ID`, `ORIGIN`, "
            + "`ESTIMATE_OPTIMIZE_TIME`, `CPU`, `MEMORY`, `IO`, `NET`, `TABLES_HASHCODE`, `EXTEND_FIELD`) "
            + "SELECT distinct `ID`, `SCHEMA_NAME`, `BASELINE_ID`, `GMT_MODIFIED`, `GMT_CREATED`, `LAST_EXECUTE_TIME`, `PLAN`, `PLAN_TYPE`, "
            + "`PLAN_ERROR`, `CHOOSE_COUNT`, `COST`, `ESTIMATE_EXECUTION_TIME`, `ACCEPTED`, `FIXED`, `TRACE_ID`, `ORIGIN`, "
            + "`ESTIMATE_OPTIMIZE_TIME`, `CPU`, `MEMORY`, `IO`, `NET`, `TABLES_HASHCODE`, `EXTEND_FIELD` FROM SPM_PLAN WHERE SCHEMA_NAME='"
            + DB_NAME + "'";

    private static final String DELETE_SPM_BASELINE_BY_SCHEMA =
        "DELETE FROM SPM_BASELINE WHERE SCHEMA_NAME='" + DB_NAME + "'";

    private static final String DELETE_SPM_PLAN_BY_SCHEMA = "DELETE FROM SPM_PLAN WHERE SCHEMA_NAME='" + DB_NAME + "'";

    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            c.createStatement().execute("use " + DB_NAME);
            c.createStatement().execute(String.format(CREATE_TABLE, TB_NAME));

            String baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "ID", "ID");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "ID", "order_id");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "order_id", "ID");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "buyer_id", "order_id");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "buyer_id", "buyer_id");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "seller_id", "seller_id");
            c.createStatement().execute(baselineAddSql);

            c.createStatement().execute("baseline persist");
        }
    }

    @Test
    public void testInstIdExist() throws Exception {
        // test metadb table meta
        try (Connection c = getMetaConnection()) {
            ResultSet rs = c.createStatement().executeQuery("desc spm_baseline");
            boolean hasInstId = false;
            while (rs.next()) {
                String fieldName = rs.getString("field");
                if (fieldName.equalsIgnoreCase("INST_ID")) {
                    hasInstId = true;
                    break;
                }
            }

            assert hasInstId;

            rs = c.createStatement().executeQuery("desc spm_baseline");
            hasInstId = false;
            while (rs.next()) {
                String fieldName = rs.getString("field");
                if (fieldName.equalsIgnoreCase("INST_ID")) {
                    hasInstId = true;
                    break;
                }
            }

            assert hasInstId;
        }
    }

    @Ignore
    public void testPlanCurd() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // check baseline in mem
            ResultSet rs = c.createStatement()
                .executeQuery(
                    "select count(distinct BASELINE_ID) from information_schema.spm where schema_name='" + DB_NAME
                        + "'");

            rs.next();
            int count = rs.getInt(1);
            assert count == 6;
            rs.close();

            // check baseline in metadb
            rs = c.createStatement()
                .executeQuery("select count(*) from metadb.spm_baseline where schema_name='" + DB_NAME
                    + "'");

            rs.next();
            count = rs.getInt(1);
            assert count == 6;
            rs.close();

            rs = c.createStatement()
                .executeQuery("select count(*) from metadb.spm_plan where schema_name='" + DB_NAME
                    + "' AND INST_ID IS NOT NULL");

            rs.next();
            count = rs.getInt(1);
            assert count == 6;
            rs.close();

            // test delete plan
            // get plan id
            rs = c.createStatement()
                .executeQuery(
                    "select baseline_id, plan_id from information_schema.spm where schema_name='" + DB_NAME
                        + "' and ACCEPTED");

            rs.next();
            String planId = rs.getString("PLAN_ID");
            int baselineId = rs.getInt("baseline_id");
            rs.close();
            // delete plan by plan id
            c.createStatement().execute("baseline delete " + baselineId);
            // check baseline in mem
            rs = c.createStatement()
                .executeQuery(
                    "select count(1) from information_schema.spm where schema_name='" + DB_NAME
                        + "' and baseline_id=" + baselineId);

            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();
            // check baseline in metadb
            rs = c.createStatement()
                .executeQuery(
                    "select count(distinct BASELINE_ID) from information_schema.spm where schema_name='" + DB_NAME
                        + "' and plan_id=" + planId);

            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();

            // test delete baseline
            c.createStatement().execute("baseline delete " + baselineId);
            // check baseline in mem
            rs = c.createStatement()
                .executeQuery(
                    "select count(distinct BASELINE_ID) from information_schema.spm where schema_name='" + DB_NAME
                        + "' and baseline_id=" + baselineId);

            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();

            // check baseline in metadb
            rs = c.createStatement()
                .executeQuery(
                    "select count(distinct BASELINE_ID) from information_schema.spm where schema_name='" + DB_NAME
                        + "' and baseline_id=" + baselineId);

            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();
        }
    }

    @Test
    public void testSpmReload() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // test baseline reload
            String baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "seller_id", "seller_id");
            ResultSet rs = c.createStatement().executeQuery(baselineAddSql);
            rs.next();
            int baselineId = rs.getInt("BASELINE_ID");
            rs.close();

            // check if this baseline id in mem
            rs = c.createStatement()
                .executeQuery("select count(1) from information_schema.spm where baseline_id = " + baselineId);
            rs.next();
            assert rs.getInt(1) > 0;
            rs.close();

            // change its inst id
            c.createStatement()
                .executeUpdate("update metadb.spm_baseline set inst_id='spm_test' where id=" + baselineId);

            // reload baseline
            c.createStatement().executeQuery("baseline load");

            // check if this baseline id still in mem
            rs = c.createStatement()
                .executeQuery("select count(1) from information_schema.spm where baseline_id = " + baselineId);
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();

            // check metadb
            rs = c.createStatement().executeQuery("select count(1) from metadb.spm_baseline where inst_id = ''");
            rs.next();
            int count = rs.getInt(1);
            assert count == 0;
            rs.close();

            rs = c.createStatement().executeQuery("select count(1) from metadb.spm_plan where inst_id =''");
            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();
        }
    }

    @Test
    public void testPlanMigration() throws Exception {
        try (Connection c = getMetaConnection()) {
            // create baseline_info
            c.createStatement().execute(CREATE_BASELINE_INFO);
            c.createStatement().execute(CREATE_PLAN_INFO);
            c.createStatement().execute(MOVE_SPM_BASELINE_TO_BASELINE_INFO);
            c.createStatement().execute(MOVE_SPM_PLAN_TO_PLAN_INFO);
            // remove plan by schema
            c.createStatement().execute(DELETE_SPM_BASELINE_BY_SCHEMA);
            c.createStatement().execute(DELETE_SPM_PLAN_BY_SCHEMA);
        }

        // refresh baseline
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // reload baseline
            c.createStatement().executeQuery("baseline load");

            // check mem
            ResultSet rs = c.createStatement()
                .executeQuery("select count(1) from information_schema.spm where schema_name = '" + DB_NAME + "'");
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();

            // check metadb
            rs = c.createStatement()
                .executeQuery("select count(1) from metadb.spm_baseline where schema_name = '" + DB_NAME + "'");
            rs.next();
            int count = rs.getInt(1);
            assert count == 0;
            rs.close();

            rs = c.createStatement()
                .executeQuery("select count(1) from metadb.spm_plan where schema_name = '" + DB_NAME + "'");
            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();
        }
    }

    /**
     * trigger schedule job, and check if each cn node got the same baseline collection,
     * and if this baseline collection had been persisted correctly
     */
    @Test
    public void testSPMBaseLineSyncScheduledJob() throws Exception {
        // trigger schedule job
        String sql;
        long now = System.currentTimeMillis();
        sql = "select schedule_id from metadb.SCHEDULED_JOBS where executor_type='BASELINE_SYNC'";
        String schedule_id = null;
        try (Connection c = this.getPolardbxConnection()) {
            ResultSet resultSet = c.createStatement().executeQuery(sql);
            resultSet.next();
            schedule_id = resultSet.getString("schedule_id");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        if (schedule_id == null) {
            Assert.fail("Cannot find schedule id for BASELINE_SYNC");
        }
        Thread.sleep(5000);
        sql = " fire schedule " + schedule_id;
        JdbcUtil.executeUpdateSuccess(this.getPolardbxConnection(), sql);

        sql = "select state from metadb.fired_SCHEDULEd_JOBS where schedule_id=" + schedule_id
            + " and gmt_modified>FROM_UNIXTIME(" + now + "/1000)";

        // waiting job done
        while (true) {
            //timeout control 5 min
            if (System.currentTimeMillis() - now > 5 * 60 * 1000) {
                Assert.fail("timeout:5 min");
                return;
            }
            Thread.sleep(5000);
            ResultSet rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
            boolean anySucc = false;
            while (rs.next()) {
                String state = rs.getString(1);
                if ("SUCCESS".equalsIgnoreCase(state)) {
                    anySucc = true;
                    rs.close();
                    break;
                }
            }
            rs.close();
            if (anySucc) {
                break;
            }
        }

        // check sync cover all cluster

        // get all inst id
        ResultSet rs = JdbcUtil.executeQuery("select inst_id, ip from server_info", this.getMetaConnection());
        Set<String> instIds = Sets.newHashSet();
        Set<String> ips = Sets.newHashSet();
        while (rs.next()) {
            instIds.add(rs.getString("inst_id"));
            ips.add(rs.getString("ip"));
        }
        rs.close();
        // get sync merge info
        rs = JdbcUtil.executeQuery(
            "select event from information_schema.module_event where module_name='spm' and "
                + "event like '%spm merge baseline%' and timestamp>FROM_UNIXTIME(" + now + "/1000)",
            this.getPolardbxConnection());

        List<String> syncInstIds = Lists.newArrayList();
        while (rs.next()) {
            String event = rs.getString("event");
            int start = event.indexOf("succeed ended,result: ") + "succeed ended,result: ".length();
            int end = event.indexOf(' ', start);

            syncInstIds.add(event.substring(start, end));
        }

        assert syncInstIds.containsAll(instIds);

        // make sure every node had been synced to load baseline
        sql =
            "select distinct host from information_schema.module_event where module_name='spm' and "
                + "event like '%BaselineLoadSyncAction%' and timestamp>FROM_UNIXTIME(" + now + "/1000)";
        rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        Set<String> hostSet = Sets.newHashSet();
        while (rs.next()) {
            hostSet.add(rs.getString("host"));
        }
        assert hostSet.containsAll(ips);
    }

    /**
     * this test method should be the last one to be executed in this case
     */
    @Ignore("this case will drop database and affect other test cases")
    public void testDropDB() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // drop database
            c.createStatement().execute("use information_schema");
            c.createStatement().execute("drop database " + DB_NAME);

            // check baseline in mem
            ResultSet rs = c.createStatement()
                .executeQuery("select count(1) from information_schema.spm where schema_name = '" + DB_NAME + "'");
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();

            // check baseline in metadb
            rs = c.createStatement()
                .executeQuery("select count(1) from metadb.spm_baseline where schema_name = '" + DB_NAME + "'");
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();

            rs = c.createStatement()
                .executeQuery("select count(1) from metadb.spm_plan where schema_name = '" + DB_NAME + "'");
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();
        }
    }

}
