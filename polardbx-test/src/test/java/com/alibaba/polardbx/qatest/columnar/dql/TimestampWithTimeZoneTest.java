package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.columnar.dql.FullTypeTest.waitForSync;

public class TimestampWithTimeZoneTest extends ColumnarReadBaseTestCase {

    public static String PRIMARY_TABLE_NAME = "time_zone_test";
    public static String COLUMNAR_INDEX_NAME = "time_cci";

    @Before
    public void before() {
        JdbcUtil.dropTable(tddlConnection, PRIMARY_TABLE_NAME);
    }

    @After
    public void dropTable() {
        JdbcUtil.dropTable(tddlConnection, PRIMARY_TABLE_NAME);
    }

    @Test
    public void testCsvZeroTimestamp() throws InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE %s (\n"
                + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "  `c_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`c_datetime` datetime DEFAULT NULL,\n"
                + "  `c_date` date DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`id`)\n"
                + ") PARTITION BY KEY(`id`) PARTITIONS 3", PRIMARY_TABLE_NAME));
        ColumnarUtils.createColumnarIndex(tddlConnection, COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "id", "id", 3);
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+08:00'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(
                "insert into %s(id,c_timestamp,c_datetime,c_date) values(null,'0000-00-00 00:00:00',null,null),(null,null,'0000-00-00 00:00:00',null),(null,null,null,'0000-00-00 00:00:00')",
                PRIMARY_TABLE_NAME));

        waitForSync(tddlConnection);

        String columnarSql =
            "/*+TDDL: WORKLOAD_TYPE=AP*/select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME
                + ")";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary)";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);

        // check twice for different time zone
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+01:00'");
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }

    @Test
    public void testCsvTimestamp_1_fsp() throws InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE %s (\n"
                + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "\t`c_timestamp_1` timestamp(1) NOT NULL DEFAULT '2000-01-01 00:00:00.0',\n"
                + "\tPRIMARY KEY (`id`)\n"
                + ") PARTITION BY KEY(`id`) PARTITIONS 3", PRIMARY_TABLE_NAME));
        ColumnarUtils.createColumnarIndex(tddlConnection, COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "id", "id", 3);
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+08:00'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(
                "insert into %s (c_timestamp_1) values('1991-01-13 06:53:43.7') ,('2033-10-21 21:45:15.8') ,('2004-08-22 22:30:49.5') ,('2013-08-17 11:51:45.9') ,('1997-11-17 03:08:26.6') ,('1992-02-09 22:55:20.2') ,('1975-06-25 06:04:20.6') ,('1983-11-22 17:05:37.4') ,('2000-08-25 01:33:00.7') ,('2026-01-13 03:40:46.8') ,('2009-10-01 15:19:25.8') ,('1981-01-06 01:54:08.3') ,('2006-12-14 11:45:54.0') ,('1992-03-09 23:14:03.3') ,('1970-08-11 05:44:01.8') ,('2023-06-01 21:30:33.2') ,('2026-12-31 15:33:10.5') ,('2026-01-25 15:43:19.7') ,('1999-08-26 02:19:44.3') ,('1994-12-13 20:52:34.5') ,(null) ,(null) ,(null) ,(null)",
                PRIMARY_TABLE_NAME));

        waitForSync(tddlConnection);

        String columnarSql =
            "/*+TDDL: WORKLOAD_TYPE=AP*/select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME
                + ")";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary)";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);

        // check twice for different time zone
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+01:00'");
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }

    @Test
    public void testOrcTimestamp_1() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE %s (\n"
                + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "\t`c_timestamp_1` timestamp(1) NOT NULL DEFAULT '2000-01-01 00:00:00.0',\n"
                + "\tPRIMARY KEY (`id`)\n"
                + ") PARTITION BY KEY(`id`) PARTITIONS 3", PRIMARY_TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+08:00'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(
                "insert into %s (c_timestamp_1) values('1991-01-13 06:53:43.7') ,('2033-10-21 21:45:15.8') ,('2004-08-22 22:30:49.5') ,('2013-08-17 11:51:45.9') ,('1997-11-17 03:08:26.6') ,('1992-02-09 22:55:20.2') ,('1975-06-25 06:04:20.6') ,('1983-11-22 17:05:37.4') ,('2000-08-25 01:33:00.7') ,('2026-01-13 03:40:46.8') ,('2009-10-01 15:19:25.8') ,('1981-01-06 01:54:08.3') ,('2006-12-14 11:45:54.0') ,('1992-03-09 23:14:03.3') ,('1970-08-11 05:44:01.8') ,('2023-06-01 21:30:33.2') ,('2026-12-31 15:33:10.5') ,('2026-01-25 15:43:19.7') ,('1999-08-26 02:19:44.3') ,('1994-12-13 20:52:34.5') ,(null) ,(null) ,(null) ,(null)",
                PRIMARY_TABLE_NAME));
        ColumnarUtils.createColumnarIndex(tddlConnection, COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "id", "id", 3);

        String columnarSql =
            "/*+TDDL: WORKLOAD_TYPE=AP*/select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME
                + ")";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary)";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);

        // check twice for different time zone
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+01:00'");
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }

    @Test
    public void testDateTimeWithTimeZone() throws InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(
                "create table %s (c1 datetime default null , `pk` bigint(20) NOT NULL AUTO_INCREMENT, PRIMARY KEY (pk) ) partition by hash(c1) partitions 16",
                PRIMARY_TABLE_NAME));
        ColumnarUtils.createColumnarIndex(tddlConnection, COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "c1", "c1", 3);
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+08:00'");
        JdbcUtil.executeSuccess(tddlConnection, "set sql_mode=''");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(
                "insert into %s(c1) values (null),('2020-12-12 00:00:00'),('2020-11-11 23:59:59.123'),('2020-11-11 23:59:59.789'),('2020-12-11'),('0000-00-00 00:00:00'),('9999-99-99 99:99:99'),('');\n",
                PRIMARY_TABLE_NAME));

        waitForSync(tddlConnection);

        String columnarSql =
            "/*+TDDL: WORKLOAD_TYPE=AP*/select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME
                + ")";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary)";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);

        // check twice for different time zone
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+01:00'");
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }

    @Test
    public void testOrcDateTimeWithTimeZone() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(
                "create table %s (c1 datetime default null , `pk` bigint(20) NOT NULL AUTO_INCREMENT, PRIMARY KEY (pk) ) partition by hash(c1) partitions 16",
                PRIMARY_TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+08:00'");
        JdbcUtil.executeSuccess(tddlConnection, "set sql_mode=''");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(
                "insert into %s(c1) values (null),('2020-12-12 00:00:00'),('2020-11-11 23:59:59.123'),('2020-11-11 23:59:59.789'),('2020-12-11'),('0000-00-00 00:00:00'),('9999-99-99 99:99:99'),('');\n",
                PRIMARY_TABLE_NAME));
        ColumnarUtils.createColumnarIndex(tddlConnection, COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "c1", "c1", 3);

        String columnarSql =
            "/*+TDDL: WORKLOAD_TYPE=AP*/select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME
                + ")";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary)";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);

        // check twice for different time zone
        JdbcUtil.executeSuccess(tddlConnection, "set time_zone='+01:00'");
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }
}
