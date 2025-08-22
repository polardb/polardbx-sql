package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

@CdcIgnore(ignoreReason = "call polardbx.columnar_set_config 语法cdc无法同步，造成报错")
public class ColumnarSetConfigTest extends DDLBaseNewDBTestCase {
    private final static String TABLE_NAME = "ColumnarSetConfigTest_tb";
    private final static String INDEX_NAME = "cci";
    final String creatTable = "CREATE TABLE " + TABLE_NAME + " ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    PRIMARY KEY (`id`) \n"
        + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`id`)";
    final String createCci =
        "ALTER TABLE " + TABLE_NAME + " ADD CLUSTERED COLUMNAR INDEX " + INDEX_NAME + "(`id`) PARTITION BY KEY(`id`)";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(TABLE_NAME);
    }

    @After
    public void after() {
        dropTableIfExists(TABLE_NAME);
    }

    @Test
    public void test() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTable);
        createCciSuccess(createCci);

        String sql = String.format(
            "select index_id from information_schema.columnar_status where table_schema = database() and table_name = '%s'",
            TABLE_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        long indexId = rs.getLong(1);

        sql = String.format("call polardbx.columnar_set_config(%s, 'type', 'snapshot')", indexId);
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        sql = "show full create table " + TABLE_NAME;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(2).contains("\"TYPE\":\"SNAPSHOT\""));

        sql = "delete from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "call polardbx.columnar_set_config('COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION', 'false')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "select * from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("columnar_heartbeat_interval_ms_self_adaption", rs.getString("config_key"));
        Assert.assertEquals("false", rs.getString("config_value"));

        sql = "delete from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "call polardbx.columnar_set_config('COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION', 'off')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "select * from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("columnar_heartbeat_interval_ms_self_adaption", rs.getString("config_key"));
        Assert.assertEquals("false", rs.getString("config_value"));

        sql = "delete from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "call polardbx.columnar_set_config('COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION', '0')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "select * from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("columnar_heartbeat_interval_ms_self_adaption", rs.getString("config_key"));
        Assert.assertEquals("false", rs.getString("config_value"));

        sql = "delete from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "call polardbx.columnar_set_config('COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION', 'true')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "select * from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("columnar_heartbeat_interval_ms_self_adaption", rs.getString("config_key"));
        Assert.assertEquals("true", rs.getString("config_value"));

        sql = "delete from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "call polardbx.columnar_set_config('COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION', '1')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "select * from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("columnar_heartbeat_interval_ms_self_adaption", rs.getString("config_key"));
        Assert.assertEquals("true", rs.getString("config_value"));

        sql = "delete from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "call polardbx.columnar_set_config('COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION', 'on')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "select * from metadb.columnar_config where config_key = 'COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION'";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("columnar_heartbeat_interval_ms_self_adaption", rs.getString("config_key"));
        Assert.assertEquals("true", rs.getString("config_value"));
        sql = "call polardbx.columnar_set_config('COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION', 'k')";
        JdbcUtil.executeFailed(tddlConnection, sql, "config error by Value should be boolean");
        sql = "call polardbx.columnar_set_config('COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION', null)";
        JdbcUtil.executeFailed(tddlConnection, sql, "config error by Value should be boolean");
    }

    @Test
    public void testBackup0() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTable);
        createCciSuccess(createCci);

        String sql = String.format(
            "select index_id from information_schema.columnar_status where table_schema = database() and table_name = '%s'",
            TABLE_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        long indexId = rs.getLong(1);

        sql = String.format("call polardbx.columnar_set_config(%s, 'type', 'snapshot')", indexId);
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        sql = "show full create table " + TABLE_NAME;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(2).contains("\"TYPE\":\"SNAPSHOT\""));

        sql = String.format(
            "select config_value from metadb.columnar_config where table_id = %s and config_key = 'columnar_backup_enable'",
            indexId);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("true", rs.getString(1));

        sql = String.format("call polardbx.columnar_set_config(%s, 'type', 'k')", indexId);
        JdbcUtil.executeFailed(tddlConnection, sql,
            "config error by Invalid type k, choices: DEFAULT, SNAPSHOT");

        sql = String.format("call polardbx.columnar_set_config(%s, 'type', null)", indexId);
        JdbcUtil.executeFailed(tddlConnection, sql,
            "config error by Invalid type NULL, choices: DEFAULT, SNAPSHOT");
    }

    @Test
    public void testBackup1() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTable);
        String createCciSql =
            "ALTER TABLE " + TABLE_NAME + " ADD CLUSTERED COLUMNAR INDEX " + INDEX_NAME + "(`id`) "
                + "PARTITION BY KEY(`id`) "
                + "COLUMNAR_OPTIONS='{ "
                + "\"type\":\"snapshot\","
                + "}'";
        createCciSuccess(createCciSql);

        String sql = String.format(
            "select index_id from information_schema.columnar_status where table_schema = database() and table_name = '%s'",
            TABLE_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        long indexId = rs.getLong(1);

        sql = "show full create table " + TABLE_NAME;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(2).contains("\"TYPE\":\"SNAPSHOT\""));

        sql = String.format(
            "select config_value from metadb.columnar_config where table_id = %s and config_key = 'columnar_backup_enable'",
            indexId);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("true", rs.getString(1));
    }

    @Test
    public void testBackup2() throws SQLException {
        String creatTableSql = "CREATE TABLE " + TABLE_NAME + " ( \n"
            + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
            + "    PRIMARY KEY (`id`), \n"
            + "    CLUSTERED COLUMNAR INDEX `cci_snapshot` (`id`) \n"
            + "    partition by hash(`id`) \n"
            + "    partitions 16 \n"
            + "    columnar_options='{\n"
            + "      \"type\":\"snapshot\",\n"
            + "    }'"
            + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`id`)";
        createCciSuccess(creatTableSql);

        String sql = String.format(
            "select index_id from information_schema.columnar_status where table_schema = database() and table_name = '%s'",
            TABLE_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        long indexId = rs.getLong(1);

        sql = "show full create table " + TABLE_NAME;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(2).contains("\"TYPE\":\"SNAPSHOT\""));

        sql = String.format(
            "select config_value from metadb.columnar_config where table_id = %s and config_key = 'columnar_backup_enable'",
            indexId);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("true", rs.getString(1));
    }
}
