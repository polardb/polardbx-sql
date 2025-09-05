package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnarSnapshotsTest extends ColumnarReadBaseTestCase {
    private static final String TABLE_NAME = "ColumnarSnapshotTest_t1";
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS `" + TABLE_NAME + "` ( \n"
        + "id int primary key,\n"
        + "a int,\n"
        + "gmt_created timestamp not null default current_timestamp,\n"
        + "gmt_modified timestamp not null default current_timestamp on update current_timestamp\n"
        + ") partition by key(id)";
    private static final String TMP_TABLE_NAME = TABLE_NAME + "_tmp";
    private static final String CREATE_TMP_TABLE = "create table if not exists " + TMP_TABLE_NAME + " (\n"
        + "  id int primary key,\n"
        + "  a int,\n"
        + "  gmt_created timestamp not null default current_timestamp,\n"
        + "  gmt_modified timestamp not null default current_timestamp on update current_timestamp\n"
        + ") partition by key(id)";
    private static final String CALL_COLUMNAR_FLUSH = "CALL polardbx.columnar_flush('%s', '%s', '%s')";
    private static final String CALL_COLUMNAR_FLUSH_GLOBAL = "CALL polardbx.columnar_flush()";

    @Before
    public void setUp() {
        JdbcUtil.dropTable(tddlConnection, TABLE_NAME);
        JdbcUtil.dropTable(tddlConnection, TMP_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TMP_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from metadb.columnar_config where table_id = 0 and config_key = 'SNAPSHOT_RETENTION_DAYS'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from metadb.columnar_config where table_id = 0 and config_key = 'AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from metadb.columnar_config where table_id = 0 and config_key = 'COLUMNAR_AUTO_SNAPSHOT_CONFIG'");
    }

    @After
    public void tearDown() {
        JdbcUtil.dropTable(tddlConnection, TABLE_NAME);
        JdbcUtil.dropTable(tddlConnection, TMP_TABLE_NAME);
    }

    @Test
    public void testSimple() throws SQLException, InterruptedException {
        String sql = "create clustered columnar index cci on " + TABLE_NAME + "(a) partition by key(id) "
            + " engine='EXTERNAL_DISK' "
            + " columnar_options='{"
            + "     \"type\":\"snapshot\", "
            + "     \"snapshot_retention_days\":\"7\""
            + " }'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show full create table " + TABLE_NAME);
        String tableDef = null;
        if (rs.next()) {
            tableDef = rs.getString(2);
        }
        System.out.println(tableDef);
        Assert.assertNotNull(tableDef);
        Assert.assertTrue(tableDef.contains("\"TYPE\":\"SNAPSHOT\""));
        Assert.assertTrue(tableDef.contains("\"SNAPSHOT_RETENTION_DAYS\":\"7\""));
        Assert.assertTrue(tableDef.contains("\"AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL\":\"-1\""));

        sql = "INSERT INTO " + TABLE_NAME + " (id, a) values (0, 0), (1, 0)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set transaction_policy = TSO");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "commit");

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(CALL_COLUMNAR_FLUSH, DB_NAME, TABLE_NAME, "cci"));
        Assert.assertTrue(rs.next());
        long tso0 = rs.getLong(1);
        waitColumnarFlush(tso0);

        sql = "UPDATE " + TABLE_NAME + " SET a = 100 where 1=1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set transaction_policy = TSO");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "commit");

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, CALL_COLUMNAR_FLUSH_GLOBAL);
        Assert.assertTrue(rs.next());
        long tso1 = rs.getLong(1);
        waitColumnarFlush(tso1);

        sql = "UPDATE " + TABLE_NAME + " SET a = 200 where 1=1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "commit");

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select sum(a) from " + TABLE_NAME
            + " as of tso " + tso0 + " force index(cci)");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(0, rs.getLong(1));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select sum(a) from " + TABLE_NAME
            + " as of tso " + tso1 + " force index(cci)");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(200, rs.getLong(1));

        sql = "INSERT INTO " + TMP_TABLE_NAME + " SELECT * FROM " + TABLE_NAME
            + " AS OF TSO " + tso1 + " FORCE INDEX (cci)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT sum(a) FROM " + TMP_TABLE_NAME);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(200, rs.getLong(1));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.columnar_snapshot_files(" + tso0 + ")");
        Assert.assertTrue(rs.next());

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.columnar_snapshot_files(" + tso1 + ")");
        Assert.assertTrue(rs.next());
    }

    @Test
    public void testFailPoint() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_clear = true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FP_FAILED_TABLE_SYNC = true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @FP_FAILED_TABLE_SYNC = 'true'");
        String sql = "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS='WaitColumnarTableCreationTask')*/"
            + " create clustered columnar index cci on " + TABLE_NAME + "(a) partition by key(id) "
            + " engine='EXTERNAL_DISK' "
            + " columnar_options='{"
            + "     \"type\":\"snapshot\", "
            + "     \"snapshot_retention_days\":\"7\""
            + " }'";
        JdbcUtil.executeFailed(tddlConnection, sql, "FP_FAILED_TABLE_SYNC");
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show full create table " + TABLE_NAME);
        String tableDef = null;
        if (rs.next()) {
            tableDef = rs.getString(2);
        }
        System.out.println(tableDef);
        Assert.assertNotNull(tableDef);
        Assert.assertFalse(tableDef.contains("\"TYPE\":\"SNAPSHOT\""));
        Assert.assertFalse(tableDef.contains("\"SNAPSHOT_RETENTION_DAYS\":\"7\""));
        Assert.assertFalse(tableDef.contains("\"AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL\":\"-1\""));
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_clear = true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FP_FAILED_TABLE_SYNC = false");
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select job_id from metadb.ddl_engine where ddl_type = 'CREATE_INDEX' and state = 'PAUSED' and object_name = 'ColumnarSnapshotTest_t1'");
        Assert.assertTrue(rs.next());
        long jobId = rs.getLong(1);
        JdbcUtil.executeSuccess(tddlConnection, "rollback ddl " + jobId);
    }

    @Test
    public void testAutoSnapshotByCron() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "call polardbx.columnar_auto_snapshot_config('DISABLE')");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Disable auto generated snapshot.", rs.getString(1));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Before config: null", rs.getString(1));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "call polardbx.columnar_auto_snapshot_config(SHOW)");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("No config found.", rs.getString(1));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "call polardbx.columnar_auto_snapshot_config('ENABLE', '* * * * * ?', '+08:00')");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Enable new auto generated snapshot.", rs.getString(1));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Before config: null", rs.getString(1));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Current config: cron expression: * * * * * ? zone id: +08:00", rs.getString(1));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "call polardbx.columnar_auto_snapshot_config(SHOW)");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Auto snapshot config: cron expression: * * * * * ? zone id: +08:00", rs.getString(1));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "call polardbx.columnar_auto_snapshot_config('ENABLE', '0 * * * * ?', '+08:00')");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Enable new auto generated snapshot.", rs.getString(1));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Before config: cron expression: * * * * * ? zone id: +08:00", rs.getString(1));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Current config: cron expression: 0 * * * * ? zone id: +08:00", rs.getString(1));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "call polardbx.columnar_auto_snapshot_config('DISABLE')");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Disable auto generated snapshot.", rs.getString(1));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Before config: cron expression: 0 * * * * ? zone id: +08:00", rs.getString(1));

        JdbcUtil.executeFailed(tddlConnection, "call polardbx.columnar_auto_snapshot_config()", "Bad arguments.");
        JdbcUtil.executeFailed(tddlConnection, "call polardbx.columnar_auto_snapshot_config('SHWO')", "Bad arguments.");
        JdbcUtil.executeFailed(tddlConnection, "call polardbx.columnar_auto_snapshot_config(123)", "Bad arguments.");

    }

    private void waitColumnarFlush(long tso0) throws InterruptedException, SQLException {
        ResultSet rs;
        int retry = 10;
        boolean succeed = false;
        while (retry-- > 0) {
            // Wait columnar to process flush.
            Thread.sleep(1000);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "select tso from information_schema.columnar_snapshots where tso <= " + tso0
                    + " order by tso desc limit 1");
            if (rs.next() && rs.getLong(1) == tso0) {
                succeed = true;
                break;
            }
        }

        Assert.assertTrue(succeed);
    }
}
