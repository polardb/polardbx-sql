package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnarSnapshotsScheduledJobTest extends ColumnarReadBaseTestCase {
    private static final String TABLE_NAME = "test_columnar_snapshots_xxx";
    private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ("
        + " id int primary key auto_increment, "
        + " name varchar(255), "
        + " CLUSTERED COLUMNAR INDEX `my_snapshot` (`id`) partition by key(`id`) engine='EXTERNAL_DISK' "
        + " columnar_options='{\"type\":\"snapshot\", \"snapshot_retention_days\":\"7\", \"auto_gen_columnar_snapshot_interval\":\"3\"}'"
        + ") partition by key (id)";
    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS " + TABLE_NAME;

    @Test
    public void testColumnarSnapshots() throws InterruptedException, SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, DROP_TABLE_SQL);
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE_SQL);
        String sql = "insert into " + TABLE_NAME + " (name) values ('Tom')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "select name from " + TABLE_NAME + " force index(my_snapshot)";
        int retry = 0;
        boolean found = false;
        do {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            if (rs.next() && "Tom".equalsIgnoreCase(rs.getString(1))) {
                found = true;
                break;
            }
            Thread.sleep(1000);
        } while (retry++ < 10);
        Assert.assertTrue(found);

        sql = "select tso_timestamp()";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        long tso0 = rs.getLong(1);

        sql = "call polardbx.COLUMNAR_GENERATE_SNAPSHOTS()";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select tso_timestamp()";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        long tso1 = rs.getLong(1);

        sql = "update " + TABLE_NAME + " set name = 'Jerry' where 1=1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        retry = 0;
        found = false;
        do {
            sql = "SELECT tso FROM INFORMATION_SCHEMA.COLUMNAR_SNAPSHOTS where TSO > " + tso0 + " and TSO < " + tso1;
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            if (rs.next()) {
                found = true;
                break;
            }
        } while (retry++ < 10);
        Assert.assertTrue(found);
        long tso2 = rs.getLong(1);

        sql = "select name from " + TABLE_NAME + " as of tso " + tso2 + " force index(my_snapshot)";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue("Tom".equalsIgnoreCase(rs.getString(1)));
    }
}
