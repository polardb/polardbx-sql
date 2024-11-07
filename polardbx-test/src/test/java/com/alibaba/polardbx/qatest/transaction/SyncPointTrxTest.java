package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SyncPointTrxTest extends CrudBasedLockTestCase {
    final private Connection metaDbConn = getMetaConnection();
    final private Connection polarxConn = getPolardbxConnection();
    final private static String QUERY_SYNC_POINT_META_COUNT = "select count(0) from cdc_sync_point_meta";

    @Test
    public void test() throws SQLException, InterruptedException {
        if (isMySQL80()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global enable_polarx_sync_point = true");
        Thread.sleep(1000);
        ResultSet rs = JdbcUtil.executeQuerySuccess(metaDbConn, QUERY_SYNC_POINT_META_COUNT);
        Assert.assertTrue("Not found count of sync point meta", rs.next());
        long before = rs.getLong(1);
        rs.close();

        triggerSyncPoint();
        triggerSyncPoint();

        rs = JdbcUtil.executeQuerySuccess(metaDbConn, QUERY_SYNC_POINT_META_COUNT);
        Assert.assertTrue("Not found count of sync point meta", rs.next());
        long after = rs.getLong(1);
        Assert.assertTrue(after > before);
    }

    private void triggerSyncPoint() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn,
            "call polardbx.trigger_sync_point_trx()");
        Assert.assertTrue("Not found result of sync point trigger", rs.next());
        String result = rs.getString(1);
        Assert.assertEquals("OK", result);
    }
}
