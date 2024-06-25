package com.alibaba.polardbx.transaction.utils;

import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import org.junit.Assert;
import org.junit.Test;

public class XidTest {
    @Test
    public void testXid() {
        String schema0 = "test_schema_0";
        String group00 = "test_group_00";
        String group01 = "test_group_01_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        Assert.assertTrue(group01.length() > 64);
        long trxId = Long.parseLong("17cda2cd95800000", 16);
        long primaryGroupId0 = IServerConfigManager.getGroupUniqueId(schema0, group00);
        String xidString00 = XAUtils.toXidString(trxId, group00, primaryGroupId0, 1000);
        String xidString01 = XAUtils.toXidString(trxId, group01, primaryGroupId0, 1001);
        Assert.assertTrue(xidString00.length() <= 128);
        Assert.assertTrue(xidString01.length() <= 128);

        String bqual00 = getBqual(xidString00);
        XAUtils.XATransInfo xatransInfo = new XAUtils.XATransInfo(trxId, bqual00, primaryGroupId0);
        Assert.assertEquals(xatransInfo.toXidString(), xidString00);

        String bqual01 = getBqual(xidString01);
        xatransInfo = new XAUtils.XATransInfo(trxId, bqual01, primaryGroupId0);
        Assert.assertEquals(xatransInfo.toXidString(), xidString01);

        String schema1 = "test_schema_1";
        long primaryGroupId1 = IServerConfigManager.getGroupUniqueId(schema1, group00);
        String xidString10 = XAUtils.toXidString(trxId, group00, primaryGroupId1, 1000);
        String xidString11 = XAUtils.toXidString(trxId, group01, primaryGroupId1, 1001);
        Assert.assertTrue(xidString10.length() < 128);
        Assert.assertTrue(xidString11.length() < 128);

        String bqual10 = getBqual(xidString00);
        xatransInfo = new XAUtils.XATransInfo(trxId, bqual10, primaryGroupId1);
        Assert.assertEquals(xatransInfo.toXidString(), xidString10);

        String bqual11 = getBqual(xidString01);
        xatransInfo = new XAUtils.XATransInfo(trxId, bqual11, primaryGroupId1);
        Assert.assertEquals(xatransInfo.toXidString(), xidString11);

        // Same group names with different schema should have different xid.
        Assert.assertNotEquals(xidString00, xidString10);
        Assert.assertNotEquals(xidString01, xidString11);
    }

    private static String getBqual(String xid) {
        String bqual = xid.split(",")[1];
        int start = bqual.indexOf('\'') + 1;
        int end = bqual.lastIndexOf('\'');
        return bqual.substring(start, end);
    }
}
