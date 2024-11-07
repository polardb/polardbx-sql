package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimeZoneUtilsTest {

    @Test
    public void testNamedTimeZone() throws Exception {
        try {

            String tzSh = "Asia/Shanghai";
            InternalTimeZone timeZoneSh = TimeZoneUtils.convertFromMySqlTZ(tzSh);
            String zoneIdSh = timeZoneSh.getTimeZone().getID();
            int zoneIdShOffSet = timeZoneSh.getTimeZone().getRawOffset();
            System.out.println(zoneIdSh);
            System.out.println(zoneIdShOffSet);
            Assert.assertTrue(zoneIdSh.equalsIgnoreCase(tzSh));
            Assert.assertTrue(zoneIdShOffSet == 28800000);

            String tz0800 = "+08:00";
            InternalTimeZone timeZone0800 = TimeZoneUtils.convertFromMySqlTZ(tz0800);
            String zoneId0800 = timeZone0800.getTimeZone().getID();
            int zoneId0800OffSet = timeZone0800.getTimeZone().getRawOffset();
            System.out.println(zoneId0800);
            System.out.println(zoneId0800OffSet);
            Assert.assertTrue(zoneId0800.equalsIgnoreCase("GMT+08:00"));
            Assert.assertTrue(zoneId0800OffSet == 28800000);

            String tzTy = "Asia/Tokyo";
            InternalTimeZone timeZoneTy = TimeZoneUtils.convertFromMySqlTZ(tzTy);
            String zoneIdTy = timeZoneTy.getTimeZone().getID();
            int zoneIdTyOffSet = timeZoneTy.getTimeZone().getRawOffset();
            System.out.println(zoneIdTy);
            System.out.println(zoneIdTyOffSet);
            Assert.assertTrue(zoneIdTy.equalsIgnoreCase(tzTy));
            Assert.assertTrue(zoneIdTyOffSet == 32400000);

            String tz0900 = "+09:00";
            InternalTimeZone timeZone0900 = TimeZoneUtils.convertFromMySqlTZ(tz0900);
            String zoneId0900 = timeZone0900.getTimeZone().getID();
            int zoneId0900OffSet = timeZone0900.getTimeZone().getRawOffset();
            System.out.println(zoneId0900);
            System.out.println(zoneId0900OffSet);
            Assert.assertTrue(zoneId0900.equalsIgnoreCase("GMT+09:00"));
            Assert.assertTrue(zoneId0900OffSet == 32400000);

        } catch (Throwable ex) {
            ex.printStackTrace();
            Assert.fail("failed");
        }

    }
}
