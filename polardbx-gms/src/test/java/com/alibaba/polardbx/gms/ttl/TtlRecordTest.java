package com.alibaba.polardbx.gms.ttl;

import org.junit.Assert;
import org.junit.Test;

public class TtlRecordTest {

    @Test
    public void testArcStatusSetBitVal() {
        try {
            TtlInfoRecord ttlInfoRecord = new TtlInfoRecord();
            ttlInfoRecord.setArcStatus(0);
            System.out.println(ttlInfoRecord.getArcStatus());
            System.out.println(ttlInfoRecord.getBitValFromArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_DATA_OF_TTL_TBL));

            //=======
            ttlInfoRecord.setBitValIntoArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_DATA_OF_TTL_TBL, false);
            System.out.println(ttlInfoRecord.getArcStatus());
            System.out.println(ttlInfoRecord.getBitValFromArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_DATA_OF_TTL_TBL));

            ttlInfoRecord.setBitValIntoArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_DATA_OF_TTL_TBL, true);
            System.out.println(ttlInfoRecord.getArcStatus());
            System.out.println(ttlInfoRecord.getBitValFromArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_DATA_OF_TTL_TBL));

            Assert.assertTrue(ttlInfoRecord.getArcStatus() == 1);

            //=========
            ttlInfoRecord.setBitValIntoArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_PART_OF_ARC_CCI, false);
            System.out.println(ttlInfoRecord.getArcStatus());
            System.out.println(ttlInfoRecord.getBitValFromArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_PART_OF_ARC_CCI));

            ttlInfoRecord.setBitValIntoArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_PART_OF_ARC_CCI, true);
            System.out.println(ttlInfoRecord.getArcStatus());
            System.out.println(ttlInfoRecord.getBitValFromArchiveStatus(
                TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_CLEANUP_EXPIRED_PART_OF_ARC_CCI));

            Assert.assertTrue(ttlInfoRecord.getArcStatus() == 3);

            //==========
            ttlInfoRecord.setBitValIntoArchiveStatus(TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_OPTIMIZING_TABLE_FOR_TTL,
                false);
            System.out.println(ttlInfoRecord.getArcStatus());
            System.out.println(
                ttlInfoRecord.getBitValFromArchiveStatus(TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_OPTIMIZING_TABLE_FOR_TTL));

            ttlInfoRecord.setBitValIntoArchiveStatus(TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_OPTIMIZING_TABLE_FOR_TTL,
                true);
            System.out.println(ttlInfoRecord.getArcStatus());
            System.out.println(
                ttlInfoRecord.getBitValFromArchiveStatus(TtlInfoRecord.ARCHIVE_STATUS_BIT_OF_OPTIMIZING_TABLE_FOR_TTL));

            Assert.assertTrue(ttlInfoRecord.getArcStatus() == 7);

        } catch (Throwable ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
