package com.alibaba.polardbx.gms.util;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class PartNameUtilTest {

    @Test
    public void testGenNewPartForDupPartName() throws SQLException {
        String partName1 = "p20201202";
        String newDupIndexStr1 = "@01";
        String newPartName1 = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partName1);
        System.out.println(newPartName1);
        Assert.assertEquals(newPartName1, partName1 + newDupIndexStr1);

        String partName2 = "p20201202";
        String oldDupIndexStr2 = "@01";
        String newDupIndexStr2 = "@02";
        String newPartName2 = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partName2 + oldDupIndexStr2);
        System.out.println(newPartName2);
        Assert.assertEquals(newPartName2, partName2 + newDupIndexStr2);

        String partName3 = "p0123451234567890";
        String oldDupIndexStr3 = "@01";
        String newPartName3 = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partName3);
        System.out.println(newPartName3);
        Assert.assertTrue(newPartName3.endsWith("@01"));

        String partName4 = "p9012345678901234567890123456789";
        String oldDupIndexStr4 = "@01";
        String newPartName4 = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partName4);
        System.out.println(newPartName4);
        Assert.assertTrue(newPartName4.endsWith("@01") && newPartName4.contains("$"));

        String partName5 = newPartName4;
        String oldDupIndexStr5 = "@02";
        String newPartName5 = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partName5);
        System.out.println(newPartName5);
        Assert.assertTrue(newPartName5.endsWith(oldDupIndexStr5) && newPartName5.contains("$"));

        String partName6 = "p-11@11";
        String newDupIndexStr6 = "@12";
        String newPartName6 = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partName6);
        System.out.println(newPartName6);
        Assert.assertTrue(newPartName6.endsWith(newDupIndexStr6));

        //  * BIGINT (8 Bytes): -9223372036854775808 ~ 9223372036854775807
        //  * BIGINT UNSIGNED (8 Bytes): 0~18446744073709551615
        String partName7 = "p-9223372036854775808@11";
        String newDupIndexStr7 = "@12";
        String newPartName7 = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partName7);
        System.out.println(newPartName7);
        Assert.assertTrue(newPartName7.endsWith(newDupIndexStr7));

        //  * BIGINT (8 Bytes): -9223372036854775808 ~ 9223372036854775807
        //  * BIGINT UNSIGNED (8 Bytes): 0~18446744073709551615
        String partName8 = "p18446744073709551615@11";
        String newDupIndexStr8 = "@12";
        String newPartName8 = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partName8);
        System.out.println(newPartName8);
        Assert.assertTrue(newPartName8.endsWith(newDupIndexStr8));
    }
}
