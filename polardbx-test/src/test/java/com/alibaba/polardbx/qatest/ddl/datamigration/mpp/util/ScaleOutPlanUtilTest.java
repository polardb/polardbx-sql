package com.alibaba.polardbx.qatest.ddl.datamigration.mpp.util;

import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import io.airlift.slice.DataSize;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

public class ScaleOutPlanUtilTest extends TestCase {

    @Test
    public void testParseDataUnit() {
        String dataUnit = "16MB";
        long dataSize = DataSize.convertToByte(dataUnit);
        Assert.assertTrue(dataSize == 1024L * 16L * 1024L);

        dataUnit = "16kB";
        dataSize = DataSize.convertToByte(dataUnit);
        Assert.assertTrue(dataSize == 1024L * 16L);

        dataUnit = "32kB";
        dataSize = DataSize.convertToByte(dataUnit);
        Assert.assertTrue(dataSize == 1024L * 32L);

        dataUnit = "1024B";
        dataSize = DataSize.convertToByte(dataUnit);
        Assert.assertTrue(dataSize == 1024L);

        dataUnit = "8GB";
        dataSize = DataSize.convertToByte(dataUnit);
        Assert.assertTrue(dataSize == 1024L * 8L * 1024L * 1024L);
    }

    @Test
    public void testParseDataUnitError() {
        String dataUnit = "8Gb";
        long dataSize;
        String msg = "";
        try {
            dataSize = DataSize.convertToByte(dataUnit);
        } catch (Exception e) {
            msg = e.getMessage();
        }
        Assert.assertTrue(msg.contains("Unknown unit"));

        dataUnit = "8";
        try {
            dataSize = DataSize.convertToByte(dataUnit);
        } catch (Exception e) {
            msg = e.getMessage();
        }
        Assert.assertTrue(msg.contains("size is not a valid data size string"));

        dataUnit = "1024g";
        try {
            dataSize = DataSize.convertToByte(dataUnit);
        } catch (Exception e) {
            msg = e.getMessage();
        }
        Assert.assertTrue(msg.contains("Unknown unit"));

        dataUnit = "xg";
        try {
            dataSize = DataSize.convertToByte(dataUnit);
        } catch (Exception e) {
            msg = e.getMessage();
        }
        Assert.assertTrue(msg.contains("size is not a valid data size string"));

        dataUnit = "24xg";
        try {
            dataSize = DataSize.convertToByte(dataUnit);
        } catch (Exception e) {
            msg = e.getMessage();
        }
        Assert.assertTrue(msg.contains("Unknown unit"));
    }
}