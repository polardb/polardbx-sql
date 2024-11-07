package com.alibaba.polardbx.executor.mpp.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class RangeScanModeTest {

    @Test
    public void testGetModeWithSerialize() {
        assertEquals(RangeScanMode.SERIALIZE, RangeScanMode.getMode("serialize"));
        assertEquals(RangeScanMode.SERIALIZE, RangeScanMode.getMode("Serialize"));
        assertEquals(RangeScanMode.SERIALIZE, RangeScanMode.getMode("SERIALIZE"));
    }

    @Test
    public void testGetModeWithNormal() {
        assertEquals(RangeScanMode.NORMAL, RangeScanMode.getMode("normal"));
        assertEquals(RangeScanMode.NORMAL, RangeScanMode.getMode("Normal"));
        assertEquals(RangeScanMode.NORMAL, RangeScanMode.getMode("NORMAL"));
    }

    @Test
    public void testGetModeWithAdaptive() {
        assertEquals(RangeScanMode.ADAPTIVE, RangeScanMode.getMode("adaptive"));
        assertEquals(RangeScanMode.ADAPTIVE, RangeScanMode.getMode("Adaptive"));
        assertEquals(RangeScanMode.ADAPTIVE, RangeScanMode.getMode("ADAPTIVE"));
    }

    @Test
    public void testGetModeWithInvalidMode() {
        assertNull(RangeScanMode.getMode("invalid"));
        assertNull(RangeScanMode.getMode(""));
        assertNull(RangeScanMode.getMode(null));
    }

    @Test
    public void testIsNormalMode() {
        assertEquals(true, RangeScanMode.NORMAL.isNormalMode());
        assertEquals(false, RangeScanMode.SERIALIZE.isNormalMode());
        assertEquals(false, RangeScanMode.ADAPTIVE.isNormalMode());
    }
}