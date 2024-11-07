package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.core.datatype.Blob;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PLUtilsTest {

    @Test
    public void testGetPrintStringWithNull() {
        Assert.assertNull(PLUtils.getPrintString(null));
    }

    @Test
    public void testGetPrintStringWithNumber() {
        Assert.assertEquals("123", PLUtils.getPrintString(123));
    }

    @Test
    public void testGetPrintStringWithDecimal() {
        Decimal decimal = Decimal.fromString("1234567890123456");
        Assert.assertEquals("1234567890123456", PLUtils.getPrintString(decimal));
    }

    @Test
    public void testGetPrintStringWithByteArray() {
        byte[] bytes = new byte[] {0x1, 0x2, 0x3};
        Assert.assertEquals("0x010203", PLUtils.getPrintString(bytes));
    }

    @Test
    public void testGetPrintStringWithBlob() {
        Blob blob = new Blob("test".getBytes());
        Assert.assertEquals("0x74657374", PLUtils.getPrintString(blob));
    }

    @Test
    public void testGetPrintStringWithSlice() {
        Slice slice = Slices.wrappedBuffer("test".getBytes());
        Assert.assertEquals("'test'", PLUtils.getPrintString(slice));
    }

    @Test
    public void testGetPrintStringWithEnumValue() {
        EnumValue enumValue = new EnumValue(new EnumType(Arrays.asList("TEST_ENUM", "TEST")), "TEST");
        Assert.assertEquals("'TEST'", PLUtils.getPrintString(enumValue));
    }

    @Test
    public void testGetPrintStringWithString() {
        String stringWithSpecialChar = "it's a test";
        String expected = "'it''s a test'";
        Assert.assertEquals(expected, PLUtils.getPrintString(stringWithSpecialChar));
    }

    @Test
    public void testGetPrintStringWithString2() {
        String stringWithSpecialChar = "it\"s a test";
        String expected = "'it\"s a test'";
        Assert.assertEquals(expected, PLUtils.getPrintString(stringWithSpecialChar));
    }
}