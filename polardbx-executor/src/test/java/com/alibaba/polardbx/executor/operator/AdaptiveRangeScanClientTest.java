package com.alibaba.polardbx.executor.operator;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class AdaptiveRangeScanClientTest {

    @Test
    public void testFindNextPowerOfTwo() throws Exception {
        Method method = AdaptiveRangeScanClient.class.getDeclaredMethod("findNextPowerOfTwo", int.class);
        method.setAccessible(true);

        // Test cases where input is less than or equal to 0
        assertEquals(1, method.invoke(null, -1));
        assertEquals(1, method.invoke(null, 0));

        // Test case where input is already a power of two
        assertEquals(4, method.invoke(null, 4));
        assertEquals(16, method.invoke(null, 16));

        // Test cases where input is not a power of two
        assertEquals(4, method.invoke(null, 3));
        assertEquals(128, method.invoke(null, 127));
        assertEquals(512, method.invoke(null, 511));
    }

    @Test
    public void testGetPolicyWithConstant() {
        assertEquals(AdaptiveRangeScanClient.AdaptivePolicy.CONSTANT,
            AdaptiveRangeScanClient.AdaptivePolicy.getPolicy("CONSTANT"));
    }

    @Test
    public void testGetPolicyWithExponential() {
        assertEquals(AdaptiveRangeScanClient.AdaptivePolicy.EXPONENT,
            AdaptiveRangeScanClient.AdaptivePolicy.getPolicy("EXPONENTIAL"));
    }

    @Test
    public void testGetPolicyWithEmpty() {
        assertEquals(AdaptiveRangeScanClient.AdaptivePolicy.EXPONENT,
            AdaptiveRangeScanClient.AdaptivePolicy.getPolicy(""));
    }

    @Test
    public void testGetPolicyWithNull() {
        assertEquals(AdaptiveRangeScanClient.AdaptivePolicy.EXPONENT,
            AdaptiveRangeScanClient.AdaptivePolicy.getPolicy(null));
    }

    @Test
    public void testGetPolicyWithOtherValue() {
        assertEquals(AdaptiveRangeScanClient.AdaptivePolicy.EXPONENT,
            AdaptiveRangeScanClient.AdaptivePolicy.getPolicy("SOME_OTHER_POLICY"));
    }

    @Test
    public void testGetPolicyWithCaseInsensitive() {
        assertEquals(AdaptiveRangeScanClient.AdaptivePolicy.CONSTANT,
            AdaptiveRangeScanClient.AdaptivePolicy.getPolicy("constant "));
        assertEquals(AdaptiveRangeScanClient.AdaptivePolicy.EXPONENT,
            AdaptiveRangeScanClient.AdaptivePolicy.getPolicy(" exponential"));
    }
}