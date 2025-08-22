package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.PruneRawString;
import com.alibaba.polardbx.common.jdbc.RawString;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.utils.GeneralUtil.formatSampleRate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author fangwu
 */
public class GeneralUtilTest {

    @Test
    public void testLsnException() {
        SQLException sqlException = new SQLException(""
            + "Fatal error when fetch data: Variable 'read_lsn' can't be set to the value of'2267571542");
        RuntimeException runtimeException = GeneralUtil.nestedException(sqlException);
        Assert.assertTrue(runtimeException.getMessage().contains("show storage"));
    }

    @Test
    public void testDecode() throws IOException {
        String traceInfo = "MULTI[22]\n"
            + "    Catalog:gdcams_tp,mk_run_meter_day_energy,data_time,2023-12-07 00:00:00_2023-12-08 00:00:00\n"
            + "    Action:datetimeTypeCompensation\n"
            + "    StatisticValue:81892745\n"
            + "    normal val:1\n"
            + "    compensation value 2023-12-04 00:00:00_2023-12-05 00:00:00:81892744";
        Map<String, String> result = GeneralUtil.decode(traceInfo);
        System.out.println(result);
        Assert.assertTrue(result.size() == 1);
        Assert.assertTrue(
            result.get("catalog:gdcams_tp,mk_run_meter_day_energy,data_time,2023-12-07 00:00:00_2023-12-08 00:00:00\n"
                + "action:datetimetypecompensation").equals("81892745"));
    }

    /**
     * Checks if isPrimary returns false for an empty/null string.
     */
    @Test
    public void testIsPrimaryEmptyString() {
        assertFalse("An empty string should not be considered as primary", GeneralUtil.isPrimary(""));
        assertFalse("An null should not be considered as primary", GeneralUtil.isPrimary(null));
    }

    /**
     * Ensures isPrimary returns false with a string containing only whitespaces.
     */
    @Test
    public void testIsPrimaryWhitespace() {
        assertFalse("A string with only whitespace characters should not be considered as primary",
            GeneralUtil.isPrimary("   "));
    }

    /**
     * Validates isPrimary returns false for a non-primary string.
     */
    @Test
    public void testIsPrimaryNonPrimaryString() {
        assertFalse("A string other than 'primary' should not be considered as primary",
            GeneralUtil.isPrimary("secondary"));
    }

    /**
     * Tests isPrimary returning true for lowercase 'primary'.
     */
    @Test
    public void testIsPrimaryLowerCasePrimary() {
        assertTrue("Lowercase 'primary' should be recognized as primary",
            GeneralUtil.isPrimary("primary".toLowerCase()));
    }

    /**
     * Verifies isPrimary returns true for uppercase 'PRIMARY'.
     */
    @Test
    public void testIsPrimaryUpperCasePrimary() {
        assertTrue("Uppercase 'PRIMARY' should be recognized as primary", GeneralUtil.isPrimary("PRIMARY"));
        assertTrue("mix case 'PriMArY' should be recognized as primary", GeneralUtil.isPrimary("PriMArY"));
    }

    @Test
    public void testSampleStringNormalCase() {
        float sampleRate = 0.5f;
        String expectedOutput = "0.5";
        assertEquals(expectedOutput, formatSampleRate(sampleRate));
    }

    @Test
    public void testSampleStringSampleRateZero() {
        float sampleRate = 0f;
        String expectedOutput = "0.0";
        assertEquals(expectedOutput, formatSampleRate(sampleRate));
    }

    @Test
    public void testSampleStringSampleRateOne() {
        float sampleRate = 1f;
        String expectedOutput = "1.0";
        assertEquals(expectedOutput, formatSampleRate(sampleRate));
    }

    @Test
    public void testSampleStringSampleRateGreaterThanOne() {
        float sampleRate = 1.5f;
        String expectedOutput = "1.5";
        assertEquals(expectedOutput, formatSampleRate(sampleRate));

        sampleRate = 0.0002f;
        expectedOutput = "0.0002";
        assertEquals(expectedOutput, formatSampleRate(sampleRate));

        sampleRate = 0.000000002f;
        expectedOutput = "0.000000002";
        assertEquals(expectedOutput, formatSampleRate(sampleRate));

        sampleRate = 2.000000002f;
        expectedOutput = "2.0";
        assertEquals(expectedOutput, formatSampleRate(sampleRate));

        sampleRate = 11.0023450002f;
        expectedOutput = "11.002345";
        assertEquals(expectedOutput, formatSampleRate(sampleRate));
    }

    @Test
    public void testIsWithinPercentageEqualNumbers() {
        // Design: Test when both numbers are equal.
        assertEquals(true, GeneralUtil.isWithinPercentage(100, 100, 10));
    }

    @Test
    public void testIsWithinPercentageExactMatch() {
        // Design: Test when difference equals the threshold.
        assertEquals(true, GeneralUtil.isWithinPercentage(100, 110, 10));
    }

    @Test
    public void testIsWithinPercentageBelowThreshold() {
        // Design: Test when difference is less than the threshold.
        assertEquals(true, GeneralUtil.isWithinPercentage(100, 105, 10));
    }

    @Test
    public void testIsWithinPercentageAboveThreshold() {
        // Design: Test when difference is greater than the threshold.
        assertEquals(false, GeneralUtil.isWithinPercentage(100, 115, 10));
    }

    @Test
    public void testIsWithinPercentageZeroPercentage() {
        // Design: Test with a zero percentage threshold.
        assertEquals(true, GeneralUtil.isWithinPercentage(100, 100, 0));
    }

    @Test
    public void testIsWithinPercentageNegativePercentage() {
        // Design: Test with a negative percentage threshold (invalid input).
        assertEquals(false, GeneralUtil.isWithinPercentage(100, 100, -10));
    }

    @Test
    public void testIsWithinPercentageDecimalThreshold() {
        // Design: Test with a decimal percentage threshold.
        assertEquals(true, GeneralUtil.isWithinPercentage(100, 106, 6.1));
    }

    @Test
    public void testIsWithinPercentageLargeNumbers() {
        // Design: Test with large numbers to ensure correct calculation of threshold.
        assertEquals(true, GeneralUtil.isWithinPercentage(1000000, 1000100, 0.1));
    }

    @Test
    public void testMapToList() {
        // test empty param
        assertEquals(Collections.emptyList(), GeneralUtil.mapToList(null, false));

        // test normal param map to list
        Map<Integer, ParameterContext> paramMap = new HashMap<>();
        List<String> list = Arrays.asList("test1", "pruned");
        RawString r1 = new RawString(list);
        RawString r2 = new PruneRawString(list, PruneRawString.PRUNE_MODE.RANGE, 0, 1, null);

        assert r1.getObjList().size() == 2;
        assert r2.getObjList().size() == 1;

        paramMap.put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {1, "arg1"}));
        paramMap.put(2, new ParameterContext(ParameterMethod.setObject1, new Object[] {2, r1}));
        paramMap.put(3, new ParameterContext(ParameterMethod.setObject1, new Object[] {3, r2}));
        paramMap.put(4, null);

        // no batch test
        List<ParameterContext> result = GeneralUtil.mapToList(paramMap, false);

        assertEquals(4, result.size());
        assertEquals(r1, result.get(1).getValue());
        assertEquals(r2, result.get(2).getValue());
        assertNull(result.get(3));

        // batch test
        result = GeneralUtil.mapToList(paramMap, true);

        assertEquals(4, result.size());
        assertEquals(r1, result.get(1).getValue());
        assert result.get(2).getValue() instanceof RawString;
        assert ((RawString) result.get(2).getValue()).getObjList().size() == 2;
        assertNull(result.get(3));
    }
}
