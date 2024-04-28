package com.alibaba.polardbx.common.utils;

import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

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
}
