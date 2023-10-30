package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class MysqlDateTimeTest {

    @Test
    public void testFastGetSmallIntBytes() {
        for (long i = 0; i < 20000; i++) {
            byte[] expectedBytes = originalGetYear(i);
            byte[] fastBytes = LongUtil.fastGetSmallLongBytesForDate(i, 4);
            Assert.assertArrayEquals(expectedBytes, fastBytes);
        }

        for (long i = 0; i < 200; i++) {
            byte[] expectedBytes = originalGetMonth(i);
            byte[] fastBytes = LongUtil.fastGetSmallLongBytesForDate(i, 2);
            Assert.assertArrayEquals(expectedBytes, fastBytes);
        }
    }

    @Test
    public void testFastGetDateTimeBytes() throws IOException {
        final Random random = new Random(System.currentTimeMillis());
        final int round = 100;
        for (int i = 0; i < round; i++) {
            MysqlDateTime mysqlDateTime = new MysqlDateTime(random.nextInt(2000),
                random.nextInt(12), random.nextInt(30), random.nextInt(24),
                random.nextInt(60), random.nextInt(60), random.nextInt(100));
            byte[] expectedBytes = mysqlDateTime.toDatetimeString(0).getBytes();
            byte[] fastBytes = mysqlDateTime.fastToDatetimeBytes(0);
            Assert.assertArrayEquals("Failed at " + mysqlDateTime, expectedBytes, fastBytes);
        }
    }

    private byte[] originalGetMonth(long month) {
        String monthString;
        if (month < 10) {
            monthString = "0" + month;
        } else {
            monthString = Long.toString(month);
        }
        return monthString.getBytes();
    }

    private byte[] originalGetYear(long year) {
        final String yearZeros = "0000";
        String yearString;
        if (year < 1000) {
            // Add leading zeros
            yearString = "" + year;
            yearString = yearZeros.substring(0, (4 - yearString.length())) +
                yearString;
        } else {
            yearString = "" + year;
        }
        return yearString.getBytes();
    }
}
