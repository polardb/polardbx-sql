package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.calculator.PartitionFunctionTimeCaculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import org.junit.Test;

/**
 * Created by zhuqiwei.
 */
public class MySQLTimeCalculatorTest {
    @Test
    public void testGetDateFromDayNumber() {
        //0000-01-01
        long dayNumber1 = 1;
        MysqlDateTime mysqlDateTime1 = new MysqlDateTime();
        PartitionFunctionTimeCaculator.getDateFromDayNumber(dayNumber1, mysqlDateTime1);
        Assert.assertTrue(
            mysqlDateTime1.getYear() == 0 && mysqlDateTime1.getMonth() == 1 && mysqlDateTime1.getDay() == 1);

        //0000-03-01
        long dayNumber2 = 60;
        MysqlDateTime mysqlDateTime2 = new MysqlDateTime();
        PartitionFunctionTimeCaculator.getDateFromDayNumber(dayNumber2, mysqlDateTime2);
        Assert.assertTrue(
            mysqlDateTime2.getYear() == 0 && mysqlDateTime2.getMonth() == 3 && mysqlDateTime2.getDay() == 1);

        //0000-07-31
        long dayNumber3 = 212;
        MysqlDateTime mysqlDateTime3 = new MysqlDateTime();
        PartitionFunctionTimeCaculator.getDateFromDayNumber(dayNumber3, mysqlDateTime3);
        Assert.assertTrue(
            mysqlDateTime3.getYear() == 0 && mysqlDateTime3.getMonth() == 7 && mysqlDateTime3.getDay() == 31);

        //0000-08-31
        long dayNumber4 = 243;
        MysqlDateTime mysqlDateTime4 = new MysqlDateTime();
        PartitionFunctionTimeCaculator.getDateFromDayNumber(dayNumber4, mysqlDateTime4);
        Assert.assertTrue(
            mysqlDateTime4.getYear() == 0 && mysqlDateTime4.getMonth() == 8 && mysqlDateTime4.getDay() == 31);

        //0000-12-31
        long dayNumber5 = 365;
        MysqlDateTime mysqlDateTime5 = new MysqlDateTime();
        PartitionFunctionTimeCaculator.getDateFromDayNumber(dayNumber5, mysqlDateTime5);
        Assert.assertTrue(
            mysqlDateTime5.getYear() == 0 && mysqlDateTime5.getMonth() == 12 && mysqlDateTime5.getDay() == 31);

        //0001-01-01
        long dayNumber6 = 366;
        MysqlDateTime mysqlDateTime6 = new MysqlDateTime();
        PartitionFunctionTimeCaculator.getDateFromDayNumber(dayNumber6, mysqlDateTime6);
        Assert.assertTrue(
            mysqlDateTime6.getYear() == 1 && mysqlDateTime6.getMonth() == 1 && mysqlDateTime6.getDay() == 1);
    }

    @Test
    public void testCalDayOfYear() {
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfYear(2022, 12, 31) == 365L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfYear(1980, 10, 31) == 305L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfYear(1900, 8, 31) == 243L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfYear(0, 6, 30) == 181L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfYear(2020, 12, 31) == 366L);
    }

    @Test
    public void testCalDayOfWeek() {
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfWeek(2022, 12, 31) == 7L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfWeek(1980, 10, 31) == 6L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfWeek(1900, 8, 31) == 6L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfWeek(0, 6, 30) == 6L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calDayOfWeek(2020, 12, 31) == 5L);
    }

    @Test
    public void testCalWeekOfYear() {
        Assert.assertTrue(PartitionFunctionTimeCaculator.calWeekOfYear(0, 1, 1) == 52L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calWeekOfYear(2022, 12, 31) == 52L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calWeekOfYear(1980, 10, 31) == 44L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calWeekOfYear(1900, 8, 31) == 35L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calWeekOfYear(0, 6, 30) == 26L);
        Assert.assertTrue(PartitionFunctionTimeCaculator.calWeekOfYear(2020, 12, 31) == 53L);
    }
}
