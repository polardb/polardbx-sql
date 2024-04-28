package com.alibaba.polardbx.qatest.dql.sharding.enums;

import com.alibaba.polardbx.qatest.dql.sharding.type.numeric.CastTestUtils;
import org.apache.commons.lang.time.DateUtils;

import java.util.Date;
import java.util.Random;
import java.util.function.Supplier;

public class TimeTypeUtil {
    public static final String DATE_TEST = "DATE_TEST";

    public static final String TIME_TEST = "TIME_TEST";

    public static final String TIMESTAMP_TEST = "TIMESTAMP_TEST";

    public static final String DATETIME_TEST = "DATETIME_TEST";

    public static final String[] SUPPORT_TYPES = new String[] {
        DATE_TEST,
        TIME_TEST,
        TIMESTAMP_TEST,
        DATETIME_TEST
    };

    public static Supplier<Object> getGenerator(final String col, boolean hasNull) {
        switch (col) {
        case DATE_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null :
                getRandomDate();
        case TIME_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null :
                getRandomTime();
        case TIMESTAMP_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null :
                getRandomTimestamp();
        case DATETIME_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null :
                getRandomDateTime();
        default:
            throw new RuntimeException("not support col" + col);
        }
    }

    public static Date getRandomDate() {
        Random rand = new Random();
        Date date = DateUtils.addDays(new Date(), rand.nextInt(365) - 365);
        return date;
    }

    public static Date getRandomTime() {
        Random rand = new Random();
        Date time = DateUtils.setMinutes(DateUtils.setHours(new Date(), rand.nextInt(24)), rand.nextInt(60));
        return time;
    }

    public static Date getRandomDateTime() {
        Random rand = new Random();
        Date dateTime =
            DateUtils.addMinutes(DateUtils.addDays(new Date(), rand.nextInt(365) - 365), rand.nextInt(24 * 60));
        return dateTime;
    }

    public static Date getRandomTimestamp() {
        Random rand = new Random();
        Date timeStamp =
            DateUtils.addMinutes(DateUtils.addDays(new Date(), rand.nextInt(365) - 365), rand.nextInt(24 * 60));
        return timeStamp;
    }
}
