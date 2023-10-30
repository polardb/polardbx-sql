package com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeStampGenerator {

    private ZonedDateTime zonedDateTime;

    public TimeStampGenerator(ZonedDateTime zonedDateTime) {
        this.zonedDateTime = zonedDateTime;
    }

    public long generateTs(long stepInMillis) {
        long epochMilli = zonedDateTime.toInstant().toEpochMilli();
        zonedDateTime = millisToZonedDateTime(epochMilli + stepInMillis, ZoneId.systemDefault());
        return epochMilli + stepInMillis;
    }

    public static ZonedDateTime millisToZonedDateTime(Long millis, ZoneId zoneId) {
        if (millis == null) {
            return null;
        }
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId);
    }

    public ZonedDateTime getZonedDateTime() {
        return zonedDateTime;
    }

    public void setZonedDateTime(ZonedDateTime zonedDateTime) {
        this.zonedDateTime = zonedDateTime;
    }
}
