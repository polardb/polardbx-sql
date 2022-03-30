package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import org.apache.commons.lang.StringUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public interface ScheduleDateTimeConverter {

    static ZonedDateTime secondToZonedDateTime(Long seconds, String timeZone){
        return secondToZonedDateTime(seconds, TimeZoneUtils.zoneIdOf(timeZone));
    }

    static ZonedDateTime secondToZonedDateTime(Long seconds, ZoneId zoneId){
        if(seconds == null){
            return null;
        }
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(seconds), zoneId);
    }

}