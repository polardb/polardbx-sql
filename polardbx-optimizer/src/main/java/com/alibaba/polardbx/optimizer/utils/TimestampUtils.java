package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.time.ZoneId;
import java.util.Optional;
import java.util.TimeZone;

public class TimestampUtils {
    public static ZoneId getZoneId(ExecutionContext context) {
        return Optional.ofNullable(context)
            .map(ExecutionContext::getTimeZone)
            .map(InternalTimeZone::getZoneId)
            .orElse(InternalTimeZone.DEFAULT_ZONE_ID);
    }

    public static TimeZone getTimeZone(ExecutionContext context) {
        return Optional.ofNullable(context)
            .map(ExecutionContext::getTimeZone)
            .map(InternalTimeZone::getTimeZone)
            .orElseGet(TimeZone::getDefault);
    }
}
