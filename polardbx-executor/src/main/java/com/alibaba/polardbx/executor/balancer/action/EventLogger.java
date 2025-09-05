package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.utils.ExecUtils;

public class EventLogger {
    public static void log(EventType eventType, String eventInfo){
        String version = ExecUtils.isMysql80Version() ? "__80__" : "__57__";
        com.alibaba.polardbx.common.eventlogger.EventLogger.log(eventType, String.format("%s %s", version, eventInfo));
    }
}
