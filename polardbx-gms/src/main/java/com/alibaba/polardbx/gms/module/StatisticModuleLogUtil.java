package com.alibaba.polardbx.gms.module;

import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;

/**
 * @author pangzhaoxing
 */
public class StatisticModuleLogUtil {

    public static void logNormal(LogPattern logPattern, String[] params) {
        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, logPattern, params, LogLevel.NORMAL);
    }

    public static void logWarning(LogPattern logPattern, String[] params) {
        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, logPattern, params, LogLevel.WARNING);
    }

    public static void logWarning(LogPattern logPattern, String[] params, Throwable e) {
        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, logPattern, params, LogLevel.WARNING, e);
    }

    public static void logCritical(LogPattern logPattern, String[] params) {
        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, logPattern, params, LogLevel.CRITICAL);
    }

    public static void logCritical(LogPattern logPattern, String[] params, Throwable t) {
        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, logPattern, params, LogLevel.CRITICAL, t);
    }

}
