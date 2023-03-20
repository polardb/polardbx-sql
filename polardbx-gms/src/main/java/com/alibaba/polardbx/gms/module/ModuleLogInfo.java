/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.module;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import java.util.Collections;
import java.util.IllegalFormatException;
import java.util.Iterator;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import static com.alibaba.polardbx.common.eventlogger.EventType.MODULE_ERROR;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_MODULE_LOG;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_MODULE_LOG_PARAMS_SIZE;
import static com.alibaba.polardbx.gms.module.LogPattern.START_OVER;
import static com.alibaba.polardbx.gms.module.Module.MODULE_LOG;
import static com.alibaba.polardbx.common.utils.logger.LoggerFactory.getLogger;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;

/**
 * Log event separated by module
 *
 * @author fangwu
 */
public class ModuleLogInfo implements ModuleInfo {

    private static final int MAX_LOG_SIZE = 1000;

    private final Map<Module, Queue<LogUnit>> moduleLog;

    private static final ModuleLogInfo m = new ModuleLogInfo();

    private ModuleLogInfo() {
        this.moduleLog = Maps.newConcurrentMap();
    }

    public static ModuleLogInfo getInstance() {
        return m;
    }

    /**
     * log normal event default
     *
     * @param m module that triggers this log
     * @param lp log pattern
     * @param params log info
     */
    public void logRecord(Module m, LogPattern lp, String[] params, LogLevel level) {
        logRecord(m, lp, params, level, Thread.currentThread().getName(), null);
    }

    public void logRecord(Module m, LogPattern lp, String[] params, LogLevel level, String traceInfo) {
        logRecord(m, lp, params, level, traceInfo, null);
    }

    public void logRecord(Module m, LogPattern lp, String[] params, LogLevel level, Throwable t) {
        logRecord(m, lp, params, level, Thread.currentThread().getName(), t);
    }

    public void logRecord(Module m, LogPattern lp, String[] params, LogLevel level, String traceInfo, Throwable t) {
        logRecord(m, lp, params, level, traceInfo, t, false);
    }

    public void logRecord(Module m, LogPattern lp, String[] params, LogLevel level, String traceInfo, Throwable t,
                          boolean needAuditLog) {
        // enable check
        if (!InstConfUtil.getBool(ENABLE_MODULE_LOG)) {
            return;
        }

        // null check
        if (ifAnyNull(m, lp, params, level)) {
            logRecord(
                MODULE_LOG,
                UNEXPECTED,
                new String[] {
                    m == null ? null : m.name(),
                    (lp == null ? null : lp.getPattern()) + "," +
                        (params == null ? null : params.length + "")},
                CRITICAL,
                traceInfo
            );
            return;
        }

        // log to file
        logToModule(m, lp, params, level, traceInfo, t);
        if (level == CRITICAL) {
            logToEvent(m, lp, params, traceInfo, t);
        }
        if (needAuditLog) {
            logToStatistic(m, lp, params, level, traceInfo, t);
        }

        // record log in mem
        if (params.length > InstConfUtil.getInt(MAX_MODULE_LOG_PARAMS_SIZE)) {
            return;
        }
        Queue<LogUnit> l = moduleLog.computeIfAbsent(m, k -> Queues.newConcurrentLinkedQueue());
        if (l.size() >= MAX_LOG_SIZE) {
            l.remove();
        }
        l.add(new LogUnit(System.currentTimeMillis(), lp, params, level, traceInfo));
    }

    /**
     * get logger by module && log this event info disk
     *
     * @param m module that triggers this log
     * @param lp log pattern
     * @param params log info
     * @param level log level, normal->info, warning->warn, critical->error
     */
    private void logToModule(Module m, LogPattern lp, String[] params, LogLevel level, String traceInfo,
                             Throwable t) {
        Logger logger = getLogger(m.getLoggerName());
        String logLine = String.format(lp.getPattern(), (Object[]) params) + "#" + traceInfo;
        try {
            switch (level) {
            case NORMAL:
                logger.info(logLine, t);
                break;
            case WARNING:
                logger.warn(logLine, t);
                break;
            case CRITICAL:
                logger.error(logLine, t);
                break;
            }
        } catch (IllegalFormatException e) {
            // avoid dead loop
            if (m == MODULE_LOG) {
                throw e;
            }

            // Try record log error by module_log module.
            logRecord(
                MODULE_LOG,
                UNEXPECTED,
                new String[] {
                    m.name() + "," + lp.getPattern() + "," + (params == null ? null : params.length + ""),
                    e.getMessage()
                },
                CRITICAL,
                traceInfo
            );
        }
    }

    private void logToStatistic(Module m, LogPattern lp, String[] params, LogLevel level, String traceInfo,
                                Throwable t) {
        Logger logger = LoggerFactory.getLogger("audit");
        String logLine = String.format(lp.getPattern(), (Object[]) params) + "#" + traceInfo;
        try {
            switch (level) {
            case NORMAL:
                logger.info(logLine, t);
                break;
            case WARNING:
                logger.warn(logLine, t);
                break;
            case CRITICAL:
                logger.error(logLine, t);
                break;
            }
        } catch (IllegalFormatException e) {
            // avoid dead loop
            if (m == MODULE_LOG) {
                throw e;
            }

            // Try record log error by module_log module.
            logRecord(
                MODULE_LOG,
                UNEXPECTED,
                new String[] {
                    m.name() + "," + lp.getPattern() + "," + (params == null ? null : params.length + ""),
                    e.getMessage()
                },
                CRITICAL,
                traceInfo
            );
        }
    }

    private void logToEvent(Module m, LogPattern lp, String[] params, String traceInfo,
                            Throwable t) {
        String logLine =
            m.name() + "#" +
                String.format(lp.getPattern(), (Object[]) params) + "#" +
                (t == null ? "" : t.getMessage()) + "#" +
                traceInfo;
        EventLogger.log(MODULE_ERROR, logLine);
    }

    public Iterator<LogUnit> logRequireLogUnit(Module m, long since) {
        Queue<LogUnit> l = moduleLog.get(m);
        if (l == null || l.size() == 0) {
            return Collections.emptyIterator();
        }

        return l.stream().filter(u -> u.since(since)).iterator();
    }

    /**
     * return true if any obj is null
     */
    private boolean ifAnyNull(Object... os) {
        for (Object o : os) {
            if (o == null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String state() {
        return ModuleInfo.buildStateByArgs(ENABLE_MODULE_LOG);
    }

    @Override
    public String status(long since) {
        return "";
    }

    @Override
    public String resources() {
        StringBuilder s = new StringBuilder();
        for (Map.Entry<Module, Queue<LogUnit>> entry : moduleLog.entrySet()) {
            s.append(entry.getKey()).append(":").append(entry.getValue().size()).append(";");
        }
        return s.toString();
    }

    @Override
    public String scheduleJobs() {
        return "";
    }

    @Override
    public String workload() {
        return "";
    }

    @Override
    public String views() {
        return "MODULE_EVENT";
    }

    public static void main(String[] args) {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        for (int i = 0; i < 100000; i++) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.STATISTICS, START_OVER, new String[] {"test", "test"}, LogLevel.NORMAL);
        }
    }
}
