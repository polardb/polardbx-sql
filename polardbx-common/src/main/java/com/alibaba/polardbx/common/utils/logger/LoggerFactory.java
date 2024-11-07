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

package com.alibaba.polardbx.common.utils.logger;

import com.alibaba.polardbx.common.utils.logger.jcl.JclLoggerAdapter;
import com.alibaba.polardbx.common.utils.logger.jdk.JdkLoggerAdapter;
import com.alibaba.polardbx.common.utils.logger.slf4j.Slf4jLoggerAdapter;
import com.alibaba.polardbx.common.utils.logger.support.FailsafeLogger;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LoggerFactory {

    private LoggerFactory() {
    }

    private static volatile LoggerAdapter LOGGER_ADAPTER;

    private static final ConcurrentMap<String, FailsafeLogger> LOGGERS =
        new ConcurrentHashMap<String, FailsafeLogger>();

    static {
        String logger = System.getProperty("tddl.application.logger");
        if ("slf4j".equals(logger)) {
            setLoggerAdapter(new Slf4jLoggerAdapter());
        } else if ("jcl".equals(logger)) {
            setLoggerAdapter(new JclLoggerAdapter());
        } else if ("jdk".equals(logger)) {
            setLoggerAdapter(new JdkLoggerAdapter());
        } else {
            try {
                setLoggerAdapter(new Slf4jLoggerAdapter());
            } catch (Throwable e1) {
                try {
                    setLoggerAdapter(new JclLoggerAdapter());
                } catch (Throwable e3) {
                    setLoggerAdapter(new JdkLoggerAdapter());
                }
            }
        }
    }

    public static void setLoggerAdapter(LoggerAdapter loggerAdapter) {
        if (loggerAdapter != null) {
            Logger logger = loggerAdapter.getLogger(LoggerFactory.class.getName());
            logger.info("using logger: " + loggerAdapter.getClass().getName());
            LoggerFactory.LOGGER_ADAPTER = loggerAdapter;
            for (Map.Entry<String, FailsafeLogger> entry : LOGGERS.entrySet()) {
                entry.getValue().setLogger(LOGGER_ADAPTER.getLogger(entry.getKey()));
            }
        }
    }

    public static LoggerAdapter getLoggerAdapter() {
        return LoggerFactory.LOGGER_ADAPTER;
    }

    public static Logger getLogger(Class<?> key) {
        FailsafeLogger logger = LOGGERS.get(key.getName());
        if (logger == null) {
            LOGGERS.putIfAbsent(key.getName(), new FailsafeLogger(LOGGER_ADAPTER.getLogger(key)));
            logger = LOGGERS.get(key.getName());
        }
        return logger;
    }

    public static Logger getLogger(String key) {
        return getLogger(key, false);
    }

    public static Logger getLogger(String key, boolean simple) {
        FailsafeLogger logger = LOGGERS.get(key);
        if (logger == null) {
            LOGGERS.putIfAbsent(key, new FailsafeLogger(LOGGER_ADAPTER.getLogger(key), simple));
            logger = LOGGERS.get(key);
        }
        return logger;
    }

    public static void setLevel(Level level) {
        LOGGER_ADAPTER.setLevel(level);
    }

    public static Level getLevel() {
        return LOGGER_ADAPTER.getLevel();
    }

    public static File getFile() {
        return LOGGER_ADAPTER.getFile();
    }

}
