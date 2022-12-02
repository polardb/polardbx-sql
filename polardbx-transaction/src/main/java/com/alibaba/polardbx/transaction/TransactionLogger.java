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

package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.sql.SQLException;
import java.text.MessageFormat;

public class TransactionLogger {

    private final static Logger logger = LoggerFactory.getLogger("trans");

    public static void debug(String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isDebugEnabled()) {
            logger.debug(message);
        }
    }

    public static void debug(long id, String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isDebugEnabled()) {
            logger.debug("[" + hex(id) + "] " + message);
        }
    }

    public static void debug(long id, String message, Object... params) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isDebugEnabled()) {
            logger.debug("[" + hex(id) + "] " + MessageFormat.format(message, params));
        }
    }

    public static void info(String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isInfoEnabled()) {
            logger.info(message);
        }
    }

    public static void info(long id, String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isInfoEnabled()) {
            logger.info("[" + hex(id) + "] " + message);
        }
    }

    public static void info(long id, String message, Object... params) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isInfoEnabled()) {
            logger.info("[" + hex(id) + "] " + MessageFormat.format(message, params));
        }
    }

    public static void warn(String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isWarnEnabled()) {
            logger.warn(message);
        }
    }

    public static void warn(long id, String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isWarnEnabled()) {
            logger.warn("[" + hex(id) + "] " + message);
        }
    }

    public static void warn(long id, String message, Object... params) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isWarnEnabled()) {
            logger.warn("[" + hex(id) + "] " + MessageFormat.format(message, params));
        }
    }

    public static void warn(long id, Throwable ex, String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isWarnEnabled()) {
            logger.warn("[" + hex(id) + "] " + message, ex);
        }
    }

    public static void warn(long id, Throwable ex, String message, Object... params) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isWarnEnabled()) {
            logger.warn("[" + hex(id) + "] " + MessageFormat.format(message, params), ex);
        }
    }

    public static void error(String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isErrorEnabled()) {
            logger.error(message);
        }
    }

    public static void error(long id, String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isErrorEnabled()) {
            logger.error("[" + hex(id) + "] " + message);
        }
    }

    public static void error(long id, String message, Object... params) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isErrorEnabled()) {
            logger.error("[" + hex(id) + "] " + MessageFormat.format(message, params));
        }
    }

    public static void error(long id, Throwable ex, String message) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isErrorEnabled()) {
            logger.error("[" + hex(id) + "] " + message, ex);
        }
    }

    public static void error(long id, Throwable ex, String message, Object... params) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isErrorEnabled()) {
            logger.error("[" + hex(id) + "] " + MessageFormat.format(message, params), ex);
        }
    }

    public static void error(String message, SQLException ex) {
        if (DynamicConfig.getInstance().isEnableTransLog() && logger.isErrorEnabled()) {
            logger.error(message, ex);
        }
    }

    private static String hex(long id) {
        return Long.toHexString(id);
    }

}
