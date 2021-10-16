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
import com.alibaba.polardbx.common.utils.logger.jcl.JclMDC;
import com.alibaba.polardbx.common.utils.logger.jdk.JdkMDC;
import com.alibaba.polardbx.common.utils.logger.log4j.Log4jLoggerAdapter;
import com.alibaba.polardbx.common.utils.logger.log4j.Log4jMDC;
import com.alibaba.polardbx.common.utils.logger.slf4j.Slf4jLoggerAdapter;
import com.alibaba.polardbx.common.utils.logger.slf4j.Slf4jMDC;

import java.util.Map;

public class MDC {

    public static final String MDC_KEY_APP = "app";
    public static final String MDC_KEY_CON = "CONNECTION";

    static MDCAdapter mdcAdapter;

    static {
        LoggerAdapter logger = LoggerFactory.getLoggerAdapter();
        if (logger instanceof Slf4jLoggerAdapter) {
            mdcAdapter = new Slf4jMDC();
        } else if (logger instanceof JclLoggerAdapter) {
            mdcAdapter = new JclMDC();
        } else if (logger instanceof Log4jLoggerAdapter) {
            mdcAdapter = new Log4jMDC();
        } else {
            mdcAdapter = new JdkMDC();
        }
    }

    public static void put(String key, String val) {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        mdcAdapter.put(key, val);
    }

    public static String get(String key) {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        return mdcAdapter.get(key);
    }

    public static void remove(String key) {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        mdcAdapter.remove(key);
    }

    public static void clear() {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        mdcAdapter.clear();
    }

    public static Map getCopyOfContextMap() {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        return mdcAdapter.getCopyOfContextMap();
    }

    public static void setContextMap(Map contextMap) {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        mdcAdapter.setContextMap(contextMap);
    }

}
