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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;

public class LoggerUtil {

    private static final Logger loggerSpm = LoggerFactory.getLogger("spm");

    public static void buildMDC(String schemaName) {
        if (TStringUtil.isNotEmpty(schemaName)) {

            MDC.put(MDC.MDC_KEY_APP, schemaName.toLowerCase());

            String connInfo = MDC.get(MDC.MDC_KEY_CON);
            if (TStringUtil.isNotEmpty(connInfo)) {
                int indexUserStart = connInfo.indexOf('=');
                int indexUserEnd = connInfo.indexOf(',');
                int indexSchemaStart = connInfo.lastIndexOf('=');

                StringBuilder newConnInfo = new StringBuilder();
                newConnInfo.append(connInfo.substring(0, indexUserStart + 1)).append(schemaName);
                newConnInfo.append(connInfo.substring(indexUserEnd, indexSchemaStart + 1)).append(schemaName);

                MDC.put(MDC.MDC_KEY_CON, newConnInfo.toString());
            }
        }
    }

    public static void logSpm(String schemaName, String msg) {
        MDC.put(MDC.MDC_KEY_APP, schemaName.toLowerCase());
        loggerSpm.info(msg);
    }

    public static void logSpmError(String schemaName, String msg, Throwable throwable) {
        MDC.put(MDC.MDC_KEY_APP, schemaName.toLowerCase());
        loggerSpm.error(msg, throwable);
    }
}
