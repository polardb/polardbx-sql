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

package com.alibaba.polardbx.common.logger.logback;

import org.apache.commons.lang.StringUtils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class ConsoleFilter extends Filter<ILoggingEvent> {

    private static boolean isEclipse = false;

    static {

        String appname = System.getProperty("appName");
        isEclipse = StringUtils.isEmpty(appname);
    }

    @Override
    public FilterReply decide(ILoggingEvent event) {
        if (isEclipse) {

            return FilterReply.ACCEPT;
        } else {
            return FilterReply.DENY;
        }
    }
}
