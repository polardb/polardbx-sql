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

package com.alibaba.polardbx.common.utils.logger.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncAppender extends AsyncAppenderBase<ILoggingEvent> {

    public static Map<String, AsyncAppender> appenderMap = new ConcurrentHashMap<>();

    boolean includeCallerData = false;

    @Override
    protected void loadAppender() {
        if (supportRemoteConsume) {
            appenderMap.put(this.getName(), this);
        }
    }

    @Override
    protected void unLoadAppender() {
        appenderMap.remove(this.getName());
    }

    /**
     * Events of level TRACE, DEBUG and INFO are deemed to be discardable.
     *
     * @return true if the event is of level TRACE, DEBUG or INFO false otherwise.
     */
    @Override
    protected boolean isDiscardable(ILoggingEvent event) {
        ch.qos.logback.classic.Level level = event.getLevel();
        return level.toInt() <= Level.INFO_INT;
    }

    @Override
    protected void preprocess(ILoggingEvent eventObject) {
        eventObject.prepareForDeferredProcessing();
        if (includeCallerData) {
            eventObject.getCallerData();
        }
    }

    public boolean isIncludeCallerData() {
        return includeCallerData;
    }

    public void setIncludeCallerData(boolean includeCallerData) {
        this.includeCallerData = includeCallerData;
    }

}
