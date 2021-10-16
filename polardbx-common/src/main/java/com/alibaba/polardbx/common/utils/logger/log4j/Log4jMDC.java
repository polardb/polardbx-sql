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

package com.alibaba.polardbx.common.utils.logger.log4j;

import com.alibaba.polardbx.common.utils.logger.MDCAdapter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Log4jMDC implements MDCAdapter {

    @Override
    public void put(String key, String val) {
        org.apache.log4j.MDC.put(key, val);
    }

    @Override
    public String get(String key) {
        return (String) org.apache.log4j.MDC.get(key);
    }

    @Override
    public void remove(String key) {
        org.apache.log4j.MDC.remove(key);
    }

    @Override
    public void clear() {
        org.apache.log4j.MDC.clear();
    }

    @Override
    public Map getCopyOfContextMap() {
        Map old = org.apache.log4j.MDC.getContext();
        if (old != null) {
            return new HashMap(old);
        } else {
            return null;
        }
    }

    @Override
    public void setContextMap(Map contextMap) {
        if (contextMap == null) {
            contextMap = new HashMap();
        }
        Map old = org.apache.log4j.MDC.getContext();
        if (old == null) {
            Iterator entrySetIterator = contextMap.entrySet().iterator();
            while (entrySetIterator.hasNext()) {
                Map.Entry mapEntry = (Map.Entry) entrySetIterator.next();
                org.apache.log4j.MDC.put((String) mapEntry.getKey(), mapEntry.getValue());
            }
        } else {
            old.clear();
            old.putAll(contextMap);
        }
    }
}
