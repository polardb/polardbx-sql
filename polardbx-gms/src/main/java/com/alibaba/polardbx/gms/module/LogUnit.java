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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;

import java.util.Date;
import java.util.IllegalFormatException;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_MODULE_LOG_PARAM_SIZE;

/**
 * Each log unit has two info, time and message
 * message can be built by simple log string, OR LOG_PATTER enum and params
 *
 * @author fangwu
 */
public class LogUnit {
    private final long timestamp;

    private LogPattern lp;

    private String[] params;

    private LogLevel level;

    private String traceInfo;

    public LogUnit(long timestamp, LogPattern lp, String[] params, LogLevel level, String traceInfo) {
        this.timestamp = timestamp;
        this.lp = lp;
        this.params = TStringUtil.truncate(params, InstConfUtil.getInt(MAX_MODULE_LOG_PARAM_SIZE));
        this.level = level;
        this.traceInfo = traceInfo;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String logWithTimestamp() {
        try {
            return new Date(timestamp) + " " + String.format(lp.getPattern(), params);
        } catch (IllegalFormatException formatException) {
            return formatException.getMessage();
        }
    }

    public String logWithoutTimestamp() {
        try {
            return String.format(lp.getPattern(), params);
        } catch (IllegalFormatException formatException) {
            return formatException.getMessage();
        }
    }

    public LogPattern getLp() {
        return lp;
    }

    public LogLevel getLevel() {
        return level;
    }

    public boolean since(long since) {
        return timestamp - since >= 0L;
    }

    public String toString() {
        return logWithTimestamp();
    }

    public String getTraceInfo() {
        return traceInfo;
    }
}
