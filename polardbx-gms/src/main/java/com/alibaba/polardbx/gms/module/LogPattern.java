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

import java.util.Date;

/**
 * log pattern
 *
 * @author fangwu
 */
public enum LogPattern {
    START_OVER("%s launched"),
    LOAD_DATA("load data: %s"),
    STATE_CHANGE_FAIL("job: %s, state change fail, from %s to %s"),
    NOT_ENABLED("%s not enabled, %s"),
    PROCESS_START("%s started: %s"),
    PROCESSING("%s processing: %s"),
    PROCESS_END("%s succeed ended,result: %s"),
    INTERRUPTED("%s job being interrupted by %s"),
    UPDATE_NDV_FOR_EXPIRED("update ndv sketch for expired:%s,%s,current:%s,record:%s,expiredTime:%s"),
    UPDATE_NDV_FOR_CHANGED("update ndv sketch for currentCardinality changed:%s,%s,"
        + "max d-value:%s,current d-value:%s, currentCardinality:%s, d-value-ratio:%s"),
    NDV_SKETCH_NOT_READY("ndv sketch failed cause by sketch bytes not ready:%s"),
    UNEXPECTED("%s ended with unexpected error: %s"),
    REMOVE("%s remove: %s");

    private final String pattern;

    LogPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }
}
