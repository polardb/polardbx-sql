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

package com.alibaba.polardbx.common.eventlogger;

public enum EventType {

    DDL_WARN(EventLevel.WARN),

    DDL_PAUSED(EventLevel.INFO),
    /**
     * CN发生切主，停止原先leader节点中的DDL
     */
    DDL_INTERRUPT(EventLevel.INFO),
    MOVE_DATABASE_PENDING(EventLevel.WARN),

    DEAD_LOCK_DETECTION(EventLevel.INFO),

    /**
     * DN need do ha
     */
    DN_HA(EventLevel.INFO),

    /**
     * event for creating db with mode=auto
     */
    CREATE_AUTO_MODE_DB(EventLevel.WARN),
    ONLINE(EventLevel.INFO),
    OFFLINE(EventLevel.INFO),

    /**
     * MODULE unexpected critical log
     */
    MODULE_ERROR(EventLevel.WARN);

    private final EventLevel level;

    EventType(EventLevel level) {
        this.level = level;
    }

    public EventLevel getLevel() {
        return this.level;
    }
}
