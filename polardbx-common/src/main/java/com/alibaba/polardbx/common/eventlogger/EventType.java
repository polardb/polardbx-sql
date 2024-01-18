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
    /**
     * Rebalance数据分布信息
     */
    REBALANCE_INFO(EventLevel.INFO),
    /**
     * DDL发生错误，通常由bug引起
     */
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

    CREATE_DATABASE_LIKE_AS(EventLevel.INFO),
    ONLINE(EventLevel.INFO),
    OFFLINE(EventLevel.INFO),

    /**
     * MODULE unexpected critical log
     */
    MODULE_ERROR(EventLevel.WARN),

    /**
     * Log for X-Protocol(XRPC)
     */

    XRPC_NEW_VALID_CLIENT(EventLevel.INFO),
    XRPC_AUTH_TIMEOUT(EventLevel.WARN),
    XRPC_KILL_CLIENT(EventLevel.WARN),

    DML_ERROR(EventLevel.WARN),

    /*
     * Usage statistics for TTL and cold-data table
     */
    CREATE_TTL_TABLE(EventLevel.INFO),
    CREATE_OSS_TABLE(EventLevel.INFO),
    TTL_EXPIRED(EventLevel.INFO),
    TTL_ARCHIVE(EventLevel.INFO),
    INIT_OSS(EventLevel.INFO),
    CLOSE_OSS(EventLevel.INFO),

    OPTIMIZER_ALERT(EventLevel.INFO);

    private final EventLevel level;

    EventType(EventLevel level) {
        this.level = level;
    }

    public EventLevel getLevel() {
        return this.level;
    }
}
