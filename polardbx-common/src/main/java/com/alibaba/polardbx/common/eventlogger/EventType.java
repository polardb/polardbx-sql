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

import java.util.EnumSet;

public enum EventType {
    /**
     * Rebalance数据分布信息
     */
    REBALANCE_INFO(EventLevel.INFO),

    /**
     * 用于统计一些DDL发生的次数
     */
    DDL_INFO(EventLevel.INFO),
    /**
     * TwoPhaseDdl信息
     */
    TWO_PHASE_DDL_INFO(EventLevel.INFO),
    /**
     * TwoPhaseDdl信息
     */
    TWO_PHASE_DDL_WARN(EventLevel.WARN),
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
     * STORAGE POOL INFO need do ha
     */
    STORAGE_POOL_INFO(EventLevel.INFO),
    /**
     * event for creating db with mode=auto
     */
    CREATE_AUTO_MODE_DB(EventLevel.WARN),

    CREATE_DATABASE_LIKE_AS(EventLevel.INFO),

    STANDARD_TO_ENTERPRISE(EventLevel.INFO),

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
    XPLAN_FEEDBACK_DISABLE(EventLevel.INFO),

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

    OPTIMIZER_ALERT(EventLevel.INFO),

    AUTO_SP(EventLevel.INFO),
    AUTO_SP_OPT(EventLevel.INFO),
    AUTO_SP_ERR(EventLevel.INFO),

    TRX_LOG_ERR(EventLevel.INFO),
    TRX_RECOVER(EventLevel.INFO),
    TRX_INFO(EventLevel.INFO),
    TRX_ERR(EventLevel.INFO),

    CDC_WARN(EventLevel.WARN);

    private final EventLevel level;

    EventType(EventLevel level) {
        this.level = level;
    }

    public EventLevel getLevel() {
        return this.level;
    }

    private final static EnumSet<EventType> TRX_EVENT = EnumSet.of(
        AUTO_SP_ERR, TRX_LOG_ERR, TRX_RECOVER, TRX_ERR
    );

    public static boolean isTrxEvent(EventType t) {
        return TRX_EVENT.contains(t);
    }
}
