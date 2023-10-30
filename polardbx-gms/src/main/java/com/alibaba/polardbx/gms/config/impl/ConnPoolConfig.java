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

package com.alibaba.polardbx.gms.config.impl;

import com.alibaba.polardbx.rpc.pool.XConnectionManager;

/**
 * @author chenghui.lch
 */
public class ConnPoolConfig {

    public String connProps = null;
    public Integer minPoolSize = null;
    public Integer maxPoolSize = null;
    public Integer idleTimeout = null;
    public Integer blockTimeout = null;
    public String xprotoConfig = null;
    public Long xprotoFlag = null;
    public Integer xprotoStorageDbPort = null;
    public Integer xprotoMaxClientPerInstance = null;
    public Integer xprotoMaxSessionPerClient = null;
    public Integer xprotoMaxPooledSessionPerInstance = null;
    public Integer xprotoMinPooledSessionPerInstance = null;
    public Long xprotoSessionAgingTime = null;
    public Long xprotoSlowThreshold = null;

    // Feature switch of X-protocol.
    public Boolean xprotoAuth = null;
    public Boolean xprotoAutoCommitOptimize = null;
    public Boolean xprotoXplan = null;
    public Boolean xprotoXplanExpendStar = null;
    public Boolean xprotoXplanTableScan = null;
    public Boolean xprotoTrxLeakCheck = null;
    public Boolean xprotoMessageTimestamp = null;
    public Boolean xprotoPlanCache = null;
    public Boolean xprotoChunkResult = null;
    public Boolean xprotoPureAsyncMpp = null;
    public Boolean xprotoDirectWrite = null;
    public Boolean xprotoFeedback = null;

    // Checker & packet for X-protocol.
    public Boolean xprotoChecker = null;
    public Long xprotoMaxPacketSize = null;

    // Pass instance-level transaction isolation level to connection pool
    public Integer defaultTransactionIsolation = null;

    public Integer maxWaitThreadCount;

    public ConnPoolConfig() {
    }

    public boolean isStorageDbXprotoEnabled() {
        final int defaultPort = XConnectionManager.getInstance().getStorageDbPort();
        return defaultPort > 0 || (0 == defaultPort && xprotoStorageDbPort != null && xprotoStorageDbPort >= 0);
    }
}
