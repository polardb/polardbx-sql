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

package com.alibaba.polardbx.optimizer.planmanager;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * <pre>
 * there are two statement map (String key --> PreparedStmtCache) in ServerConnection
 * one is for COM_QUERY, the other is for
 * COM_STMT_PREPARE/COM_STMT_EXECUTE_COM_STMT_CLOSE.
 * </pre>
 */
public class StatementMap {

    private static final Logger logger = LoggerFactory.getLogger(StatementMap.class);
    /**
     * 同一个session中query串行
     * 不用考虑并发安全
     */
    private final Map<String, PreparedStmtCache> stmtCacheMap;

    public StatementMap(boolean isServerPrepare) {
        if (isServerPrepare) {
            /*
              二进制prepare协议下 stmtId由内核分配
              不需要区分stmtId的大小写
             */
            this.stmtCacheMap = new HashMap<>();
        } else {
            this.stmtCacheMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        }
    }

    public void put(String name, PreparedStmtCache sm) {
        // overwrite if it's exist, follow mysql way
        stmtCacheMap.put(name, sm);
    }

    public PreparedStmtCache delete(String stmtId) {
        PreparedStmtCache cache = stmtCacheMap.remove(stmtId);
        if (cache == null) {
            logger.warn("remove sm is null " + stmtId);
            return null;
        } else {
            return cache;
        }
    }

    public PreparedStmtCache find(String stmtId) {
        return stmtCacheMap.get(stmtId);
    }
}
