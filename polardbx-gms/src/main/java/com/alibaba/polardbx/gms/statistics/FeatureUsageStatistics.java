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

package com.alibaba.polardbx.gms.statistics;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class FeatureUsageStatistics {
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureUsageStatistics.class);

    private static final String FEATURE_USAGE_STATISTICS_TABLE = GmsSystemTables.FEATURE_USAGE_STATISTICS;

    private static final Map<String, AtomicLong> CACHE_KEY_STATISTICS = new ConcurrentHashMap<>();

    private static ScheduledExecutorService scheduledExecutorService = null;
    private final static int DEFAULT_SCAN_INTERVAL = 5 * 60 * 1000;
    private final static int MAX_KEY_LEN = 128;

    public static void increaseByOne(String key) {
        increaseByStep(key, 1);
    }

    public static void increaseByStep(String key, long step) {
        if (StringUtils.isEmpty(key) || key.length() > MAX_KEY_LEN) {
            throw new AssertionError(
                "Key can't be null and the length can't greater than " + String.valueOf(MAX_KEY_LEN));
        }
        AtomicLong cacheValue = CACHE_KEY_STATISTICS.get(key);
        if (cacheValue == null) {
            cacheValue = new AtomicLong(step);
            CACHE_KEY_STATISTICS.putIfAbsent(key, cacheValue);
        } else {
            cacheValue.addAndGet(step);
        }
    }

    public static void init() {

        if (scheduledExecutorService == null) {
            scheduledExecutorService = ExecutorUtil.createScheduler(1,
                new NamedThreadFactory("FeatureUsageStatistics-Threads"),
                new ThreadPoolExecutor.CallerRunsPolicy());
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    upsert();
                } catch (Exception e) {
                }
            }, DEFAULT_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * insert a new entry or update the value for existing name
     */
    private static void upsert() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        int i = 1;
        StringBuilder sb = new StringBuilder();
        sb.append("insert into " + FEATURE_USAGE_STATISTICS_TABLE + " (name, val) values");
        for (Map.Entry<String, AtomicLong> cacheItem : CACHE_KEY_STATISTICS.entrySet()) {
            if (cacheItem.getValue().longValue() > 0) {
                if (i == 1) {
                    sb.append("(?, ?)");
                } else {
                    sb.append(",(?, ?)");
                }
                MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, cacheItem.getKey());
                MetaDbUtil.setParameter(i++, params, ParameterMethod.setLong, cacheItem.getValue().getAndSet(0));
            }
        }
        if (!params.isEmpty()) {
            sb.append(" on duplicate key update val = val + values(val)");
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                MetaDbUtil.insert(sb.toString(), params, metaDbConn);
            } catch (Exception e) {
                LOGGER.error("Failed to query the system table '" + FEATURE_USAGE_STATISTICS_TABLE + "'", e);
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                    FEATURE_USAGE_STATISTICS_TABLE,
                    e.getMessage());
            }
        }
    }

    public class FeatureUsageStatisticsKeys {
        /**
         * the length of the key can't great than 128
         */
        public static final String MOVE_DATABASE_COUNT = "MOVE_DATABASE_COUNT";
        public static final String MOVE_DATABASE_SUCCESS_COUNT = "MOVE_DATABASE_SUCCESS_COUNT";
    }
}
